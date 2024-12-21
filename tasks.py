
import subprocess
import threading
from multiprocessing import Manager, freeze_support
from multiprocessing import Process, Queue, parent_process
from concurrent.futures import ThreadPoolExecutor
import os
import json
import logging 
import functools
import logging
from logging.handlers import RotatingFileHandler

from external.client import YandexWeatherAPI
from utils import get_url_by_city_name
import time

# создаем собственный объект `logger` с именем `myapp`
logger = logging.getLogger('myapp')
logger.setLevel(logging.INFO)

#для корневого процесса создаем файл с логом
if not parent_process():
    handler = RotatingFileHandler('myapp.log', maxBytes=20000, backupCount=5)
    logger.addHandler(handler)

#декоратор для отлова исключений и логирования вызовов
def execution_controller(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            timestamp = time.process_time()
            result = func(*args, **kwargs)
            logger.info(f"{func} called successfully, execution time:{time.process_time()-timestamp}" ) 
            return result
        except Exception as e:
            logger.error(f"{func} caused exception: {repr(e)}" ) 
            return None
    return wrapper


#по сути предоставляет фунционал pool.map, но с общей памятью
class CustomProcessPool:
    def __init__(self, process_count:int):
        self.process_count = process_count

    def run(self, target, args):
        process_pull = []
        for i in range(self.process_count):
            process_pull.append(Process(target=target, args=args))
        for i,p in enumerate(process_pull):
            process_pull[i].start()
        for i,p in enumerate(process_pull):
            process_pull[i].join()


#получение данных через API
#Получите информацию о погодных условиях для указанного списка городов, используя API Яндекс Погоды.
#IO-bound
class DataFetchingTask:
    @execution_controller
    def _get_weather_data_for_city(city_name:str):
        url_with_data = get_url_by_city_name(city_name)
        resp = YandexWeatherAPI.get_forecasting(url_with_data)
        resp['city_name'] = city_name
        return resp
    

    @execution_controller
    def run(cities:list):
        with ThreadPoolExecutor() as pool:
            thread_results = pool.map(DataFetchingTask._get_weather_data_for_city, cities, timeout=5)
        return [r for r in thread_results if r]

#вычисление погодных параметров;
#Вычислите среднюю температуру и проанализируйте информацию об осадках за указанный период для всех городов.
#CPU-bound
class DataCalculationTask:
    @execution_controller
    def _get_data_from_analyser(weather_data:dict):
        if not os.path.exists("tmp"):
            os.mkdir("tmp")

        thread = threading.current_thread()
        ident = thread.ident
        filename_in = f"tmp/analyser_input_data_{ident}.json"
        filename_out = f"tmp/analyser_results_{ident}.json"
        json.dump(weather_data, open(filename_in,'w'))

        command_to_execute = [
            "python",
            "./external/analyzer.py",
                "-i",
            filename_in,
            "-o",
            filename_out,
        ]
        run = subprocess.run(command_to_execute, capture_output=True, stderr=None)

        result = None
        if run.returncode == 0:
            result = json.load(open(filename_out))
            result['city_name'] = weather_data['city_name']
        try:
            os.remove(filename_in)
            os.remove(filename_out)
        except Exception as e:
            pass
        
        return result        
    
    @execution_controller
    def _process_task_get_average_per_city(data:dict):
        temperatures = [day['temp_avg'] for day in data["days"] if day['temp_avg']]
        relevant_hours = [day['relevant_cond_hours'] for day in data["days"] if day['relevant_cond_hours']]
        result = {
            "avg_temperature": sum(temperatures)/len(temperatures),
            "relevant_hours":sum(relevant_hours)
        }
        return result

    @execution_controller
    def _process_task_calculate_average(in_queue:Queue, out_queue:Queue):
        while not in_queue.empty():
            item = in_queue.get()
            result = DataCalculationTask._process_task_get_average_per_city(item)
            item["summary"] = result
            out_queue.put(item)

    @execution_controller
    def run(weather_data_list:list):
        results = []
        with ThreadPoolExecutor() as pool:
            thread_results = pool.map(DataCalculationTask._get_data_from_analyser, weather_data_list, timeout=5)
            results = [r for r in thread_results if r]

        queue_tasks = Manager().Queue()
        for r in results:
            queue_tasks.put(r)
        queue_results = Manager().Queue()

        custom_process_pool = CustomProcessPool(3)
        custom_process_pool.run(DataCalculationTask._process_task_calculate_average, (queue_tasks,queue_results))

        results = []
        while not queue_results.empty():
            results.append(queue_results.get())

        return results

#объединение вычисленных данных;
#Объедините полученные данные и сохраните результат в текстовом файле.
class DataAggregationTask:
    @execution_controller
    def _process_task_clean_null_days(item:dict):
        key = item['city_name']
        data = {
            "days":[d for d in item["days"] if d["hours_count"]],
            "summary": item["summary"]
        }
        return key, data

    @execution_controller
    def _process_task_format_weather_data(in_queue:Queue, out_dict:dict): #out_dict - shared memory
        while not in_queue.empty():
            item = in_queue.get()
            key, data = DataAggregationTask._process_task_clean_null_days(item)
            out_dict[key] = data

    @execution_controller
    def run(data:list):
        queue_tasks = Manager().Queue()
        for d in data:
            queue_tasks.put(d)
        out_dict = Manager().dict()
        custom_process_pool = CustomProcessPool(3)
        custom_process_pool.run(DataAggregationTask._process_task_format_weather_data, (queue_tasks,out_dict))
        result = {k:out_dict[k] for k in out_dict}
        json.dump(result,open("summary_result.json", 'w'))
        return result
    pass

#финальный анализ и получение результата.
#Проанализируйте результат и сделайте вывод, какой из городов наиболее благоприятен для поездки.
class DataAnalyzingTask:
    @execution_controller
    def run(data:dict):
        max_avg_temperature = None
        max_count_clean_hours = None
        for city_name in data:
            city_avg_temperature = data[city_name]["summary"]["avg_temperature"]
            if max_avg_temperature:
                if city_avg_temperature>max_avg_temperature:
                    max_avg_temperature = city_avg_temperature
            else:
                max_avg_temperature = city_avg_temperature

            city_clean_hours = data[city_name]["summary"]["relevant_hours"]
            if max_count_clean_hours:
                if city_clean_hours>max_count_clean_hours:
                    max_count_clean_hours = city_clean_hours
            else:
                max_count_clean_hours = city_clean_hours

        cities_with_max_avg_temp = []
        cities_with_max_count_clean_hours = []
        for city_name in data:
            if data[city_name]["summary"]["avg_temperature"] == max_avg_temperature:
                cities_with_max_avg_temp.append(city_name)
            if data[city_name]["summary"]["relevant_hours"] == max_count_clean_hours:
                cities_with_max_count_clean_hours.append(city_name)

        return {
            "max_avg_temperature":max_avg_temperature,
            "cities_with_max_avg_temperature": cities_with_max_avg_temp,
            "max_count_clean_hours":max_count_clean_hours,
            "cities_with_max_count_clean_hours":cities_with_max_count_clean_hours
        }

    pass

from utils import CITIES
if __name__ == '__main__':
    freeze_support()
    cities_list = list(CITIES.keys())
    weather_data = DataFetchingTask.run(cities_list)
    proceeded_weather_data = DataCalculationTask.run(weather_data)
    final_table = DataAggregationTask.run(proceeded_weather_data)
    best_cities = DataAnalyzingTask.run(final_table)
    print(best_cities)
