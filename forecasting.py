# import logging
# import threading
# import subprocess
from multiprocessing import freeze_support


from external.client import YandexWeatherAPI
from tasks import (
    DataFetchingTask,
    DataCalculationTask,
    DataAggregationTask,
    DataAnalyzingTask,
)
from utils import CITIES, get_url_by_city_name


def forecast_weather():
    """
    Анализ погодных условий по городам
    """
    # city_name = "MOSCOW"
    # url_with_data = get_url_by_city_name(city_name)
    # resp = YandexWeatherAPI.get_forecasting(url_with_data)
    # print(resp)
    cities_list = list(CITIES.keys())
    weather_data = DataFetchingTask.run(cities_list)
    proceeded_weather_data = DataCalculationTask.run(weather_data)
    final_table = DataAggregationTask.run(proceeded_weather_data)
    best_cities = DataAnalyzingTask.run(final_table)
    return best_cities


if __name__ == "__main__":
    result = forecast_weather()
    print(result)
