from tasks import (
    DataFetchingTask,
    DataCalculationTask,
    DataAggregationTask,
    DataAnalyzingTask,
)

import unittest

class MyTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.prepare_data1 = DataFetchingTask.run(["MOSCOW"])
        cls.prepare_data2 = DataCalculationTask.run(cls.prepare_data1)
        cls.prepare_data3 = DataAggregationTask.run(cls.prepare_data2)


    def test_DataFetchingTask(self):
        results = DataFetchingTask.run([])
        self.assertEqual(results, [])

        results = DataFetchingTask.run()
        self.assertEqual(results, None)

        results = DataFetchingTask.run("Moscow")
        self.assertEqual(results, [])

        results = DataFetchingTask.run(123)
        self.assertEqual(results, None)



    def test_DataCalculationTask(self):
        results = DataCalculationTask.run(self.prepare_data1)
        self.assertTrue(type(results)==list)

        results = DataCalculationTask.run([])
        self.assertEqual(results, [])

        results = DataCalculationTask.run()
        self.assertEqual(results, None)

        results = DataCalculationTask.run("Moscow")
        self.assertEqual(results, [])

        results = DataCalculationTask.run(123)
        self.assertEqual(results, None)

    
    def test_DataAggregationTask(self):
        results = DataAggregationTask.run(self.prepare_data2)
        self.assertTrue(type(results)==dict)

        results = DataAggregationTask.run([])
        self.assertEqual(results, {})

        results = DataAggregationTask.run()
        self.assertEqual(results, None)

        results = DataAggregationTask.run("Moscow")
        self.assertEqual(results, {})

        results = DataAggregationTask.run(123)
        self.assertEqual(results, None)

        

    def test_DataAnalyzingTask(self):
        results = DataAnalyzingTask.run(self.prepare_data3)
        self.assertTrue(type(results)==dict)
        self.assertTrue("cities_with_max_avg_temperature" in results)
        self.assertTrue("cities_with_max_count_clean_hours" in results)

        results = DataAnalyzingTask.run([])
        self.assertTrue(results["cities_with_max_avg_temperature"] == [])
        self.assertTrue(results["cities_with_max_count_clean_hours"] == [])

        results = DataAnalyzingTask.run()
        self.assertEqual(results, None)

        results = DataAnalyzingTask.run("Moscow")
        self.assertEqual(results, None)

        results = DataAnalyzingTask.run(123)
        self.assertEqual(results, None)

        



if __name__ == '__main__':
    unittest.main()