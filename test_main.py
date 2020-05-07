import unittest
import main
from pyspark.sql import SparkSession


class TestMain(unittest.TestCase):

    def setUp(self):
        """Init of tests """
        spark = SparkSession.builder.appName('Taxi').getOrCreate()
        self.taxiDf = spark.read.csv("resources/train-10000.csv", header=True)

    def test_meanSpeed(self):
        """ verify the result of meanSpeed function for the first 5 values"""
        df_mean_speed = main.mean_speed(self.taxiDf)
        df_mean_speed.show()
        list_mean_speed = [row["meanSpeed(km/h)"] for row in df_mean_speed.take(5)]
        compare = [11.856428146656878, 9.803658835090804, 10.82220083941101, 12.465721030245636, 9.836594146211462]
        for i in range(len(list_mean_speed)):
            self.assertAlmostEqual(list_mean_speed[i], compare[i], places=4)

    def test_get_nb_drive_per_day(self):
        """ verify the result of get_nb_drive_per_day function"""
        drive_per_day = main.get_nb_drive_per_day(self.taxiDf)
        drive_per_day.show()
        list_driver_per_day = [row["count"] for row in drive_per_day.collect()]
        self.assertEqual(list_driver_per_day, [1356, 1286, 1475, 1514, 1428, 1420, 1520])

    def test_get_nb_drive_per_slice_day(self):
        """ verify the result of get_nb_drive_per_slice_day function"""
        nb_drive_per_slice_day = main.get_nb_drive_per_slice_day(self.taxiDf)
        self.assertEqual(nb_drive_per_slice_day["0-6"], 1422)
        self.assertEqual(nb_drive_per_slice_day["6-12"], 2643)
        self.assertEqual(nb_drive_per_slice_day["12-18"], 3092)
        self.assertEqual(nb_drive_per_slice_day["18-24"], 2842)

    def test_get_km_per_day(self):
        """ verify the result of get_km_per_day function"""
        nb_km_by_day = main.get_km_per_day(self.taxiDf)
        nb_km_by_day.show()
        list_km_by_day = [row["sum(dist)"] for row in nb_km_by_day.collect()]
        compare = [4789.794910823723, 4530.8014779966325, 4994.494979438139, 4932.921239493415,
                   5028.187267415691, 4704.324866755778, 5073.513208014454]
        for i in range(len(list_km_by_day)):
            self.assertAlmostEqual(list_km_by_day[i], compare[i], places=4)
