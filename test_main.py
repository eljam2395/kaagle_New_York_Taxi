import unittest
import main
from pyspark.sql import SparkSession


class TestMain(unittest.TestCase):

    def setUp(self):
        """Initialisation des tests."""
        spark = SparkSession.builder.appName('Taxi').getOrCreate()
        self.taxiDf = spark.read.csv("resources/train-10000.csv", header=True)

    def test_meanSpeed(self):
        """ verify the result of the meanSpeed function"""
        df_mean_speed = main.mean_speed(self.taxiDf)
        df_mean_speed.show()
        list_mean_speed = [row["meanSpeed(km/h)"] for row in df_mean_speed.take(5)]
        self.assertEqual(list_mean_speed, [11.856428146656878, 9.803658835090804, 10.82220083941101, 12.465721030245636, 9.836594146211462])


if __name__ == '__main__':
    unittest.main()
