import unittest
from unittest import mock
import sys
import pandas as pd

class dst_time_tester(unittest.TestCase):
    def setUp(self):
        # Create mock modules
        self.awsglue_transforms = mock.MagicMock()
        self.awsglue_utils = mock.MagicMock()
        self.pyspark_context = mock.MagicMock()
        self.awsglue_context = mock.MagicMock()
        self.awsglue_job = mock.MagicMock()

        # Patch sys.modules to simulate imports
        self.patcher = mock.patch.dict(sys.modules, {
            'awsglue.transforms': self.awsglue_transforms,
            'awsglue.utils': self.awsglue_utils,
            'pyspark.context': self.pyspark_context,
            'awsglue.context': self.awsglue_context,
            'awsglue.job': self.awsglue_job,
        })
        self.patcher.start()


    def tearDown(self):
        self.patcher.stop()

    def test_normal_day(self):
        from scripts.energy_market_etl import dataframe_processor
        normal_day = pd.DataFrame({
            "Delivery day": ["01/05/2024"], "Hour 1": [33.42], "Hour 2": [93.26], "Hour 3A": [63.78], "Hour 3B": [None],
            "Hour 4": [70.14], "Hour 5": [49.33], "Hour 6": [36.96], "Hour 7": [91.36], "Hour 8": [85.57],
            "Hour 9": [91.14], "Hour 10": [56.66], "Hour 11": [39.76], "Hour 12": [72.98], "Hour 13": [71.23],
            "Hour 14": [28.28], "Hour 15": [89.9], "Hour 16": [64.65], "Hour 17": [32.04], "Hour 18": [29.32],
            "Hour 19": [48.2], "Hour 20": [71.78], "Hour 21": [98.48], "Hour 22": [40.75], "Hour 23": [84.53],
            "Hour 24": [84.68],
        })
        df_final = dataframe_processor(normal_day)
        self.assertEqual(len(df_final), 24, "Should have 24 entries.")
        time_diffs = df_final["timestamp_utc"].diff().dropna()
        self.assertTrue((time_diffs == pd.Timedelta(hours=1)).all(),
            "UTC timestamps should have 1-hour intervals")

    def test_fall_dst_day(self):
        from scripts.energy_market_etl import dataframe_processor
        dst_fall_day = pd.DataFrame({
            "Delivery day": ["27/10/2019"],
            "Hour 1": [2.16], "Hour 2": [2.88], "Hour 3A": [73.3], "Hour 3B": [21.91],
            "Hour 4": [68.86], "Hour 5": [92.62], "Hour 6": [15.54], "Hour 7": [13.31],
            "Hour 8": [89.67], "Hour 9": [34.94], "Hour 10": [9.83], "Hour 11": [43.42],
            "Hour 12": [91.92], "Hour 13": [42.58], "Hour 14": [31.08], "Hour 15": [99.67],
            "Hour 16": [43.93], "Hour 17": [99.27], "Hour 18": [14.79], "Hour 19": [34.13],
            "Hour 20": [70.66], "Hour 21": [51.64], "Hour 22": [31.5], "Hour 23": [16.42],
            "Hour 24": [86.53],
        })
        df_final = dataframe_processor(dst_fall_day)
        self.assertEqual(len(df_final), 25, "Should have 24 entries.")
        time_diffs = df_final["timestamp_utc"].diff().dropna()
        self.assertTrue((time_diffs == pd.Timedelta(hours=1)).all(),
            "UTC timestamps should have 1-hour intervals")

    def test_spring_dst_day(self):
        from scripts.energy_market_etl import dataframe_processor
        dst_spring_day = pd.DataFrame({
            "Delivery day": ["31/03/2019"],
            "Hour 1": [86.83], "Hour 2": [88.85], "Hour 3A": [None], "Hour 3B": [None],
            "Hour 4": [73.68], "Hour 5": [21.91], "Hour 6": [48.31], "Hour 7": [80.68],
            "Hour 8": [87.08], "Hour 9": [76.69], "Hour 10": [77.46], "Hour 11": [73.98],
            "Hour 12": [8.71], "Hour 13": [53.0], "Hour 14": [53.3], "Hour 15": [52.63],
            "Hour 16": [39.42], "Hour 17": [94.08], "Hour 18": [29.06], "Hour 19": [21.26],
            "Hour 20": [71.43], "Hour 21": [5.1], "Hour 22": [37.84], "Hour 23": [3.19],
            "Hour 24": [9.74],
        })
        df_final = dataframe_processor(dst_spring_day)
        self.assertEqual(len(df_final), 23, "Should have 24 entries.")
        time_diffs = df_final["timestamp_utc"].diff().dropna()
        self.assertTrue((time_diffs == pd.Timedelta(hours=1)).all(),
                        "UTC timestamps should have 1-hour intervals")



if __name__ == '__main__':
    unittest.main()