import unittest
import os
from pyspark.sql.types import StringType
from pyspark.sql.functions import *
from pyspark.sql.types import *

from Data_pipeline import ingestion
from pyspark.sql import SparkSession

class IngestionTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.appName("testing data ingestion").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_ingestion_count(self):
        spark = SparkSession.builder.appName("Testing  Data Ingestion").getOrCreate()
        schema = StructType([
            StructField("abstract", StructType([
                StructField("_value", StringType())
            ])),
            StructField("label", StructType([
                StructField("_value", StringType())
            ])),
            StructField("numberOfSignatures", LongType())
        ])
        path = "/Users/darkstorm/PycharmProjects/Mydatapipeline/"
        input_data_path = os.path.join(path, "data", "input_data.json")
        petition_df = spark.read.json(input_data_path, schema)
        print(petition_df)
        len_df = petition_df.count()
        print(len_df)
        self.assertNotEquals(0, len_df)
        return petition_df
