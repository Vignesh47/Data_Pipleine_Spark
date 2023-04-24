#importing required Libraries for data ingestion


import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit,when,column
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging
import logging.config
import os

class Ingest:

    def __init__(self,spark):
        self.spark = spark
        # Creating Object for Spark function
        spark = SparkSession.builder.appName("My new app").enableHiveSupport().getOrCreate()
        logging.info("Spark has been initiated.......")


    def ingest_data(self):
        logger = logging.getLogger("Ingest")
        # define the schema for the JSON data
        schema = StructType([
            StructField("abstract", StructType([
                StructField("_value", StringType())
            ])),
            StructField("label", StructType([
                StructField("_value", StringType())
            ])),
            StructField("numberOfSignatures", LongType())
        ])
        logging.info("Schema has been  Created")

        # read the JSON file into a DataFrame
        input_data_path = os.path.join(os.getcwd(), "data", "input_data.json")
        print(input_data_path)
        # petition_df = self.spark.read.json("/Users/darkstorm/Downloads/New Resume/updated/Task/input_data.json", schema)
        petition_df = self.spark.read.json(input_data_path, schema)
        logging.info("File has been Loaded successfully")
        return petition_df

