# Data Transforamtion Data pipeline using Spark


#importing Librarires
import pyspark.sql.functions as f
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark
from pyspark.sql import SparkSession
import re
from pyspark.sql.window import Window
from pyspark.ml.feature import Tokenizer, StopWordsRemover
from nltk.stem.snowball import SnowballStemmer
import logging
import logging.config


#importing ingestion, Transformation and Storage file
import ingestion
import transformation
import storage
class data_pipeline:
    logging.config.fileConfig("/Users/darkstorm/PycharmProjects/Mydatapipeline/Configs/logging.config")
    def pipeline(self):
        logging.info("Pipeline has been Initiated successfully")
        ingest = ingestion.Ingest(self.spark)
        raw_data = ingest.ingest_data()
        logging.info("File ingestion has been successfully completed")
        transform = transformation.Transform(self.spark)
        base_data = transform.transform_data(raw_data)
        logging.info("Base --->>>> Data has been successfully cleaned")
        store = storage.Storage(self.spark)
        store.storage_data(base_data)
        logging.info("Integration--->>>> Data has been stored successfully")

    def spark_build(self):
        self.spark = SparkSession.builder.appName("My First Spark Code").getOrCreate()

if __name__ == "__main__":
    pipeline_new = data_pipeline()
    pipeline_new.spark_build()
    pipeline_new.pipeline()




