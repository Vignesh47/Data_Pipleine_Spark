import datetime
import logging
import logging.config
import os


class Storage:

    def __init__(self,spark):
        self.spark = spark
    def storage_data(self,base_data):
        logger = logging.getLogger("Storage")
        logging.info("The File has been stored in")
        # save the dataframe into csv format
        output_file_path = os.path.join(os.getcwd(), "output", f"petition_{datetime.datetime.now()}")
        base_data.write.csv(output_file_path, header=True)
        logging.info("File has been stored here")