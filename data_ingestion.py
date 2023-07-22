import os
import sys
from exception import CustomException
from logger import logging
import pandas as pd
from dataclasses import dataclass
import mysql.connector

@dataclass
class DataIngestionConfig:
    train_data_path: str = os.path.join('artifacts', 'train.csv')
    test_data_path: str = os.path.join('artifacts', 'test.csv')


def connect_database():
    logging.info("Connecting to a Database")
    try:
        mydb = mysql.connector.connect(
                host = "localhost",
                user = "root",
                passwd = "*******",# Give your database password
                database = "stores_sales_prediction",
                auth_plugin='mysql_native_password')
        logging.info("Successfully connected")
        return mydb
    except Exception as e:
        logging.exception(e)
        raise CustomException(e, sys)


def read_train_data_from_db(db):
    ingestion_config = DataIngestionConfig()
    logging.info("Reading the Train data from the database")
    try:
        df = pd.read_sql("SELECT * FROM train", con=db)
        os.makedirs(os.path.dirname(ingestion_config.train_data_path), exist_ok=True)
        df.to_csv(ingestion_config.train_data_path, index=False, header=True)
        logging.info("Reading the Train data successful")
        return ingestion_config.train_data_path
    except Exception as e:
        logging.exception(e)
        raise CustomException(e, sys)


def read_test_data_from_db(db):
    ingestion_config = DataIngestionConfig()
    logging.info("Reading the test data from the database")
    try:
        df = pd.read_sql("SELECT * FROM test", con=db)
        os.makedirs(os.path.dirname(ingestion_config.train_data_path), exist_ok=True)
        df.to_csv(ingestion_config.test_data_path, index=False, header=True)
        logging.info("Reading the test data successful")
        return ingestion_config.test_data_path
    except Exception as e:
        logging.exception(e)
        raise CustomException(e, sys)
        
