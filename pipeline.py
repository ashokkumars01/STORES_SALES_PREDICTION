import os
import sys
from logger import logging
from exception import CustomException
from data_transformation import Data_Transformer, initiate_data_transform
from data_ingestion import connect_database, read_train_data_from_db, read_test_data_from_db
from data_model import Model_Build
from predict_pipeline import Prediction

def Model_Pipeline():

    logging.info("Pipeline started")

    try:
        ### Data Ingestion
        train_data_path, test_data_path = initiate_data_transform()

        ### Data Transformer
        processed_train_data_path, processed_test_data_path = Data_Transformer(train_data_path, test_data_path)

        ### Data Model
        _, _, gradboost_regression_path = Model_Build(processed_train_data_path)

        ### Predict Pipeline
        pred = Prediction(gradboost_regression_path, processed_test_data_path)

        logging.info("Pipeline completed")

    except Exception as e:
        logging.exception(e)
        raise CustomException(e, sys)

Model_Pipeline()