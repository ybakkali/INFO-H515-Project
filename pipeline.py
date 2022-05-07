from typing import Union

from pyspark.sql import SparkSession, DataFrame
from pyspark.ml.feature import Imputer

from dataset import Dataset, DatasetEnum
import pandas as pd

from start_session import *


class Pipeline:
    @staticmethod
    def predict_missing_values(df: Union[pd.DataFrame, DataFrame]) -> DataFrame:
        """
        This function will predict missing values in a dataframe using Pyspark MLlib
        """
        # TODO there are actually no missing value in any dataset ^^'
        if isinstance(df, pd.DataFrame):
            # Convert the pandas dataframe to a spark dataframe
            df = SparkSession.builder.getOrCreate().createDataFrame(df)
        # Create a new Imputer object
        imputer = Imputer(inputCols=df.columns, outputCols=df.columns)
        # Fit the Imputer model
        imputer_model = imputer.fit(df)
        # Transform the dataframe
        df = imputer_model.transform(df)
        # Return the dataframe
        return df


if __name__ == '__main__':
    # Create a new Pipeline object
    pipeline = Pipeline()
    # Load the dataframe
    dataset = Dataset()
    dataset.load_dataset(DatasetEnum.candidate_items)
    df_main = dataset.candidate_items
    # Predict missing values
    print("Before predicting missing values", df_main.count())
    df_main = pipeline.predict_missing_values(df_main)
    # show the difference
    print("After predicting missing values : ", df_main.count())
