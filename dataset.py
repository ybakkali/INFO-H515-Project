import os
import enum

# Create an enum that contains candidate_item, item_features, train_purchases, and train_sessions
from pyspark.pandas import DataFrame
from start_session import *


class DatasetEnum(enum.Enum):
    candidate_items = "candidate_items"
    item_features = "item_features"
    train_purchases = "train_purchases"
    train_sessions = "train_sessions"


class Dataset:
    candidate_items_path: str = "dressipi_recsys2022/candidate_items.csv"
    item_features_path: str = "dressipi_recsys2022/item_features.csv"
    train_purchases_path: str = "dressipi_recsys2022/train_purchases.csv"
    train_sessions_path: str = "dressipi_recsys2022/train_sessions.csv"

    candidate_items: DataFrame = None
    item_features: DataFrame = None
    train_purchases: DataFrame = None
    train_sessions: DataFrame = None

    def __init__(self):
        # if folder is not exist, raise
        if not os.path.exists("dressipi_recsys2022"):
            raise FileNotFoundError("Il faut mettre le contenu du .zip au même niveau que le current working "
                                    "directory.\n "
                                    f"Le CWD est {os.getcwd()}")
        for path in [self.candidate_items_path, self.item_features_path, self.train_purchases_path, self.train_sessions_path]:
            if not os.path.exists(path):
                raise FileNotFoundError(f"{path} not found")

    def load_dataset(self, dataset: DatasetEnum):
        if dataset == DatasetEnum.candidate_items:
            # create a SparkSession dataframe from csv file
            self.candidate_items = SPARK.read.csv(self.candidate_items_path, header=True)
            # change column type to int
            self.candidate_items = self.candidate_items.withColumn("item_id", self.candidate_items["item_id"].cast("int"))

        elif dataset == DatasetEnum.item_features:
            self.item_features = SPARK.read.csv(self.item_features_path, header=True)
            # self.item_features = pd.read_csv(self.item_features_path)
            # self.item_features.item_id = self.item_features.item_id.astype(int)
            # self.item_features.feature_category_id = self.item_features.feature_category_id.astype(int)
            # self.item_features.feature_value_id = self.item_features.feature_value_id.astype(int)

        elif dataset == DatasetEnum.train_purchases:
            self.train_purchases = SPARK.read.csv(self.train_purchases_path, header=True)
            # self.train_purchases = pd.read_csv(self.train_purchases_path)
            # self.train_purchases.session_id = self.train_purchases.session_id.astype(int)
            # self.train_purchases.item_id = self.train_purchases.item_id.astype(int)
            # self.train_purchases.date = pd.to_datetime(self.train_purchases.date)

        elif dataset == DatasetEnum.train_sessions:
            self.train_sessions = SPARK.read.csv(self.train_sessions_path, header=True)
            # self.train_sessions = pd.read_csv(self.train_sessions_path)
            # self.train_sessions.session_id = self.train_sessions.session_id.astype(int)
            # self.train_sessions.item_id = self.train_sessions.item_id.astype(int)
            # self.train_sessions.date = pd.to_datetime(self.train_sessions.date)


