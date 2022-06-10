import json
import logging
import os
import pytest
import random
import requests_mock
import string
import tempfile

from dbx import export_parquet, file_exists, get_dir_content, get_dataframe_from_parquet

from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession

class TestDbx():
    def __init__(self):
        self.spark = SparkSession.builder.appName('dbx_tests').getOrCreate()
        print('Hello there')
        # Call another definition.
        self.andBye()

    def test_export_parquet(self):
        print('HELLO')
        parquet_path = '/data/in/files/parquet'
        df = get_df()
        df.write.mode('overwrite').partitionBy('Dept').parquet(parquet_path)
        export_parquet(parquet_path, self.spark)
        self.assertEqual(True, False)

    def __get_df(self):
        data = [("James", "Sales", 3000),
                ("Michael", "Sales", 4600),
                ("Robert", "Sales", 4100),
                ("Maria", "Finance", 3000),
                ("James", "Sales", 3000),
                ("Scott", "Finance", 3300),
                ("Jen", "Finance", 3900),
                ("Jeff", "Marketing", 3000),
                ("Kumar", "Marketing", 2000),
                ("Saif", "Sales", 4100)
                ]
        columns = ["Name", "Dept", "Salary"]
        return self.spark.createDataFrame(data=data, schema=columns)


