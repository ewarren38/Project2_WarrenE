# Part 1
# Author: Eleanor Warren
# Class: ST 554 (601)
# Date: 3/24/26

# Import modules
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from functools import reduce
from pyspark.sql.types import *
import pandas as pd

class SparkDataCheck:
    
    def __init__(self, dataframe):
        self.df = dataframe

    # create an instance of our class while reading in a csv
    @classmethod
    def from_csv(self, session, path):
        # SparkDataCheck = self
        self.file = spark.read.load(path,
                     format="csv", 
                     sep=",", 
                     inferSchema="true", 
                     header="true")
        # return SparkDataCheck
        return self

    
  