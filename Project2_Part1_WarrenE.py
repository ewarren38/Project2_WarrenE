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
    def from_csv(self, spark, path):
        # SparkDataCheck = self
        df = spark.read.load(path,
                     format="csv", 
                     sep=",", 
                     inferSchema="true", 
                     header="true")
        # return SparkDataCheck
        return self

    
    # create an instance of our class from a standard pandas dataframe
    @classmethod
    def from_pandas(self, spark, pd):
        df = spark.createDataFrame(pd)
        return self
    
    
    def check_limits(self, column, lower = None, upper = None):
        # first check that at least one bound was supplied
        if (lower != None) | (upper != None):
            
            # then check that the column passed in numeric using pyspark.sql.types
            if isintance(df.column.dtype, NumericType):

                # If we have a lower and upper bound
                if (lower != None) & (upper != None):
                    
                    codeBool = udf(lambda x: x.between(lower, upper, inclusive = 'both') if x != NULL else NULL)
                    self.df.withColumn('inBounds', codeBool(col))

                    self.df \
                        .withColumn("inBounds", self.df.col.between(lower, upper, inclusive = 'both'))
                # If we have only an upper bound
                elif (lower == None) & (upper != None):
                    self.df \
                        .withColumn("inBounds", self.df.col.between(lower, upper, inclusive = 'right'))
                # If we have only a lower bound
                elif (lower != None) & (upper == None):                      
                    self.df \
                        .withColumn("inBounds", self.df.col.between(lower, upper, inclusive = 'left'))
                else: #both are NULL
    
        else:              
            print("Supplied column is not numeric type. No changes have been made to the dataframe.")
    
        # always return self
        return self
     
       