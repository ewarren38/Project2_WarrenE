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
    """
    Data quality check
    """    
    def __init__(self, df: DataFrame):
        self.df = df

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
        return self(df)

    
    # Create an instance of our class from a standard pandas dataframe
    @classmethod
    def from_pandas(self, spark, pdf):
        df = spark.createDataFrame(pdf)
        return self(df)
    
    # Method to check if the values of a numeric column are within bounds
    def check_limits(self, column: str, lower = None, upper = None):
        """
        Add a Boolean column to our dataframe that indicates whether the values of another
        column fall within a numeric range
        """
        # first check that at least one bound was supplied
        if (lower != None) | (upper != None):
            
            # then check that the column passed is numeric using pyspark.sql.types
            if isintance(self.df[column].dtype, NumericType):

                # If we have a lower and upper bound
                if (lower != None) & (upper != None):
                    
                    codeBool = udf(lambda x: x.between(lower, upper, inclusive = 'both') if x != NULL else NULL)
                    self.df.withColumn('inBounds', codeBool(column))

                    #self.df \
                    #    .withColumn("inBounds", self.df[column].between(lower, upper, inclusive = 'both'))
                
                # If we have only an upper bound
                elif (lower == None) & (upper != None):
                    codeBool = udf(lambda x: x <= upper if x != NULL else NULL)
                    self.df.withColumn('inBounds', codeBool(column))
                    
                # If we have only a lower bound
                elif (lower != None) & (upper == None):                      
                    codeBool = udf(lambda x: x >= lower if x != NULL else NULL)
                    self.df.withColumn('inBounds', codeBool(column))
                
                # If no bounds were given
                else:
                    print("No upper or lower bounds provided")    
        else:              
            print("Supplied column is not numeric type. No changes have been made to the dataframe.")
    
        # always return self
        return self
    
    # Method to check if the values of a string column are in a given list of levels
    def check_string(self, column: str, levels: list):
        """
        Add a Boolean column to our dataframe that indicates whether the values
        of another column are found in the list of levels given
        """
        # check that the column passed is string using pyspark.sql.types
        if isintance(self.df[column].dtype, StringType):
            codeBool = udf(lambda x: x.isin(levels) if x!= NULL else NULL)
            self.df.withColumn('inLevels', codeBool(column))
        else:
            print("Supplied column is not string type. No changes have been made to the dataframe.")
        return self

    # Method to check if the values of a column are NULL
    def check_missing(self, column):
        """
        Add a Boolean column to our dataframe that indicates if the values
        of another column are missing (NULL)
        """
        self.df.withColumn('isMissing', self.df[column].isNULL())
        return self