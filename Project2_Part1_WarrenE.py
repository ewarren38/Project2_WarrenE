# Part 1
# Author: Eleanor Warren
# Class: ST 554 (601)
# Date: 3/24/26

# Import modules
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from functools import reduce
import pyspark.sql.types as T # naming for easy reference
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
        df = spark.read.load(path,
                     format="csv", 
                     sep=",", 
                     inferSchema="true", 
                     header="true")
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
        # First check that at least one bound was supplied
        # If not, print 'no bounds provided'
        if (lower != None) | (upper != None):
            
            # Then check that the column passed is numeric using pyspark.sql.types
            # Had to use schema to get data type from our reference column
            if isinstance(self.df.schema[column].dataType, T.NumericType):
            
                # If we have both a lower and upper bound
                # PySpark uses "None" instead of "NULL"
                if (lower != None) & (upper != None):
                    self.df = self.df.withColumn("inBounds", \
                            F.when(self.df[column].isNotNull(), \
                            self.df[column].between(lower, upper)) \
                            .otherwise(None))
                    
                # If we have only an upper bound
                elif (lower == None) & (upper != None):
                    self.df = self.df.withColumn('inBounds', \
                            F.when(self.df[column].isNotNull(), \
                            self.df[column] <= upper) \
                            .otherwise(None))
                    
                # If we have only a lower bound
                elif (lower != None) & (upper == None):                 
                    self.df = self.df.withColumn('inBounds', \
                            F.when(self.df[column].isNotNull(), \
                            self.df[column] >= lower) \
                            .otherwise(None))                
                
            # If the column was non-numeric
            else:
                print("Supplied column is not numeric type. No changes have been made to the dataframe.")    
        
        # If no bounds were given
        else:              
            print("No upper or lower bounds provided")
    
        # Always return self
        return self
    
    # Method to check if the values of a string column are in a given list of levels
    def check_string(self, column: str, levels: list):
        """
        Add a Boolean column to our dataframe that indicates whether the values
        of another column are found in the list of levels given
        """
        # check that the column passed is string using pyspark.sql.types
        # find the data type of our column
        if isinstance(self.df.schema[column].dataType, T.StringType):
            self.df = self.df.withColumn("inLevels", \
                            F.when(self.df[column].isNotNull(), \
                            self.df[column].isin(levels)) \
                            .otherwise(None))
        else:
            print("Supplied column is not string type. No changes have been made to the dataframe.")
        return self

    # Method to check if the values of a column are NULL
    def check_missing(self, column):
        """
        Add a Boolean column to our dataframe that indicates if the values
        of another column are missing (NULL)
        """
        self.df = self.df.withColumn('isMissing', F.isnull(self.df[column]))
        return self