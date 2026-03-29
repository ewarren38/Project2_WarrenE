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
        return self(df) # return our instance with this specific df as its DataFrame
    
    
    # Create an instance of our class from a standard pandas dataframe
    @classmethod
    def from_pandas(self, spark, pdf):
        df = spark.createDataFrame(pdf)
        return self(df) # return our instance with this specific df as its DataFrame
    
    
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
    
    
    # Summarization method to find the min and max values of numeric columns
    def get_minmax(self, column: str = None, group: str = None):
        # Check if we were given a column
        if column != None:
            # If so, is it numeric?
            if isinstance(self.df.schema[column].dataType, T.NumericType):
                # If so, check if we were also given a grouping column
                if group != None:
                    # For a numeric column with a grouping column, find the min and max values per group
                    sum_df = self.df \
                        .groupBy(group) \
                        .agg(F.min(column), F.max(column))
                    p_df = sum_df.toPandas()
                # For a numeric column with no grouping, find it's min and max values
                else:
                    sum_df = self.df \
                        .agg(F.min(column), F.max(column))
                    p_df = sum_df.toPandas() # convert to a regular pandas dataframe
                return p_df
            # If the column isn't numeric, print Error.
            else:
                print("Error: The supplied column is not numeric type")
                return None
        # If no columns were specified, perform calculation for all the numeric cols in dataframe
        else:
            # Get a list of column names for our numeric columns only
            num_list = [col for col in self.df.columns if isinstance(self.df.schema[col].dataType, T.NumericType)]
            # Initialize a pandas dataframe to store the min and max for each numeric column.
            p_df = pd.DataFrame()
            # If we were given a grouping variable
            if group != None:
                # For all numeric columns, group, calculate min and max, then concat to our pandas DF
                for i in num_list:
                    sum_df = self.df.groupBy(group).agg(F.min(i), F.max(i)).toPandas()
                    p_df = pd.concat([p_df, sum_df], axis = 1)
            # If we were not given a grouping variable
            else:
                # For all numeric cols, calculate min and max, concatenate
                for i in num_list:
                    sum_df = self.df.agg(F.min(i), F.max(i)).toPandas()
                    p_df = pd.concat([p_df, sum_df], axis = 1)
            return p_df
        
    
    # Create a method that counts the number of unique values found in either one string
    # variable or across the combinations of two string variables.
    def get_stringlevels(self, col1: str, col2: str = None): # default for second column is None
        # First, check if we were given a string column in col1
        if isinstance(self.df.schema[col1].dataType, T.StringType):
            # Then check if we were given a second column at all
            if col2 != None:
                # If so, is col2 string as well?
                if isinstance(self.df.schema[col2].dataType, T.StringType):
                    # If col1 and col2 are both strings, find the unique groupings and count
                    count = self.df \
                    .groupby([col1, col2]) \
                    .agg(F.count("*") \
                    .alias("count")) \
                    .show() # show all of them
                    return count
                # If col2 is not a string, print warning
                else:
                    print("Warning: Please ensure that all supplied columns are of StringType.")
            # If we weren't given a second column
            else:
                # Find counts for unique levels of col1 only
                count = self.df \
                    .groupBy(col1) \
                    .agg(F.count(col1) \
                    .alias("count")) \
                    .show()
                return count
        # If col1 isn't StringType, print Error.
        else:
            print("Error: All supplied columns are not StringType. Please supply a string column.")
