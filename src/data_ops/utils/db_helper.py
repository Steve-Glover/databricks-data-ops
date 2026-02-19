from databricks.connect import DatabricksSession
from pyspark.dbutils import DBUtils

# from pyspark.sql import SparkSession

""" 
db_helper.utils.py provides a collection of utilities to enable
python modules and scripts to interact with the databricks enviornment 
"""

spark = DatabricksSession.builder.getOrCreate()

def get_spark():
    return DatabricksSession.builder.getOrCreate()

def get_catalog(match_str:str)->str:
    """
    returns the first catelog name that matches the search string in the databricks workspace

    Args:
        match_str (str): Substring to match catalog names against.

    Returns:
        list: first string of the  catalog names containing the match_str.
    """
    catalogs_df = spark.sql("SHOW CATALOGS")
    catalog_names = [row.catalog for row in catalogs_df.collect() if match_str in row.catalog]
    return catalog_names[0]


def get_dbutils():
    """init dbutils in python scripts"""
    dbutils = DBUtils(spark)      
    return dbutils


def run_with_retry(notebook, timeout, args={}, max_retries=3):
    """ Run databricks notebook. Unpon timeout or exception attempt to 
        execute max_retries times.
    """
    dbutils = get_dbutils()
    num_retries = 0
    while True:
        try:
            return dbutils.notebook.run(notebook, timeout, args)
        except Exception as e:
            if num_retries >= max_retries:
                raise e
            else:
                print("Retrying error", e)
                num_retries += 1

dbutils = get_dbutils()

if __name__ == "__main__":
    print(get_catalog("cda_ds"))
