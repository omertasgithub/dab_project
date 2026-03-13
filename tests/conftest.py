import sys
import os
import pytest

# Run the tests from the root directory
sys.path.append(os.getcwd())

# Returning a Spark Session
'''
@pytest.fixture()
def spark():
    try:
        from databricks.connect import DatabricksSession
        spark = DatabricksSession.builder.getOrCreate()
    except ImportError: 
        try:
            from pyspark.sql import SparkSession
            spark = SparkSession.builder.getOrCreate()
        except:
            raise ImportError("Neither Databricks Session or Spark Session are available")
    return spark 
 
'''
'''

@pytest.fixture()
def spark():
    try:
        from databricks.connect import DatabricksSession
        spark = DatabricksSession.builder.getOrCreate()
    except Exception:   # catch ANY failure (auth, connection, etc.)
        from pyspark.sql import SparkSession
        spark = SparkSession.builder \
            .master("local[*]") \
            .appName("pytest") \
            .getOrCreate()

    return spark
'''

@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("pytest") \
        .getOrCreate()

    yield spark
    spark.stop()