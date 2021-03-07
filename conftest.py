from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession

import pytest


@pytest.fixture(scope='session')
def spark_test_session():
    spark_test_session = SparkSession.builder.appName("TestApp").master("local").getOrCreate()
    spark_test_session.conf.set("spark.sql.session.timeZone", "UTC")
    yield spark_test_session
    spark_test_session.stop()
