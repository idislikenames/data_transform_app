from main import combine_df
import pandas as pd
from datetime import datetime
from pyspark.sql.types import StructType,StructField, StringType,TimestampType,DateType,LongType


def test_filter_spark_data_frame(spark_test_session):
    input_aw_data = [("3", "1000",datetime.strptime("2018-09-15T00:00:00+00:00",'%Y-%m-%dT%H:%M:%S%z'),\
                      datetime.strptime("2018-09-15T00:01:00+00:00",'%Y-%m-%dT%H:%M:%S%z'))]
    input_rs_data = [("user1", "2000",datetime.strptime("2018-09-15T23:59:00+00:00",'%Y-%m-%dT%H:%M:%S%z'),\
        datetime.strptime("2018-09-16T00:01:00+00:00",'%Y-%m-%dT%H:%M:%S%z'))]
    input_schema = StructType([
        StructField("UserId", StringType()),
        StructField("XeroUserId", StringType()),
        StructField("LoginTime", TimestampType()),
        StructField("LogoutTime", TimestampType())
    ])

    aw_df = spark_test_session.createDataFrame(data=input_aw_data, schema=input_schema)
    rs_df = spark_test_session.createDataFrame(data=input_rs_data, schema=input_schema)

    out_data= [("1000",datetime.strptime("2018-09-15",'%Y-%m-%d'),"AwesomeWorkflow",60),("2000",datetime.strptime("2018-09-15",'%Y-%m-%d'),"ReceiptScanner",120)]
    out_schema =  StructType([
        StructField("XeroUserId", StringType()),
        StructField("Date", DateType()),
        StructField("Product", StringType()),
        StructField("UsageDuration", LongType())
    ])
    expected_output = spark_test_session.createDataFrame(data=out_data, schema=out_schema)
    real_output = combine_df(aw_df, rs_df,"2018-09-15")
    real_output.show()
    real_output = sort_df(
        real_output.toPandas(),
        ['XeroUserId', 'Date','Product','UsageDuration'],
    )
    expected_output = sort_df(
        expected_output.toPandas(),
        ['XeroUserId', 'Date','Product','UsageDuration'],
    )
    pd.testing.assert_frame_equal(expected_output, real_output, check_like=True)


def sort_df(data_frame, columns_list):
    return data_frame.sort_values(columns_list).reset_index(drop=True)

