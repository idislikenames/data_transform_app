# An data transformation application that takes two files in input folder and create a parquet in output folder.

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType,LongType,TimestampType
from pyspark.sql.functions import lit, col, date_format
from datetime import datetime, timedelta


def read_df(spark, file_aw, file_rs):
    input_schema = StructType([
        StructField("UserId", StringType()),
        StructField("XeroUserId", StringType()),
        StructField("LoginTime", TimestampType(), True),
        StructField("LogoutTime", TimestampType(), True)
    ])

    date_key_filter = datetime.strftime(datetime.strptime("2018-09-15", "%Y-%m-%d"), '%Y-%m-%d')

    df_aw_raw = spark.read.csv(f"input/{file_aw}_{date_key_filter}.csv", header=True, schema=input_schema)
    df_rs_raw = spark.read.json(f"input/{file_rs}_{date_key_filter}.json", schema=input_schema, multiLine=True)

    return df_aw_raw, df_rs_raw, date_key_filter


def combine_df(df_aw_raw, df_rs_raw, date_filter):
    df_aw = df_aw_raw.withColumn("Date", date_format(col('LoginTime'), "yyyy-MM-dd").cast("date")) \
        .withColumn("Product", lit("AwesomeWorkflow")) \
        .withColumn("UsageDuration", col("LogoutTime").cast(LongType()) - col("LoginTime").cast(LongType()))

    df_aw.show()

    df_rs = df_rs_raw.withColumn("Date", date_format(col('LoginTime'), "yyyy-MM-dd").cast("date")) \
        .withColumn("Product", lit("ReceiptScanner")) \
        .withColumn("UsageDuration", col("LogoutTime").cast(LongType()) - col("LoginTime").cast(LongType()))

    df_rs.show()

    #depending on local setting is might needed to point dirver and executors to the same python.
    #import os
    #os.environ["PYSPARK_PYTHON"] = "path_to_local_python/venv/bin/python3.7"
    #os.environ["PYSPARK_DRIVER_PYTHON"] = "path_to_local_python/venv/bin/python3.7"

    output= df_aw.union(df_rs).drop("UserId","LoginTime","LogoutTime")
    # show output
    print("output is:")
    output.show()
    # write using date as partition
    output_partition = output.drop("Date")
    output_path = f"output/combined_usage.parquet/Date={date_filter}"
    output_partition.write.mode("overwrite").parquet(output_path)

    # return df with date key for testing
    return output


if __name__ == '__main__':
    spark = SparkSession.builder.appName("SimpleApp").master("local").getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    file_aw_name = "AwesomeWorkflow"
    file_rs_name = "ReceiptScanner"
    try:
        input_aw, input_rs, date_filter = read_df(spark,file_aw_name,file_rs_name)
        combine_df(input_aw, input_rs, date_filter)
    finally:
        spark.stop()
