### A data transform Spark application running in local mode
### Assumptions: 
1) AwesomeWorkflow and ReceiptScanner both pushes daily files to the same bucket following naming conventions.
File names include date keys that is the same date with ```Date``` field in the data. i.e.  AwesomeWorkflow_2018-09-15.csv
2) Timestamps in both systems are in UTC. In this example the job run on 2018-09-16, to process the data from the previous date 2018-09-15.

### High level solution description:
The data processing workflow is batch processed. 
Each day using scheduling tools such as Airflow, a S3 sensor would monitor the arrival of these files for a run date. S3 event notifications can also be used , with a sensor to wait for file arrival notifications from Amazon SQS queue.

Once the files arrive, data transform task would be triggered to aggregate two files and generate a parquet output.

The data transform application takes one csv and one json file, to produce a parquet output.
The output parquet is partitioned by date for later querying or downstream applications.


Output can be read by:

    testdf = spark.read.parquet("output/combined_usage.parquet/")
    testdf.show()


#### To run the application in local mode and see the console output:
1) clone project
2) when open the project with IDE, usually it will prompt to install the dependencies in requirements.txt
Alternatively use pip to install the dependencies.
3) In IDE like PyCharm, right click on class name to run ```main.py```. In terminal, run ```python3 Main.py```, or run ```pytest test.py```

A test case is created to test when the log in time is at the very beginning of the day, 
or when a user log in one day and log out the following day. 
The date key should be consistent with the log in time, and usage duration should be calculated correctly.

Sample input and output:

```
input : 
+------+----------+-------------------+-------------------+----------+---------------+-------------+
|UserId|XeroUserId|          LoginTime|         LogoutTime|      Date|        Product|UsageDuration|
+------+----------+-------------------+-------------------+----------+---------------+-------------+
|     1|      1001|2018-09-15 15:00:00|2018-09-15 15:04:00|2018-09-15|AwesomeWorkflow|          240|
|     2|      1002|2018-09-15 14:00:00|2018-09-15 15:00:00|2018-09-15|AwesomeWorkflow|         3600|
+------+----------+-------------------+-------------------+----------+---------------+-------------+

+-------+----------+-------------------+-------------------+----------+--------------+-------------+
| UserId|XeroUserId|          LoginTime|         LogoutTime|      Date|       Product|UsageDuration|
+-------+----------+-------------------+-------------------+----------+--------------+-------------+
|User007|      1001|2018-09-15 23:00:00|2018-09-15 23:30:00|2018-09-15|ReceiptScanner|         1800|
| User32|      1003|2018-09-15 14:00:00|2018-09-15 15:00:00|2018-09-15|ReceiptScanner|         3600|
+-------+----------+-------------------+-------------------+----------+--------------+-------------+

output is:
+----------+----------+---------------+-------------+
|XeroUserId|      Date|        Product|UsageDuration|
+----------+----------+---------------+-------------+
|      1001|2018-09-15|AwesomeWorkflow|          240|
|      1002|2018-09-15|AwesomeWorkflow|         3600|
|      1001|2018-09-15| ReceiptScanner|         1800|
|      1003|2018-09-15| ReceiptScanner|         3600|
+----------+----------+---------------+-------------+
```