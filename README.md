### A data transform Spark application running in local mode
####Assumptions: 
1) AwesomeWorkflow and ReceiptScanner both pushes daily files to the same bucket following naming conventions.
File names include date keys. i.e.  AwesomeWorkflow_2018-09-15.csv
2) Timestamps in both systems are in UTC. In this example the job run on 2018-09-16, to process the data from the previous date 2018-09-15.

####High level solution description:
The data processing workflow is batch processed. 
Each day using scheduling tools such as Airflow, a S3 sensor would monitor the arrival of these files. S3 event notifications can also be used , with sensor to wait for file arrival notifications from Amazon SQS queue.

Once the files arrive, data transform task would be triggered to aggregate two files and generate a parquet output.

The data transform application takes one csv and one json file, to produce a parquet output.
The output parquet is partitioned by date for later querying or downstream applications.


Output can be read by:

    testdf = spark.read.parquet("output/combined_usage.parquet/")
    testdf.show()


#### To run the application in local mode and see the console output:
1) clone project
2) when open the project with IDE, usually it will install the dependencies in requirements.txt
Alternatively use pip to install the dependencies.
3) run ```Main.py```, or in terminal run ```pytest test.py```
    

