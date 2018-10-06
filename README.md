# etl-microservice-datalake
The Lambda function described here can be used as the ETL microservice for Redshift tables' data unload- per table per partition. The Lambda function accepts necessary parameters to identify which table to unload, optionally if the table is paritioned you can provide the partition value. You will also provide the unload destination path as s3 location of your data lake, the fileformat of the unloaded data. For the full list of available paramters check the AWS Glue Job section.

The lambda function will create a AWS Glue job. The name of the job will be <schemaname>.<tablename>_ExtractToDataLake where schemaname.tablename is the prefixed table name in your Redshift cluster. This job will show up in your AWS console under AWS Glue > Jobs. As an example if the lambda function accepts "demo_master.orders" as the parameter- tablename the corresponding AWS Glue job will be demo_master.orders_ExtractToDataLake.
 
Any subsequent invocation of the lambda function with the same tablename as its input parameter will instantiate a new job run id. This way you can browse the AWS Glue job by entering the schemamame.tablename format in the AWS Glue > Jobs page. Just keep in mind the number of jobs created by the Lambda function will be counted towards the AWS Glue Limits which is 25 jobs per AWS account. This is a soft limit and you can request a limit increase by contacting the AWS Support.
 
**AWS Glue Job:**

The AWS Glue job used as a pyspark code. This pyspark code is a generic code for unloading any Redshift table- partitioned or non-partitioned, into s3. This example shows the pyspark code is stored in an s3 bucket which is owned by the AWS account owner. This s3 bucket gets created by AWS Glue when you create a AWS Glue job from the AWS console and the bucket name looks like aws-Glue-scripts-<aws-account-number>-<region>. 
    
When this pyspark code is called by the AWS Lambda function, it accepts a number of mandatory parameters.
* s3 Path for Data Lake: includes the bucket name plus any additional path with the bucket name as the root.
* JDBC url: should in a format jdbc:redshift://<redshift-endpoint>:<redshift-port>/<redshift-database>?user=<redshift-dbuser>&password=<redshift-password. You can find the Redshift endpoint from the AWS Redshift console. The database user should have select privilege on the table.
* Redshift Role ARN: this IAM role must have s3 read and write policy to the bucket which you want to setup as data lake. For more information on setting up bucket policy to s3 bucket check this link. Also this IAM role should be attached to the Redshift cluster as described here.
* Tablename prefixed with schemaname.

The optional parameters are 
* WriteMode which can take any value documented as SaveMode under Spark.
* FileFormat which can be csv, orc or parquet.
* snapshotYN: a  Y/N value which tells whether the full table unload or partition unload.
* DayPartitionKey which is the string for the partition key column name in the table.
* DayPartitionValue the string value for the partition key
