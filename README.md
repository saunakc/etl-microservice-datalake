# etl-microservice-datalake

The ETL microservice works on exporting Redshift tables into s3 data lake. Partitioned tables are stored in s3 with Hive compatible paritioned format as partition_key=partition_value folder path. This storage format makes it easy to get consumed by AWS
analytical services such as Glue, Athena, Redshift Spectrum, EMR.

Customer needs to schedule the microservice via Cloudwatch Events Rule with target as a Lambda Function. The target will need the below parameters to pass on to the Lambda Function in the form of JSON input text.

    prefixedTablename = 'PrefixedTablename',
    snapshotYN = 'Y',
    fileFormat = 'ORC',
    writeMode = 'append',
    s3FilePath = 's3://mybucket/finance/',
    jdbcUrl = 'jdbc:redshift://mycluster.csdiadofcvnr.us-east-1.redshift.amazonaws.com:8192/mydatabase',
    glueRSConnection = 'glue_connection',
    redshiftRoleArn = 'arn:aws:iam::000123456789:role/redshiftrole'

The flow of scripts calling is as below:
Cloudwatch Events Rule > Lambda Function > Lambda Function calls Glue Script
