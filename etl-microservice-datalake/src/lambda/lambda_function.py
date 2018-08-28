#!/usr/bin/env python

from __future__ import print_function

# Copyright 2016-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
# Licensed under the Apache License, Version 2.0 (the "License"). You may not
use this file except in compliance with the License. A copy of the License is
located at
# http://aws.amazon.com/apache2.0/
# or in the "license" file accompanying this file. This file is distributed on
an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
or implied. See the License for the specific language governing permissions and
limitations under the License.

import sys
import os
import boto3
import base64
import datetime
from boto.kms.exceptions
import NotFoundException
from awsglue.transforms
import *
from awsglue.utils
import getResolvedOptions
from pyspark.context
import SparkContext
from awsglue.context
import GlueContext
from awsglue.job
import Job
from pyspark.sql
import Row
from pyspark.sql
import functions
from pyspark.sql
import SQLContext

## @params: [TempDir, JOB_NAME]

args = getResolvedOptions(sys.argv, [ 'TempDir',
'JOB_NAME',
's3path_datalake',
'jdbc_url',
'redshift_role_arn',
'prefixed_table_name',
'write_mode',
'snapshot_yn',
'file_format',
'day_partition_key',
'day_partition_value'
])

this_session = boto3.session.Session()
currentRegion = this_session.region_name

cw = boto3.client('cloudwatch', region_name = currentRegion)

try:
cw.put_metric_data(
Namespace = 'Glue-ETLMS',
MetricData = [{
'MetricName': 'Invocation count',
'Dimensions': [{
'Name': 'TableName',
'Value': args['prefixed_table_name']
}],
'Timestamp': datetime.datetime.utcnow(),
'Value': 1,
'Unit': 'Count'
}]
)
except:
print "Reading argument from Lambda function failed: exception %s" %
sys.exc_info()[1]
cw.put_metric_data(
Namespace = 'Glue-ETLMS',
MetricData = [{
'MetricName': 'Invocation errors',
'Timestamp': datetime.datetime.utcnow(),
'Value': 1,
'Unit': 'Count'
}]
)
exit

filePath = args['s3path_datalake']
jdbcUrl =  args['jdbc_url']
redshiftRoleArn = args['redshift_role_arn']
prefixedTableName = args['prefixed_table_name']
snapshotYN = args['snapshot_yn']
writeMode = args['write_mode']
fileFormat = args['file_format']
dayPartitionKey = args['day_partition_key']
dayPartitionValue = args['day_partition_value']

schemaName = prefixedTableName.split(".")[0]
tableName = prefixedTableName.split(".")[1]

print "s3pathDatalake: %s" % s3pathDatalake
print "redshiftRoleArn: %s" % redshiftRoleArn
print "schemaName: %s" % schemaName
print "tableName: %s" % tableName
print "snapshotYN: %s" % snapshotYN
print "writeMode: %s" % writeMode
print "fileFormat: %s" % fileFormat
print "dayPartitionKey: %s" % dayPartitionKey
print "dayPartitionValue: %s" % dayPartitionValue

extract_sql = ""
filePath = ""

sc = SparkContext()
sql_context = SQLContext(sc)
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

if snapshotYN.lower() == 'n':
extract_sql = "select * from " + prefixedTableName
filePath = s3pathDatalake + schemaName + "/" + fileFormat + "/" + tableName +
"/"
elif snapshotYN.lower() == 'y':
extract_sql = "select * from %s where snapshot_day >= dateadd('day', -1, trunc(
sysdate)) order by snapshot_day" % prefixedTableName
filePath = s3pathDatalake + schemaName + "/" + fileFormat + "/" + tableName +
"/" + dayPartitionValue + "/"##
print "Filepath: %s" % s3pathDatalake
print "extract_sql: %s" % extract_sql
try:
print "Connecting to jdbc url %s" % jdbcUrl
datasource0 = sql_context.read\
.format("com.databricks.spark.redshift")\
.option("url", jdbcUrl)\
.option("query", extract_sql)\
.option("aws_iam_role", redshiftRoleArn)\
.option("tempdir", args['TempDir'])\
.load()## spark.read.jdbc(jdbcUrl, pushdown_query)
if snapshotYN.lower() == 'y':
datasource0 = datasource0.withColumnRenamed("snapshot_day", "snapshot_date")
datasource0.printSchema()
except: #print "JDBC connection to %s failed: exception %s" % jdbcUrl %
sys.exc_info()[1]
print(sys.exc_info()[1])
cw.put_metric_data(
Namespace = 'Glue-ETLMS',
MetricData = [{
'MetricName': 'Invocation errors',
'Dimensions': [{
'Name': 'TableName',
'Value': prefixedTableName
}],
'Timestamp': datetime.datetime.utcnow(),
'Value': 1,
'Unit': 'Count'
}]
)
exit
datasource0.printSchema()
try:
if fileFormat == 'csv':
datasource0.write.mode(writeMode).csv(filePath)
elif fileFormat == 'orc':
datasource0.write.mode(writeMode).orc(filePath)
elif fileFormat == 'parquet':
datasource0.write.mode(writeMode).parquet(filePath)
except: #print "Writing to s3 failed: %s" % sys.exc_info()[1]
print(sys.exc_info()[1])
cw.put_metric_data(
Namespace = 'Glue-ETLMS',
MetricData = [{
'MetricName': 'Invocation errors',
'Dimensions': [{
'Name': 'TableName',
'Value': prefixedTableName
}],
'Timestamp': datetime.datetime.utcnow(),
'Value': 1,
'Unit': 'Count'
}]
)
exit
job.commit()
