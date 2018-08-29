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

import boto3
from datetime import datetime, timedelta

this_session = boto3.session.Session()
currentRegion = this_session.region_name

currentAccount = boto3.client('sts').get_caller_identity()['Account']

glue = boto3.client(service_name='glue', region_name=currentRegion, endpoint_url='https://glue.us-east-1.amazonaws.com')

def lambda_handler(event, context):
# TODO implement
print "Event is: %s" %event

if event is not None:
    prefixedTablename = event['PrefixedTablename']
    snapshotYN = event['SnapshotYN']
    fileFormat = event['FileFormat']
    writeMode = event['WriteMode']
    s3FilePath = event['s3FilePath']
    jdbcUrl = event['JdbcUrl']
    glueRSConnection = event['GlueRSConnection']
    redshiftRoleArn = event['RedshiftRoleArn']
else:
    print "Insufficient paramaters. Aborting ..."

## Retrieve snapshot based partition from current system time. Current snapshot is sysdate - 1.
prev_day_date_time = datetime.now() - timedelta(days = 1)
print prev_day_date_time
day_partition_value = "snapshot_day=%s" %prev_day_date_time.strftime("%Y-%m-%d")

print day_partition_value
print "writeMode: %s" %writeMode
print "prefixedTablename: %s" %prefixedTablename
print "snapshotYN: %s" %snapshotYN
print "fileFormat: %s" %fileFormat


## Create Glue job with the parameters provided in the input
myJob = glue.create_job(Name=prefixedTablename + "_ExtractToDataLake", \
                        Role='AWSGlueServiceRoleDefault', \
                        Command={'Name': 'glueetl', 'ScriptLocation': 's3://aws-glue-scripts-' +
                        currentAccount + '-' + currentRegion + '//ExtractToDataLake'},\
                        Connections= {'Connections' : glueRSConnection},\
                        MaxRetries = 1, \
                        ExecutionProperty = {'MaxConcurrentRuns': 1}, \
                        DefaultArguments = {"--TempDir": "s3://aws-glue-temporary-" + currentAccount + '-' + currentRegion + "/" }
                        )
#myNewJobRun = glue.start_job_run(JobName=myJob['Name'])
myNewJobRun = glue.start_job_run( \
                        JobName=myJob['Name'], \
                        Arguments = { \
                                    '--prefixed_table_name': prefixedTablename, \
                                    '--write_mode': writeMode, \
                                    '--snapshot_yn': snapshotYN, \
                                    '--file_format': fileFormat, \
                                    '--s3path_datalake' : s3FilePath, \
                                    '--jdbc_url' :jdbcUrl, \
                                    '--redshift_role_arn' : redshiftRoleArn, \
                                    '--day_partition_key':   'partition_0', \
                                    '--day_partition_value':  day_partition_value} 
                                 )
## Glue returns control here immediately 
print "Jobrun ID %s" %myNewJobRun['JobRunId']
status = glue.get_job_run(JobName=myJob['Name'], RunId=myNewJobRun['JobRunId'])
print "Jobrun status %s" %status['JobRun']['JobRunState']
