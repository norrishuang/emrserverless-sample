
# 向 EMR Serverless 提交 SparkSQL任务 Glue Data Catalog
SPARK_APPLICATION_ID=00fborselo6u0g09
JOB_ROLE_ARN=arn:aws:iam::812046859005:role/EMR-Serverless-HMS-EMRServerlessJobRole-16C21VVE86SKI
S3_BUCKET=emr-hive-us-east-1-812046859005
MariaDBHost=mysql-db-01-v8.cghfgy0zyjlk.us-east-1.rds.amazonaws.com
SECRET_ID=rds-users-credentials
DBUSER=$(aws secretsmanager get-secret-value --secret-id $SECRET_ID | jq --raw-output '.SecretString' | jq -r .MasterUsername)
DBPASSWORD=$(aws secretsmanager get-secret-value --secret-id $SECRET_ID | jq --raw-output '.SecretString' | jq -r .MasterUserPassword)
JDBCDriverClass=org.mariadb.jdbc.Driver
JDBCDriver=mariadb-connector-java.jar
SPARKSQLFILE=s3://emr-hive-us-east-1-812046859005/tpcds_2_4/q5.sql
JOBNAME=SparkSQL_TPCDS_Q5
# PARAM01='2020-01-02'
# HOUR='18'

JOBID=$(aws emr-serverless start-job-run \
  --name $JOBNAME \
  --application-id $SPARK_APPLICATION_ID \
  --execution-role-arn $JOB_ROLE_ARN \
  --job-driver '{
      "sparkSubmit": {
          "entryPoint": "s3://'${S3_BUCKET}'/SparkForHiveSQL.py",
          "entryPointArguments":["-f", "'${SPARKSQLFILE}'", "-s", "'${S3_BUCKET}'" ,"-d", "tpcds"],
          "sparkSubmitParameters": 
          "--conf spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory --conf spark.driver.cores=2 --conf spark.executor.memory=4G --conf spark.driver.memory=2G --conf spark.executor.cores=2"
        }
     }' \
    --configuration-overrides '{
        "monitoringConfiguration": {
        "s3MonitoringConfiguration": {
            "logUri": "s3://'${S3_BUCKET}'/sparklogs/"
        }
    }
}' | jq --raw-output '.jobRunId')


# get job status
JOB_STATE='START'
while [ $JOB_STATE != 'COMPLETED' ] && [ $JOB_STATE != 'FAILED' ]
    JOB_STATE=$(aws emr-serverless get-job-run \
        --application-id $SPARK_APPLICATION_ID \
        --job-run-id $JOBID | jq --raw-output '.jobRun.state')
    sleep 5

echo "Job State: $JOB_STATE"
RET=1
if [ $JOB_STATE == 'COMPLETED' ] 
then
    RET = 0
else
    RET = 1
fi

return $RET