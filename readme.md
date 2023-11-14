
## EMRServerless Sample

* Airflow 调用 EMR Serverless 的方法

    * Airflow DAG 参考 airflow/spark-tpcds-glue-catalog.py

  
    * 通过Spark运行HiveSQL的方法，参考代码 spark-sample/SparkForHiveSQL.py
        ''' shell
        aws emr-serverless start-job-run \
            --application-id $SPARK_APPLICATION_ID \
        --execution-role-arn $JOB_ROLE_ARN \
        --job-driver '{
            "sparkSubmit": {
                "entryPoint": "s3://'${S3_BUCKET}'/SparkJobSample.py",
                "entryPointArguments":["-f","'${SPARKSQLFILE}'","-s","'${S3_BUCKET}'","--hivevar","DT=\"'${PARAM01}'\"","--hivevar","HOUR=\"'${HOUR}'\""],
                "sparkSubmitParameters": "--jars s3://'${S3_BUCKET}'/'${JDBCDriver}' --conf spark.hadoop.javax.jdo.option.ConnectionDriverName='${JDBCDriverClass}' --conf spark.hadoop.javax.jdo.option.ConnectionUserName='${DBUSER}' --conf spark.hadoop.javax.jdo.option.ConnectionPassword='${DBPASSWORD}' --conf spark.hadoop.javax.jdo.option.ConnectionURL=\"jdbc:mariadb://'${MariaDBHost}':3306/hivemetastore\"  --conf spark.driver.cores=2 --conf spark.executor.memory=2G --conf spark.driver.memory=2G --conf spark.executor.cores=2"
                }
            }' \
            --configuration-overrides '{
                "monitoringConfiguration": {
                "s3MonitoringConfiguration": {
                    "logUri": "s3://'${S3_BUCKET}'/sparklogs/"
                }
            }
        }'
        '''

* DophinScheduler 调用 EMR Serverless 的方法