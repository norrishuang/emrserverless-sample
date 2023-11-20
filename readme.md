
## EMRServerless Sample

本项目提供在 [Amazon EMR Serverless](https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/emr-serverless.html) 上提交任务的方法和例子。包括通过aws cli，MWAA（airflow），DophinScheduler等方式提交。

* Airflow 调用 EMR Serverless 的方法

    * Airflow DAG 参考 airflow/spark-tpcds-glue-catalog.py

  
    * 通过Spark运行HiveSQL的方法，参考代码 spark-sample/SparkForHiveSQL.py
        ```shell
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
        ```

* DophinScheduler 调用 EMR Serverless 的方法
   * 由于使用aws cli命令启动EMR Serverless任务是异步的，因此需要在脚本中保持监控job执行的状态。参考[dolphinscheduler.sh](./dolphinscheduler-sample/dolphinscheduler.sh)