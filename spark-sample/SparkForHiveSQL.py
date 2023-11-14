import sys
from datetime import datetime
import getopt
import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# SparkSQL for EMRServerless
# 该脚本用于使用SparkSQL进行HiveSQL语句的执行
# 脚本参数说明
#   [-f --sqlfile] HiveSQL脚本存放的S3路径，例如：s3://<s3-bucket>/sqlfiles/mysql-scrirpt.sql
#   [-s --s3bucket] 指定 spark.sql.warehouse.dir 路径的 s3 桶名称， s3://{s3buchet_name}/warehouse/
#   [-h --hivevar] Parameters input to the sql file. Example：—hivevar DT=’20220901’ —hivevar 
#   [-d --database] 指定hive数据库，默认 default

# EMR Serverless 举例
'''
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

if __name__ == "__main__":

    # print(len(sys.argv))
    if (len(sys.argv) == 0):
        print("Usage: spark-sql-executor [-f sqlfile,-s s3bucket,-h hivevar,-d database]")
        sys.exit(0)
    vSQLFile = ''
    vS3Bucket = ''

    logger = logging.getLogger()

    database = 'default'
    opts,args = getopt.getopt(sys.argv[1:],"f:s:h:d:",["sqlfile=","s3bucket=","hivevar="])
    for opt_name,opt_value in opts:
        if opt_name in ('-f','--sqlfile'):
            vSQLFile = opt_value
            logger.info("SQLFile:" + vSQLFile)
            print("SQLFile:" + vSQLFile)
        elif opt_name in ('-s','--s3bucket'):
            vS3Bucket = opt_value
            logger.info("S3Bucket:" + vS3Bucket)
            print("S3Bucket:" + vS3Bucket)
        elif opt_name in ('-h','--hivevar'):
            hivevar = opt_value
            exec(hivevar)
            print("hivevar:" + hivevar)
        elif opt_name in ('-d','--database'):
            database = opt_value
            print("database:" + database)
        else:
            logger.info("need parameters [sqlfile,s3bucket,hivevar or database]")
            exit()
    vWarehouse = "s3://" + vS3Bucket + "/warehouse/"
    logger.info("SQL File: " + vSQLFile)
    print("SQL File: " + vSQLFile)
    logger.info("Warehouse location: " + vWarehouse)

    spark = SparkSession \
        .builder \
        .config("spark.sql.warehouse.dir", vWarehouse) \
        .enableHiveSupport() \
        .getOrCreate()
    sc = spark.sparkContext
    rdd = sc.wholeTextFiles(vSQLFile)
    #从文件中获取内容
    vSqlContext = rdd.collect()[0][1]

    #处理换行符
    # rSql = vSqlContext.replace('\n', '')
    # rSql = vSqlContext
    #按分号拆分sql
    sqlList = vSqlContext.split(";",)

    # 处理 Hive SQL兼容性
    hiveSQLCompat = "set spark.sql.hive.convertMetastoreParquet = true"
    spark.sql(hiveSQLCompat)
    hiveSQLCompat = "set spark.sql.ansi.enabled = false"
    spark.sql(hiveSQLCompat)

    spark.sql(f'use {database}')
    #遍历 sqlList 执行, 需要从变量域中获取变量 format_map(vars())，因此sql中定义的变量格式 {parameter}
    for sql in sqlList:
        if sql.strip() != '':
            logger.info("execsql:" + sql)
            print("execsql:" + sql)
            spark.sql(sql.format_map(vars())).show()
