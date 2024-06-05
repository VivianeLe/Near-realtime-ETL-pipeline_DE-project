import os
import time
import datetime
from uuid import *
import pyspark.sql.functions as sf
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark import SparkConf, SparkContext
from pyspark.sql import Row
from pyspark.sql.functions import *
from pyspark.sql.window import Window as W
import mysql.connector
from kafka import KafkaProducer

def calculating_clicks(df):
    clicks_data = df.filter(df.custom_track == 'click')
    clicks_data = clicks_data.na.fill({'bid':0})
    clicks_data = clicks_data.na.fill({'job_id':0})
    clicks_data = clicks_data.na.fill({'publisher_id':0})
    clicks_data = clicks_data.na.fill({'group_id':0})
    clicks_data = clicks_data.na.fill({'campaign_id':0})
    clicks_data.registerTempTable('clicks')
    clicks_output = spark.sql("""select job_id , date(ts) as date , hour(ts) as hour , publisher_id , campaign_id , group_id , avg(bid) as bid_set, count(*) as clicks , sum(bid) as spend_hour, ts from clicks
    group by job_id , date(ts) , hour(ts) , publisher_id , campaign_id , group_id, ts """)
    return clicks_output 
    
def calculating_conversion(df):
    conversion_data = df.filter(df.custom_track == 'conversion')
    conversion_data = conversion_data.na.fill({'job_id':0})
    conversion_data = conversion_data.na.fill({'publisher_id':0})
    conversion_data = conversion_data.na.fill({'group_id':0})
    conversion_data = conversion_data.na.fill({'campaign_id':0})
    conversion_data.registerTempTable('conversion')
    conversion_output = spark.sql("""select job_id , date(ts) as date , hour(ts) as hour , publisher_id , campaign_id , group_id , count(*) as conversions, ts  from conversion
    group by job_id , date(ts) , hour(ts) , publisher_id , campaign_id , group_id, ts """)
    return conversion_output 
    
def calculating_qualified(df):    
    qualified_data = df.filter(df.custom_track == 'qualified')
    qualified_data = qualified_data.na.fill({'job_id':0})
    qualified_data = qualified_data.na.fill({'publisher_id':0})
    qualified_data = qualified_data.na.fill({'group_id':0})
    qualified_data = qualified_data.na.fill({'campaign_id':0})
    qualified_data.registerTempTable('qualified')
    qualified_output = spark.sql("""select job_id , date(ts) as date , hour(ts) as hour , publisher_id , campaign_id , group_id , count(*) as qualified, ts  from qualified
    group by job_id , date(ts) , hour(ts) , publisher_id , campaign_id , group_id, ts """)
    return qualified_output
    
def calculating_unqualified(df):
    unqualified_data = df.filter(df.custom_track == 'unqualified')
    unqualified_data = unqualified_data.na.fill({'job_id':0})
    unqualified_data = unqualified_data.na.fill({'publisher_id':0})
    unqualified_data = unqualified_data.na.fill({'group_id':0})
    unqualified_data = unqualified_data.na.fill({'campaign_id':0})
    unqualified_data.registerTempTable('unqualified')
    unqualified_output = spark.sql("""select job_id , date(ts) as date , hour(ts) as hour , publisher_id , campaign_id , group_id , count(*) as unqualified, ts  from unqualified
    group by job_id , date(ts) , hour(ts) , publisher_id , campaign_id , group_id, ts """)
    return unqualified_output
    
def process_final_data(clicks_output,conversion_output,qualified_output,unqualified_output):
    final_data = clicks_output.join(conversion_output,['job_id','date','hour','publisher_id','campaign_id','group_id', 'ts'],'full').\
    join(qualified_output,['job_id','date','hour','publisher_id','campaign_id','group_id','ts'],'full').\
    join(unqualified_output,['job_id','date','hour','publisher_id','campaign_id','group_id','ts'],'full')
    return final_data 
    
def process_cassandra_data(df):
    clicks_output = calculating_clicks(df)
    conversion_output = calculating_conversion(df)
    qualified_output = calculating_qualified(df)
    unqualified_output = calculating_unqualified(df)
    final_data = process_final_data(clicks_output,conversion_output,qualified_output,unqualified_output)
    return final_data
    
def retrieve_company_data():
    sql = """(SELECT id as job_id, company_id, group_id, campaign_id FROM job) test"""
    company = spark.read.format('jdbc').options(url=URL, driver=DRIVER, dbtable=sql, user=USER, password=PASSWORD).load()
    return company 
    
def import_to_kafka(output):
    output.selectExpr("CAST(job_id AS STRING) AS key", "to_json(struct(*)) AS value") \
    .write.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("topic", "json_data").save()
    return print('Data imported successfully')

def get_mysql_latest_time():    
    sql = """(select max(latest_update_time) from final) data"""
    mysql_time = spark.read.format('jdbc').options(url=URL, driver=DRIVER, dbtable=sql, user=USER, password=PASSWORD).load()
    mysql_time = mysql_time.take(1)[0][0]
    if mysql_time is None:
        mysql_latest = '1998-01-01 23:59:59'
    else :
        mysql_latest = mysql_time.strftime("%Y-%m-%d %H:%M:%S")
    return mysql_latest

def main_task(mysql_time):
    print('The host is ' ,HOST)
    print('The port using is ',PORT)
    print('The db using is ',DB_NAME)
    print('-----------------------------')
    print('Retrieving data from Cassandra')
    print('-----------------------------')
    df = spark.read.format("org.apache.spark.sql.cassandra").options(table="tracking",keyspace="de_pro").load().where(col('ts')>= mysql_time)
    print('-----------------------------')
    print('Selecting data from Cassandra')
    print('-----------------------------')
    df = df.select('ts','job_id','custom_track','bid','campaign_id','group_id','publisher_id')
    df = df.filter(df.job_id.isNotNull())
    df.printSchema()
#   process_df = process_df(df)
    print('-----------------------------')
    print('Processing Cassandra Output')
    print('-----------------------------')
    cassandra_output = process_cassandra_data(df)
    print('-----------------------------')
    print('Merge Company Data')
    print('-----------------------------')
    company = retrieve_company_data()
    print('-----------------------------')
    print('Finalizing Output')
    print('-----------------------------')
    final_output = cassandra_output.join(company,'job_id','left').drop(company.group_id).drop(company.campaign_id)
    print('-----------------------------')
    print('Import Output to Kafka')
    print('-----------------------------')
    import_to_kafka(final_output)
    return print('Task Finished')

if __name__ == "__main__":
    
    spark = SparkSession.builder \
        .config("spark.jars.packages","com.datastax.spark:spark-cassandra-connector_2.12:3.1.0") \
            .getOrCreate()
    USER = 'root'
    PASSWORD = '1'
    HOST = 'localhost'
    PORT = '3306'
    DB_NAME = 'data_engineering'
    URL = 'jdbc:mysql://' + HOST + ':' + PORT + '/' + DB_NAME
    DRIVER = "com.mysql.cj.jdbc.Driver"
    
    mysql_time = get_mysql_latest_time()
    main_task(mysql_time)
    spark.stop()
    
