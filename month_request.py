#import dum' stuff as schemas an' dataframe --> python data structures transitions
import os
import uuid
#from data_struct import data_struct_t as schema
os.environ['SPARK_HOME'] = '/opt/spark/'

#import some valuable stuff from PySpark library
import findspark
from urllib.parse import unquote
findspark.init('/opt/spark/')
import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.streaming import *
from pyspark.sql.types import *
from pyspark.sql.functions import udf
from pyspark import StorageLevel
from dependencies.io_spark_ueba_anomalies import get_anomalies_file
from dependencies.io_spark_ueba_anomalies import get_json_conf_file
from dependencies.io_spark_ueba_anomalies import fill_catalogue_namespace
from dependencies.io_spark_ueba_anomalies import read_from_db


@udf(returnType=StringType())
def get_uuid():
  return str(uuid.uuid4())

spark = (SparkSession.builder
          .appName("test_UEBA_anomalies_match_modules")
          .config("spark.dynamicAllocation.enabled", "true")
          .config("spark.executor.memory", "6g")
          .config("spark.driver.memory", "6g")
          .config("spark.executor.cores", "1")
          .config("spark.decommission.enabled", "false") 
          .config('spark.sql.legacy.timeParserPolicy', 'LEGACY')
          .config("spark.sql.session.timeZone", "Europe/Moscow")
          .master("local[*]")
          .getOrCreate())
anomalies_raw = get_anomalies_file('./configs/import_anomalies_table.csv')
db_properties_realtime = get_json_conf_file('./configs/request_db_conf_realtime.json')
db_properties_month = get_json_conf_file('./configs/request_db_conf_month.json')

# MAKE CATALOGUES FOR DBs IN SPARK SESSION NAMESPACE
suffix = '_read'
spark = fill_catalogue_namespace(db_properties_realtime, spark, suffix=suffix)
spark = fill_catalogue_namespace(db_properties_month, spark, suffix=suffix, ssl_on=False)

# MATCH INDEXES WITH ANOMALY CLASSES 
anomalies_class_pool = dict()
for idx in anomalies_raw:
  anomaly_class_id = (anomalies_raw.get(idx)).get("anomaly_class")
  if anomaly_class_id:
    if anomaly_class_id in anomalies_class_pool:
      (anomalies_class_pool.get(anomaly_class_id)).append(int(idx))
    else:
      anomalies_class_pool.update({anomaly_class_id : list([int(idx)])})

# READ INTO DATAFRAMES
for anomaly_class_id in anomalies_class_pool:
  #connection goes at this loop
  req_fields = [(anomalies_raw.get(idx)).get("req_fields") for idx in anomalies_class_pool.get(anomaly_class_id)]
  req_fields = set([field for liste in req_fields for field in liste])
  #gathering fields among the anomaly class
  fields = [(anomalies_raw.get(idx)).get("fields") for idx in anomalies_class_pool.get(anomaly_class_id)]
  fields = set([field for liste in fields for field in liste])
  #goin through several databases those are bound to that particular anomaly class
  for db_properties_prod in db_properties_realtime.get(anomaly_class_id):
    props = {"prod_name": db_properties_prod,
    "db_properties_realtime": (db_properties_realtime.get(anomaly_class_id)).get(db_properties_prod),
    "req_fields": req_fields,
    "catalogue_id": ((db_properties_realtime.get(anomaly_class_id)).get(db_properties_prod)).get("database") + suffix,
    "one_month_only": True
    }
    df_one_month = read_from_db(props, spark)
    if db_properties_month.get(anomaly_class_id):
      if db_properties_month.get(anomaly_class_id).get("Universal"):
        props.update({ "sup_catalogue_id": ((db_properties_month.get(anomaly_class_id)).get("Universal")).get("database") + suffix,
        "db_properties_month": (db_properties_month.get(anomaly_class_id).get("Universal"))
        })
        
        print(props.get("sup_catalogue_id") + "." + (props.get("db_properties_month").get("database"))\
        + "." + (list(((props.get("db_properties_month")).get("tables")).keys()))[0])
        df_one_month.withColumn("uuid", get_uuid()).withColumnRenamed("first_occurrence", "timestamp").writeTo(props.get("sup_catalogue_id")\
        + "." + (props.get("db_properties_month").get("database"))\
        + "." + (list(((props.get("db_properties_month")).get("tables")).keys()))[0]).append()
        df_one_month.withColumn("uuid", get_uuid()).withColumnRenamed("first_occurrence", "timestamp").show(3)
        #df_one_month.withColumnRenamed("first_occurrence", "timestamp").show(5)
df_one_month.drop()
