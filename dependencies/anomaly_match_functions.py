import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.streaming import *
from pyspark.sql.types import *
from pyspark.sql.functions import udf
from pyspark import StorageLevel

anomaly_type_to_match_function_name_dict = {
"списочная": "listlike"

}

def match_df_to_anomaly_listlike(df_last_time, df_one_month, **kwargs):
  anomaly_fields = kwargs.get("fields")
  target_field = kwargs.get("target_field")
  name = kwargs.get("name")
  # if not (anomaly_fields and target_field):
  # spark_session.stop()
  # print("ERR: missing arg(s) target_field / fields")
  # return -1
  # LOOK UP maith
  #match to selected fields anomaly
  if not df_one_month.isEmpty():
    df_deviate = df_last_time.select(*anomaly_fields).subtract(df_one_month.select(*anomaly_fields)).orderBy(target_field)
    df_deviate = df_deviate.hint('broadcast').alias("source").join(df_last_time.alias("target"), target_field, "inner")\
                  .select(*["source." + field for field in anomaly_fields], *["target." + field for field in df_last_time.columns if field not in anomaly_fields])\
                  .dropDuplicates(anomaly_fields).orderBy(df_deviate[target_field]).withColumn("anomaly", f.lit(str(name)))
  else:
    print('that\'s bad')
    return df_one_month
  return df_deviate

def match_df_to_anomaly_aggregate(df_last_time, df_one_month, **kwargs):
  anomaly_fields = kwargs.get("fields")
  target_field = kwargs.get("target_field")
  name = kwargs.get("name")
  treshold = kwargs.get("treshold")
  
  if not (anomaly_fields and target_field):
    spark_session.stop()
    print("ERR: missing arg(s) target_field / fields")
    return -1

  return df_deviate

def match_function_call(*pos_args: str, **kw_args):
  val = int(-1)
  prefix = "match_df_to_anomaly_"
  full_func_name = kw_args.get("type").lower()
  if full_func_name in anomaly_type_to_match_function_name_dict:
    full_func_name = anomaly_type_to_match_function_name_dict.get(full_func_name)
  if prefix not in full_func_name[: len(prefix)]:
    full_func_name = prefix + full_func_name
  val = globals()[full_func_name](*pos_args, **kw_args)
  return val
