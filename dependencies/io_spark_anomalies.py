
import csv
import json
from glob import glob
import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

def get_anomalies_file(file_path:str, encoding:str='utf-8', delimiter=';') -> list(dict()):
"""
return value -> List[Dict()] : list of dictionaries
args: 
pos args : file_path:str, kv args : encoding:str, delimiter:str

get_anomalies_file(file_path, encoding='utf-8', delimiter=';')

"""
translate_dict = dict({
'duser': 'target_user_name',
'dhost': 'device_host',
'logon_type': 'logon_type',
'suser': 'attacker_user_name',
'daddr': 'target_address',
'saddr': 'attacker_address'
})
idx_dict = dict()
with open(file_path, 'r', encoding=encoding) as file_r:
reader = csv.DictReader(file_r, delimiter=delimiter)
ctr = int(0)
for row in reader:
record = dict()
for key in row:
if key in ('fields', 'req_fields'):
fields = (row[key].strip()).split(',')
fields = [ translate_dict.get(field.strip()) if translate_dict.get(field.strip()) else field.strip() for field in fields ]
record.update({key.replace('\ufeff', '') : fields})
elif key == 'target_field':
field = row[key]
if translate_dict.get(field.strip()):
field = translate_dict.get(field.strip())
record.update({key.replace('\ufeff', '') : field.strip()})
else:
if key:
val = row[key]
record.update({(key.strip()).replace('\ufeff', '') : str(row[key]).strip()})
idx_dict.update({ctr: record})
ctr += 1
return idx_dict

def get_json_conf_file(file_path:str, encoding='utf-8') -> dict():
request_conf = dict()
with open(file_path, 'r', encoding=encoding) as file_r:
request_conf = json.load(file_r)
return request_conf

def fill_catalogue_namespace(props:dict, spark_session, suffix:str='_read', ssl_on:bool=True):
dbs = dict()
for idx in props:
for sub_idx in props.get(idx):
dbs.update({((props.get(idx)).get(sub_idx)).get("database") : ((props.get(idx)).get(sub_idx))})
for db in dbs:
try:
catalogue_id = '{}{}'.format(db, suffix)
except Exception as err:
print(err)
catalogue_id = 'missconf_' + str(uuid.uuid4())
spark_session.conf.set("spark.sql.catalog.{}".format(catalogue_id), "com.clickhouse.spark.ClickHouseCatalog")
spark_session.conf.set("spark.sql.catalog.{}.protocol".format(catalogue_id), "https")
spark_session.conf.set("spark.sql.catalog.{}.host".format(catalogue_id), dbs.get(db)['host'])
spark_session.conf.set("spark.sql.catalog.{}.http_port".format(catalogue_id), dbs.get(db)['port'])
spark_session.conf.set("spark.sql.catalog.{}.user".format(catalogue_id), dbs.get(db)['user'])
spark_session.conf.set("spark.sql.catalog.{}.password".format(catalogue_id), dbs.get(db)['password'])
spark_session.conf.set("spark.sql.catalog.{}.database".format(catalogue_id), dbs.get(db)['database'])
if ssl_on:
spark_session.conf.set("spark.sql.catalog.{}.option.ssl".format(catalogue_id), 'true')
spark_session.conf.set("spark.sql.catalog.{}.option.sslrootcert".format(catalogue_id), "/home/jovyan/experiments/Rvision_anomalies/logon_profile/ca.crt")
spark_session.conf.set("spark.sql.catalog.{}.option.sslmode".format(catalogue_id), 'none')
spark_session.conf.set("spark.{}.useNullableQuerySchema".format(catalogue_id), 'true')
spark_session.conf.set("spark.{}.ignoreUnsupportedTransform".format(catalogue_id), 'true')
spark_session.conf.set("spark.{}.write.format".format(catalogue_id), "json")
return spark_session

def construct_request(table, **kwargs):
req_fields = kwargs.get("req_fields")
db_props = kwargs.get("db_props")
catalogue_id = kwargs.get("catalogue_id")
timestamp_interval = kwargs.get("timestamp_interval")
request_prepared = "SELECT {} FROM {}.{}.{} WHERE timestamp {} {}GROUP BY {} ORDER BY first_occurrence DESC".format(
(', '.join(list(req_fields) + ['min(timestamp) as first_occurrence, max(timestamp) as last_occurrence'])), catalogue_id, db_props.get("database"), table, timestamp_interval,\
(lambda conds: ' '.join(['AND {}=\'{}\''.format(key, conds.get(key)) if isinstance(conds.get(key), str) else 'AND {} IN (\'{}\')'.format(key, '\', \''.join(conds.get(key))) for key in conds]) + ' ' if conds else '')((db_props.get("tables")).get(table)), (', '.join(req_fields)))
print(request_prepared)
return request_prepared

def make_db_request(spark_session, table, **kwargs):
db_props_realtime = kwargs.get("db_properties_realtime")
catalogue_id = kwargs.get("catalogue_id")
fields = kwargs.get("fields")
req_fields = kwargs.get("req_fields")
db_props_month = kwargs.get("db_properties_month")
one_month_only = kwargs.get("one_month_only")
if kwargs.get("db_properties_month"):
db_props_month = kwargs.get("db_properties_month")
table_month = (list((kwargs.get("db_properties_month")).get("tables").keys())[0])
else:
db_props_month = db_props_realtime
table_month = table
if kwargs.get("sup_catalogue_id"):
catalogue_id_month = kwargs.get("sup_catalogue_id")
else:
catalogue_id_month = catalogue_id
timestamp_intervals = dict({
"month": "BETWEEN now() - INTERVAL 1 MONTH AND date_trunc('DAY', now())",
"five_minutes": "BETWEEN now() - INTERVAL 5 MINUTE AND now() AND end_time > now() - INTERVAL 1 DAY" 
})
if not one_month_only:
req_fields_month = fields
else:
req_fields_month = req_fields
request_one_month = construct_request(table_month, timestamp_interval=timestamp_intervals.get("month"), db_props=db_props_month, catalogue_id=catalogue_id_month, req_fields=req_fields_month)
df_one_month = spark_session.sql(request_one_month)
files = glob("./storage/{}/*.json".format(catalogue_id.lower() + '_' + table))
if not one_month_only:
if not files:
request_five_min = construct_request(table, db_props = db_props_realtime, timestamp_interval = timestamp_intervals.get("five_minutes"), **kwargs)
df_last = spark_session.sql(request_five_min)
else:
df_last = spark_session.read.json(files[0]).select(f.max(f.col("last_occurrence")).alias("last_occurrence"))
last_idx_extracted = df_last.collect()[0][0]
timestamp_intervals.update({"last_time" : "BETWEEN '{}' AND now() AND end_time > now() - INTERVAL 1 DAY".format(last_idx_extracted)})
request_last_updated = construct_request(table, db_props = db_props_realtime, timestamp_interval = timestamp_intervals.get("last_time"), **kwargs)
df_last = spark_session.sql(request_last_updated)

#MUST HAVE TIMESTAMP
df_last.select("last_occurrence").orderBy(f.col("last_occurrence").desc()).limit(1).coalesce(1).write.json("./storage/{}".format(catalogue_id.lower() + '_' + table), mode="overwrite")
df_last = df_last.drop("last_occurrence")
df_one_month = df_one_month.drop("last_occurrence")
return df_last, df_one_month
else:
df_one_month = df_one_month.drop("last_occurrence")
return df_one_month

def read_from_db(props:dict, spark_session=None):
print("call read_from_db")
tables = list((props.get("db_properties_realtime")).get("tables").keys())
if not props.get("one_month_only"):
df_last, df_one_month = make_db_request(spark_session, tables[0], **props)
for table in tables[1:]:
df_last_sup, df_one_month_sup = make_db_request(spark_session, table, **props)
df_last = df_last.union(df_last_sup)
df_one_month = df_one_month.union(df_one_month_sup)
return df_last, df_one_month
else:
df_one_month = make_db_request(spark_session, tables[0], **props)
for table in tables[1:]:
df_one_month_sup = make_db_request(spark_session, table, **props)
df_one_month = df_one_month.union(df_one_month_sup)
return df_one_month
