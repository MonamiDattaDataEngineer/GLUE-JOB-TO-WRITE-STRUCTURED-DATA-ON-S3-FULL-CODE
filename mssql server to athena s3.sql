import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

import boto3
import json

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
client = boto3.client('secretsmanager')

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

url_mssql = 'jdbc:sqlserver://inbound-crm-dev-mssql.caphsxy1o5sy.ap-south-1.rds.amazonaws.com:1433;databaseName=MSIL_MSCRM'
res_sv = client.get_secret_value(
    SecretId='arn:aws:secretsmanager:ap-south-1:692425240801:secret:rds!db-7d0aac3b-64eb-403e-b3b6-95f27a3fb780-bDe24q'
)
user = json.loads(res_sv['SecretString'])["username"]
password = json.loads(res_sv['SecretString'])["password"]


# df_pcl_citybase = spark.read.format('jdbc').options(url=url_mssql, user=user, password=password, dbtable="pcl_citybase").load()
# df_pcl_citybase.printSchema()
# df_pcl_citybase.write.format('parquet').mode('overwrite').save('s3://msil-inbound-crm-raw-non-prod/source/parquet/pcl_citybasemigration/snapshot_dt=20230920/')

# df_pcl_statebase = spark.read.format('jdbc').options(url=url_mssql, user=user, password=password, dbtable="pcl_statebase").load()
# df_pcl_statebase.printSchema()
# df_pcl_statebase.write.format('parquet').mode('overwrite').save('s3://msil-inbound-crm-raw-non-prod/source/parquet/pcl_statebasemigration/snapshot_dt=20230920/')

# df_pcl_systemuserbase = spark.read.format('jdbc').options(url=url_mssql, user=user, password=password, dbtable="systemuserbase").load()
# df_pcl_systemuserbase.printSchema()
# df_pcl_systemuserbase.write.format('parquet').mode('overwrite').save('s3://msil-inbound-crm-raw-non-prod/source/parquet/tmp/systemuserbaseupdated/')

# df_pcl_variantbase = spark.read.format('jdbc').options(url=url_mssql, user=user, password=password, dbtable="pcl_variantbase").load()
# df_pcl_variantbase.printSchema()
# df_pcl_variantbase.write.format('parquet').mode('overwrite').save('s3://msil-inbound-crm-raw-non-prod/source/parquet/tmp/pcl_variantbaseupdated/')

# df_accountbase = spark.read.format('jdbc').options(url=url_mssql, user=user, password=password, dbtable="AccountBase").load()
# df_accountbase.printSchema()
# df_accountbase.write.format('parquet').mode('overwrite').save('s3://msil-inbound-crm-raw-non-prod/source/parquet/tmp/accountbasemigrationupdated/')

# df_pcl_incidentbase = spark.read.format('jdbc').options(url=url_mssql, user=user, password=password, dbtable="incidentbase").load()
# df_pcl_incidentbase.printSchema()
# df_pcl_incidentbase.write.format('parquet').mode('overwrite').save('s3://msil-inbound-crm-raw-non-prod/source/parquet/tmp/2023-09-23/')

#df_contactbase = spark.read.format('jdbc').options(url=url_mssql, user=user, password=password, dbtable="ContactBase").load()
#df_contactbase.printSchema()
#df_contactbase.write.format('parquet').mode('overwrite').save('s3://msil-inbound-crm-raw-non-prod/source/parquet/contactbasemigration/snapshot_dt#=20230920/')

#df_pcl_variantBase = spark.read.format('jdbc').options(url=url_mssql, user=user, password=password, dbtable="pcl_variantBase").load()
#df_pcl_variantBase.printSchema()
#df_pcl_variantBase.write.format('parquet').mode('overwrite').save('s3://msil-inbound-crm-raw-non-prod/source/parquet/pcl_variantBase')

# df_pcl_modelmasterbase = spark.read.format('jdbc').options(url=url_mssql, user=user, password=password, dbtable="pcl_modelmasterbase").load()
# df_pcl_modelmasterbase.printSchema()
# df_pcl_modelmasterbase.write.format('parquet').mode('overwrite').save('s3://msil-inbound-crm-raw-non-prod/source/prospects/parquet/pcl_modelmasterbasemigration')

# df_accountbase = spark.read.format('jdbc').options(url=url_mssql, user=user, password=password, dbtable="accountbase").load()
# df_accountbase.printSchema()
# df_accountbase.write.format('parquet').save('s3://msil-inbound-crm-raw-non-prod/source/parquet/accountbasemigration/snapshot_dt=2023-11-08/')

# df_pcl_variantBase = spark.read.format('jdbc').options(url=url_mssql, user=user, password=password, dbtable="pcl_variantbase").load()
# df_pcl_variantBase.printSchema()
# df_pcl_variantBase.write.format('parquet').save('s3://msil-inbound-crm-raw-non-prod/source/parquet/pcl_variantBase/snapshot_dt=2023-11-08/')

# df_leadbase = spark.read.format('jdbc').options(url=url_mssql, user=user, password=password, dbtable="leadbase").load()
# df_leadbase.printSchema()
# df_leadbase.write.format('parquet').save('s3://msil-inbound-crm-raw-non-prod/source/parquet/leadbase/snapshot_dt=2023-11-08/')

# df_pcl_systemuserbase = spark.read.format('jdbc').options(url=url_mssql, user=user, password=password, dbtable="systemuserbase").load()
# df_pcl_systemuserbase.printSchema()
# df_pcl_systemuserbase.write.format('parquet').save('s3://msil-inbound-crm-raw-non-prod/source/parquet/systemuserbasemigration/snapshot_dt=2023-11-08/')
# df_pcl_modelmasterbase = spark.read.format('jdbc').options(url=url_mssql, user=user, password=password, dbtable="pcl_modelmasterbase").load()
# df_pcl_modelmasterbase.printSchema()
# df_pcl_modelmasterbase.write.format('parquet').save('s3://msil-inbound-crm-raw-non-prod/source/prospects/parquet/pcl_modelmasterbasemigration/snapshot_dt=2023-11-08/')

# df_stringmapbase = spark.read.format('jdbc').options(url=url_mssql, user=user, password=password, dbtable="stringmapbase").load()
# df_stringmapbase.printSchema()
# df_stringmapbase.write.format('parquet').save('s3://msil-inbound-crm-raw-non-prod/source/prospects/parquet/StringMapBase/snapshot_dt=2023-11-08/')

df_accountbase = spark.read.format('jdbc').options(url=url_mssql, user=user, password=password, dbtable="AccountBase").load()
df_accountbase.printSchema()
df_accountbase.write.format('parquet').mode('overwrite').save('s3://msil-inbound-crm-raw-non-prod/source/parquet/accountbasemigration/snapshot_dt=20231122/')

job.commit()