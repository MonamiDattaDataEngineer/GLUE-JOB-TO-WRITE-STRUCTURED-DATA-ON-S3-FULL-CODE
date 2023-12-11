%connections 'docdb-inbound-crm-sit-cluster'

%idle_timeout 360
%glue_version 3.0
%worker_type G.1X
%number_of_workers 2

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F


sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

###sit connection check####

url = 'mongodb://docdb-inbound-crm-sit-cluster.cluster-caphsxy1o5sy.ap-south-1.docdb.amazonaws.com:27017/prospect-db'
#"mongodb://docdb-crm-inbound.cluster-caphsxy1o5sy.ap-south-1.docdb.amazonaws.com:27017"
documentdb_uri = "mongodb://docdb-inbound-crm-sit-cluster.cluster-caphsxy1o5sy.ap-south-1.docdb.amazonaws.com:27017"
documentdb_write_uri = "mongodb://docdb-inbound-crm-sit-cluster.cluster-caphsxy1o5sy.ap-south-1.docdb.amazonaws.com:27017"
read_docdb_options = {
    "uri": documentdb_uri,
    "database": "prospect-db",
    "collection": "prospect_details",
    "username": "docdbinboundqauser",
    "password": "CRMsit098",
    "ssl": "true",
    "ssl.domain_match": "false",
    "partitioner": "MongoSamplePartitioner",
    "partitionerOptions.partitionSizeMB": "10",
    "partitionerOptions.partitionKey": "_id"
}
 
 
dyf = glueContext.create_dynamic_frame.from_options(connection_type="documentdb", connection_options=read_docdb_options)
 
df = dyf.toDF()
 
df.printSchema()
df.show()

#####to check if any data has been inserted in the table##########

dyf_docdb = glueContext.create_dynamic_frame.from_options(connection_type="documentdb", connection_options=read_docdb_options)
df_docdb = dyf_docdb.toDF()
df_docdb.createOrReplaceTempView('docdb')

DATASET_DATE = '20231130'

if 'leadid' in df_docdb.columns:
    df_insert = spark.sql(f'''
    select
        *
    from
        msil_mscrm_structured_dev.prospect_dl
    where
         DATASET_DATE='{DATASET_DATE}'
         and leadid not in (select coalesce(leadid, '') from docdb)
    ''')
else:
    df_insert = spark.sql(f'''
    select
        *
    from
        msil_mscrm_structured_dev.prospect_dl
    where
         DATASET_DATE='{DATASET_DATE}'
    ''')
	
df_insert.count()

if 0-----data has been inserted
if 140391-----	this much data is not inserted
	