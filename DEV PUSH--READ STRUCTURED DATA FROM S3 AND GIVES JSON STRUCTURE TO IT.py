##########job_msil_mscrm_prospect_dl_to_documentdb_outbound_sit###########
#######################################TASK-0#################################################
# IMPORTS

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import udf
from pyspark.sql import functions as F
from pyspark.sql import types as T
from awsglue.dynamicframe import DynamicFrame

import boto3

import json
import requests
from datetime import datetime as dt

import time
from botocore.exceptions import ClientError
glue_client = boto3.client('glue')

# SPARK CONFIG

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

s3_resource = boto3.resource('s3')

print('##############TASK-0-IMPORTS+SPARK_CONFIG-COMPLETED################')

#######################################TASK-1#################################################
# PARAMETERS

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'DATASET_DATE', 'DATA_STAGE_PATH', 'DATA_TARGET_PATH_PREFIX', 'DATA_TARGET_PATH'])
JOB_NAME = args['JOB_NAME']
DATA_TARGET_PATH = args['DATA_TARGET_PATH']
DATA_STAGE_PATH = args['DATA_STAGE_PATH']
DATA_TARGET_PATH_PREFIX = args['DATA_TARGET_PATH_PREFIX']
DATASET_DATE = args['DATASET_DATE']

Client = JOB_NAME.split('_')[1]
Domain = JOB_NAME.split('_')[2]
EntityName = JOB_NAME.split('_')[3]
Source = JOB_NAME.split('_')[4]
Target = JOB_NAME.split('_')[6]
Action = JOB_NAME.split('_')[7]
Env = JOB_NAME.split('_')[8]
job_run_id = dt.today().strftime("%Y%m%d%H%M%S%f")

database = f"{Client}_{Domain}_{Action}_{Env}"
target_table = f"{EntityName}_{Target}"
crawler = f"crawler-{database}-{target_table}"

partition_col = 'job_run_id'
partition_col_value = job_run_id

# SET PATH
target_write_path = DATA_TARGET_PATH + Env + '/' + DATA_TARGET_PATH_PREFIX + '/' + target_table + '/' + f'{partition_col}={partition_col_value}/'
data_stage_path = DATA_STAGE_PATH + Env + '/' + DATA_TARGET_PATH_PREFIX + '/' + target_table + '/' + JOB_NAME.lower()

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
    "partitionerOptions.partitionSizeMB": "1000000",
    "partitionerOptions.partitionKey": "_id",
    "sampleSize": "100000"
}

write_documentdb_options = {
    "uri": documentdb_uri,
    "database": "prospect-db",
    "collection": "prospect_details",
    "username": "docdbinboundqauser",
    "password": "CRMsit098",
    "ssl": "true",
    "ssl.domain_match": "false",
    "partitioner": "MongoSamplePartitioner",
    "partitionerOptions.partitionSizeMB": "10",
    "partitionerOptions.partitionKey": "_id",
    "retryWrites": "false"
}


print (f"Client : {Client}")
print (f"Domain : {Domain}")
print (f"EntityName : {EntityName}")
print (f"Source : {Source}")
print (f"Target : {Target}")
print (f"Action : {Action}")
print (f"Env : {Env}")

print (f"target_write_path : {target_write_path}")
print (f"data_stage_path : {data_stage_path}")
print (f"database : {database}")
print (f"target_table : {target_table}")
print (f"crawler : {crawler}")

print (f"DATASET_DATE : {DATASET_DATE}")
print (f"job_run_id : {job_run_id}")

print('##############TASK-1-IMPORTS+SPARK_CONFIG-COMPLETED################')

#######################################TASK-2#################################################
# UDF
"""
def run_crawler(crawler, database, target_table, dataset_date):
    def get_crawler_state(glue_client, crawler):
        response_get = glue_client.get_crawler(Name=crawler)
        state = response_get["Crawler"]["State"]
        return state
    def set_crawler_to_ready_state(glue_client, crawler):
        state = get_crawler_state(glue_client, crawler)
        state_previous = state
        print (f"Crawler {crawler} is {state.lower()}.")
        while (state != "READY") :
            time.sleep(30)
            state = get_crawler_state(glue_client, crawler)
            if state != state_previous:
                print (f"Crawler {crawler} is {state.lower()}.")
                state_previous = state
    try:
        tables_in_inbound_db = [tbl['tableName'] for tbl in spark.sql(f'''show tables in {database}''').select('tableName').collect()]
        if target_table in tables_in_inbound_db:
            
            if f'dataset_date={dataset_date}' in [partition['partition']  for partition in spark.sql(f'''show partitions {database}.{target_table}''').select('partition').collect()]:
                print (f"table -> {database}.{target_table} already present, partion {dataset_date} already exists")
            else:
                print (f"table -> {database}.{target_table} already present, adding partition {dataset_date}")
                spark.sql(f"alter table {database}.{target_table} add partition (dataset_date = '{dataset_date}')")
                print (f"alter table {database}.{target_table} add partition (dataset_date = '{dataset_date}')")
                print ('added partition successfully')
        else:
            print (f"table -> {database}.{target_table} not present")
            print ('crawler setting ready state')
            set_crawler_to_ready_state(glue_client, crawler)
            print ('started crawler')
            glue_client.start_crawler(Name=crawler)
            print ('crawler setting ready state')
            set_crawler_to_ready_state(glue_client, crawler)
    except Exception as e:
        raise Exception(e)
"""

def run_crawler(crawler, database, target_table, partition_col, partition_col_value):
    def get_crawler_state(glue_client, crawler):
        response_get = glue_client.get_crawler(Name=crawler)
        state = response_get["Crawler"]["State"]
        return state
    def set_crawler_to_ready_state(glue_client, crawler):
        state = get_crawler_state(glue_client, crawler)
        state_previous = state
        print (f"Crawler {crawler} is {state.lower()}.")
        while (state != "READY") :
            time.sleep(30)
            state = get_crawler_state(glue_client, crawler)
            if state != state_previous:
                print (f"Crawler {crawler} is {state.lower()}.")
                state_previous = state
    try:
        tables_in_inbound_db = [tbl['tableName'] for tbl in spark.sql(f'''show tables in {database}''').select('tableName').collect()]
        if target_table in tables_in_inbound_db:
            
            if f'partition_col={partition_col_value}' in [partition['partition']  for partition in spark.sql(f'''show partitions {database}.{target_table}''').select('partition').collect()]:
                print (f"table -> {database}.{target_table} already present, partion {partition_col_value} already exists")
            else:
                print (f"table -> {database}.{target_table} already present, adding partition {partition_col_value}")
                spark.sql(f"alter table {database}.{target_table} add partition ({partition_col} = '{partition_col_value}')")
                print (f"alter table {database}.{target_table} add partition ({partition_col} = '{partition_col_value}')")
                print ('added partition successfully')
        else:
            print (f"table -> {database}.{target_table} not present")
            print ('crawler setting ready state')
            set_crawler_to_ready_state(glue_client, crawler)
            print ('started crawler')
            glue_client.start_crawler(Name=crawler)
            print ('crawler setting ready state')
            set_crawler_to_ready_state(glue_client, crawler)
    except Exception as e:
        raise Exception(e)


print('##############TASK-2-UDF-DEFINED################')
#raise Exception('Forced Exception')
###################################TASK-3-DATA-LOADING#######################################
print('##############TASK-3-DATA-LOADING###############')

dyf_docdb = glueContext.create_dynamic_frame.from_options(connection_type="documentdb", connection_options=read_docdb_options)
df_docdb = dyf_docdb.toDF()
df_docdb.createOrReplaceTempView('docdb')

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

df_insert = df_insert
df_insert.write.format('parquet').mode('overwrite').save(f'{data_stage_path}/stage0')
df_insert = spark.read.format('parquet').load(f'{data_stage_path}/stage0')
df_insert = df_insert.cache()

df_insert.createOrReplaceTempView('prospect')
df_insert.printSchema()
count_of_records = df_insert.count()

print(f"count of records to be inserted: {count_of_records}")

print(f"starting loading into prospect_details")

df_insert_json = spark.sql(f'''
select
    struct(
        createdByDefault createdBy,
        AdminCreatedon createdOn,
        sourceCampaign sourceCampaign
        ) administration,
        agentId agentId,
        agentName agentName,
        buySell buySell,
        companyname_src companyName,
        SecondCreatedon createdOn,
        createdonDB createdOnDB,
        struct(
            alternateContactNumber alternateContactNumber ,
            pcl_carregistrationnumber carRegistrationNumber,
            countryCode countryCode,
            customerType customerType,
            cdl_emailid emailId,
            cdl_firstName firstName,
            cdl_lastName lastName,
            middle_name middleName,
            cdl_mobileNumber mobileNumber,
            modeOfCommunication,
            msilContact msilContact,
            queryDescription queryDescription
        ) customerDetails,
        struct(
            address1_dealer address1,
            address2_dealer address2,
            address3_dealer address3,
            city_dealer city,
            citycode_dealer cityCode,
            code code,
            contactnumber_dealer contactNumber,
            dealerType dealerType,
            email_dealer email,
            forcode_dealer forCode,
            loccd locCd,
            mulCode_dealer mulCode,
            name name,
            outletcd outletCode,
            region_dealer region,
            regioncode_dealer regionCode,
            state_dealer state,
            statecode_dealer stateCode,
            type1 type,
            uniquecode_dealer uniqueCode,
            enquiryType,
            zone zone
            ) dealerDetails,
    dmsEnquiryCreatedOn dmsEnquiryCreatedOn,
    dmsEnquiryExist dmsEnquiryExist,
    dmsEnquiryId dmsEnquiryId,
    enquiryType,
    struct(
            /*array(
                struct(
                    questionId
                    ,array(response) as response
                    )
                )  as interests*/
    new_response interests
    ) enrollmentDetails,
    leadSource leadSource,
    lob lob,
    prospectCode prospectCode,
    prospectId prospectId,
    prospectType prospectType,
    struct(
        accessoriesInterested accessoriesInterested,
        accessoriesPartNumber accessoriesPartNumber,
        averageRunningDay averageRunningDay,
        avgLoadCarrier avgLoadCarrier,
        sparePartInterested, 
        sparePartNumber,
        buyerType buyerType,
        carColor,
        customerProfile customerProfile,
        /*array(
                struct(
                    questionId
                    ,array(response) as response
                    )
                )  as interests,*/
        new_response interests,
        licenseAvailable licenseAvailable,
        primaryUsage primaryUsage,
        usageArea usageArea
    ) purchaseDetails,
    struct(
        corrected_channel channel,
        colorCode colorCode,
        manufacturingYear manufacturingYear,
        model_api model,
        modelcd modelCode,
        pcl_carregistrationNumber registrationNumber,
        type type,
        variant_api variant,
        variantcd variantCode
    ) vehicleDetails,
    voc voc,
    leadid
from
    prospect
''').withColumn('enrollmentDetails', F.when(F.col('prospectType')!='Enquiry MSDS', None).otherwise(F.col('enrollmentDetails')))\
    .withColumn('purchaseDetails', F.when(F.col('prospectType')=='Enquiry MSDS', None).otherwise(F.col('purchaseDetails')))

df_insert_json = df_insert_json.cache()
df_insert_json.select("prospectId","leadid","prospectType").show(n=100, truncate= False)
dyf_insert_json = DynamicFrame.fromDF(df_insert_json, glueContext, "dyf_insert_json")
dyf_inserted_json = glueContext.write_dynamic_frame.from_options(dyf_insert_json, connection_type="documentdb", connection_options=write_documentdb_options)

df_inserted_json = dyf_inserted_json.toDF()
df_inserted_json.withColumn('dataset_date', F.lit(str(DATASET_DATE))).write.format('parquet').mode('overwrite').save(f'{data_stage_path}/stage1')

#dff = glueContext.write_dynamic_frame.from_options(dyf_insert_json, connection_type="documentdb", connection_options=write_documentdb_options)
#dynamic_frame = DynamicFrame.fromDF(df_insert_json, glueContext, "dynamic_frame")
#dff = glueContext.write_dynamic_frame.from_options(dynamic_frame, connection_type="documentdb", connection_options=write_documentdb_options)

'''
records_migrated = ending_sequence_id-starting_sequence_id
print (f"{records_migrated} records inserted")
if records_migrated == count_of_records:
    print(f"All records loaded completely into prospect_details")
else:
    raise Exception(f"{count_of_records-records_migrated} records not migrated \n JOB FAILED")
'''
print('##############TASK-3-DATA-LOADING-COMPLETED################')

###################################TASK-3-DATA-VALIDATION#######################################
print ("##############TASK-4-DATA-VALIDATION##############")

validation = True
df_tgt = df_inserted_json
df_src = df_insert_json

df_src.createOrReplaceTempView('src')
df_tgt.createOrReplaceTempView('tgt')

#####count validation
src_count = df_src.count()
tgt_count = df_tgt.count()

print ("src_count :", src_count)
print ("tgt_count :", tgt_count)

if (src_count == tgt_count):
    print (f"count validation successfull")
else:
    validation = False
    print (f"count validation failed")


#####column names validation
src_columns = df_src.columns
tgt_columns = df_tgt.columns

print ("src_columns :", src_columns)
print ("tgt_columns :", tgt_columns)

if (src_columns == tgt_columns):
    print (f"column validation successfull")
else:
    validation = False
    print (f"column validation failed")


#####schema validation
src_schema = df_src.schema
tgt_schema = df_tgt.schema

print ("src_schema :", src_schema)
print ("tgt_schema :", tgt_schema)

#if (src_schema == tgt_schema):
#    print (f"Schema validation successfull")
#else:
#    validation = False
#    print (f"Schema validation failed")

#####Full Data validation

source_minus_target_count = spark.sql('''
select 
    count(*)
from (
    (select * from src)
        minus
    (select * from tgt)
)
''').collect()[0][0]

target_minus_source_count = spark.sql('''
select 
    count(*)
from (
    (select * from tgt)
        minus
    (select * from src)
)
''').collect()[0][0]

print ("datalake_minus_documentdb_count :", source_minus_target_count)
print ("documentdb_minus_datalake_count :", target_minus_source_count)

if(target_minus_source_count==0) and (source_minus_target_count==0):
    print (f"Full Data validation successfull")
else:
    validation = False
    print (f"Full Data validation failed")



if validation:
    print ("All Validation Completed successfully")
else:
    print('Data Validation Failed')
    raise Exception("Data Validation Failed")

print('##############TASK-4-DATA-VALIDATION-COMPLETED################')

###################################TASK-5-DATA-SYNC#######################################

print ("##############TASK-5-DATA-SYNC##############")
df_insert_json.write.format('parquet').mode('overwrite').save(target_write_path)

###################################TASK-6-REFRESH-ATHENA#######################################

run_crawler(crawler=crawler, database=database, target_table=target_table, partition_col=partition_col, partition_col_value=partition_col_value)

print('##############TASK-6-REFRESH-ATHENA-COMPLETED################')

print ("##############TASK-6-DATA-SYNC-COMPLETED##############")

print('##############JOB-COMPLETED-SUCCESSFULLY################')

job.commit()
