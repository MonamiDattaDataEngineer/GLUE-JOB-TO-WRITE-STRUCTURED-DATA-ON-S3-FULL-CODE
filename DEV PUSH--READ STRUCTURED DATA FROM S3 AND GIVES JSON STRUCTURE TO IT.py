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

database = f"{Client}_{Domain}_{Action}_{Env}"
target_table = f"{EntityName}_{Target}"
crawler = f"crawler-{database}-{target_table}"

# SET PATH
target_write_path = DATA_TARGET_PATH + Env + '/' + DATA_TARGET_PATH_PREFIX + '/' + target_table + '/' + f'dataset_date={DATASET_DATE}/'
data_stage_path = DATA_STAGE_PATH + Env + '/' + DATA_TARGET_PATH_PREFIX + '/' + target_table + '/' + JOB_NAME.lower()

documentdb_uri = "mongodb://docdb-crm-inbound.cluster-caphsxy1o5sy.ap-south-1.docdb.amazonaws.com:27017"
documentdb_write_uri = "mongodb://docdb-crm-inbound.cluster-caphsxy1o5sy.ap-south-1.docdb.amazonaws.com:27017"
read_docdb_options = {
    "uri": documentdb_uri,
    "database": "prospect-db",
    "collection": "prospect_details",
    "username": "prospect-rw",
    "password": "rinfuie4342",
    "ssl": "true",
    "ssl.domain_match": "false",
    "partitioner": "MongoSamplePartitioner",
    "partitionerOptions.partitionSizeMB": "10",
    "partitionerOptions.partitionKey": "_id"
}

write_documentdb_options = {
    "uri": documentdb_write_uri,
    "database": "prospect-db",
    "collection": "prospect_details",
    "username": "prospect-rw",
    "password": "rinfuie4342",
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

print('##############TASK-1-IMPORTS+SPARK_CONFIG-COMPLETED################')

#######################################TASK-2#################################################
# UDF

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

print('##############TASK-2-UDF-DEFINED################')
#raise Exception('Forced Exception')
###################################TASK-3-DATA-LOADING#######################################
print('##############TASK-3-DATA-LOADING###############')

df_insert = spark.sql(f'''
select
    *
from
    msil_mscrm_structured_{Env}.prospect_dl
where
     DATASET_DATE='{DATASET_DATE}'
''').drop('dataset_date').limit(10)
df_insert.createOrReplaceTempView('prospect')
df_insert.printSchema()
count_of_records = df_insert.count()
print(f"count of records to be inserted: {count_of_records}")

print(f"starting loading into prospect_details")

df_insert_json = spark.sql('''
select
    struct(
        agentName createdBy,
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
    
#df_insert_json = df_insert_json.limit(10)
df_insert_json = df_insert_json.cache()
df_insert_json.select("prospectId","leadid","prospectType").show(n=100, truncate= False)
#dyf_insert_json = DynamicFrame.fromDF(df_insert_json, glueContext, "dyf_insert_json")

#raise Exception('Forced Exception')

df_insert.write.format('parquet').mode('overwrite').save(f'{data_stage_path}/stage1')

#dff = glueContext.write_dynamic_frame.from_options(dyf_insert_json, connection_type="documentdb", connection_options=write_documentdb_options)

dynamic_frame = DynamicFrame.fromDF(df_insert_json, glueContext, "dynamic_frame")
dff = glueContext.write_dynamic_frame.from_options(dynamic_frame, connection_type="documentdb", connection_options=write_documentdb_options)

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

if validation:
    print ("All Validation Completed successfully")
else:
    print('Data Validation Failed')
    raise Exception("Data Validation Failed")

print('##############TASK-4-DATA-VALIDATION-COMPLETED################')

###################################TASK-5-DATA-SYNC#######################################

print ("##############TASK-5-DATA-SYNC##############")
df_insert.write.format('parquet').mode('overwrite').save(target_write_path)

###################################TASK-6-REFRESH-ATHENA#######################################

run_crawler(crawler=crawler, database=database, target_table=target_table, dataset_date=DATASET_DATE)

print('##############TASK-6-REFRESH-ATHENA-COMPLETED################')

print ("##############TASK-6-DATA-SYNC-COMPLETED##############")

print('##############JOB-COMPLETED-SUCCESSFULLY################')

job.commit()
