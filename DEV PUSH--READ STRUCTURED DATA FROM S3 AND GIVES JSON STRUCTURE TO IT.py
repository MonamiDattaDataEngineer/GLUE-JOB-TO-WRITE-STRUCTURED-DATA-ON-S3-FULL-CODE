#######job name---job_msil_mscrm_prospect_dl_to_dl_structured_dev##############

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
from pyspark.sql.functions import udf
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.functions import when, col, concat_ws,expr
from pyspark.sql.functions import date_format,to_timestamp,from_utc_timestamp
from datetime import datetime as dt


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
spark.conf.set( "spark.sql.crossJoin.enabled" , "true" )

s3_resource = boto3.resource('s3')

print('##############TASK-0-IMPORTS+SPARK_CONFIG-COMPLETED################')

#######################################TASK-1#################################################
# PARAMETERS

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'DATA_SOURCE_PATH', 'DATA_TARGET_PATH', 'DATA_STAGE_PATH',  'DATA_TARGET_PATH_PREFIX', 'DATASET_DATE'])

JOB_NAME = args['JOB_NAME']
DATA_SOURCE_PATH = args['DATA_SOURCE_PATH']
DATA_TARGET_PATH = args['DATA_TARGET_PATH']
DATA_STAGE_PATH = args['DATA_STAGE_PATH']
#SQL_SCRIPT_PATH_PREFIX = args['SQL_SCRIPT_PATH_PREFIX']
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
data_source_path = DATA_SOURCE_PATH
target_write_path = DATA_TARGET_PATH + Env + '/' + DATA_TARGET_PATH_PREFIX + '/' + target_table + '/' + f'dataset_date={DATASET_DATE}/'
data_stage_path = DATA_STAGE_PATH + Env + '/' + DATA_TARGET_PATH_PREFIX + '/' + target_table + '/' + JOB_NAME.lower()

#sql_script_bucket = SQL_SCRIPT_PATH_PREFIX.split('//')[1].split('/', 1)[0]
#sql_script_key = SQL_SCRIPT_PATH_PREFIX.split('//')[1].split('/', 1)[1] + f'dataset_date={DATASET_DATE}/{JOB_NAME}.sql'

print (f"Client : {Client}")
print (f"Domain : {Domain}")
print (f"EntityName : {EntityName}")
print (f"Source : {Source}")
print (f"Target : {Target}")
print (f"Action : {Action}")
print (f"Env : {Env}")

print (f"data_source_path : {data_source_path}")
print (f"target_write_path : {target_write_path}")
print (f"data_stage_path : {data_stage_path}")
#print (f"SQL_SCRIPT_PATH_PREFIX : {SQL_SCRIPT_PATH_PREFIX}")
print (f"database : {database}")
print (f"target_table : {target_table}")
print (f"crawler : {crawler}")

print (f"DATASET_DATE : {DATASET_DATE}")

#raise Exception('Forced Exception')

print('##############TASK-1-PARAMETERS+SET_PATH-COMPLETED################')

#######################################TASK-2#################################################
# UDF
def read_s3_content(bucket_name, key):
    response = s3_resource.Object(bucket_name, key).get() 
    return response['Body'].read()

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

###################################TASK-3-DATA-EXTRACTION#######################################

url_city_val_api = 'https://www.cf.marutisuzukisubscribe.com/api/common/cities-brief'
url_dealer_api = 'https://cf.msilcrm.co.in/crm-common/api/common/msil/dms/dealer-master'

res_city_val = requests.get(url_city_val_api)

if res_city_val.status_code == 200:
    res_city_val_json = res_city_val.json()
    city_val_json = res_city_val_json['data']
    
df_city_val = spark.createDataFrame(data=city_val_json)
stateCds = [rw.stateCd for rw in df_city_val.select('stateCd').distinct().filter("stateCd is not Null").collect()]

dealer_headers = {
    'x-api-key': 'rzEw0mlWmf3zZKkhA01VnxPrXss6rSO5YedEPpye', 
    'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36'
}
dealer_params = {
    'stateCd' : None
}

dealer_api_data = dict()
dealer_api_data['data'] = list()

for stateCd in stateCds:
    dealer_params['stateCd'] = stateCd
    res_dealer_master_val = requests.get(url=url_dealer_api, headers=dealer_headers, params=dealer_params)
    try:
        scd = res_dealer_master_val.json()['data'][0]['stateCd']
    except Exception as e:
        scd = 'default'
    finally:
        if scd.strip('\t') == stateCd.strip('\t'):
            dealer_api_data['data'] = dealer_api_data['data'] + res_dealer_master_val.json()['data']
        else:
            print (stateCd, scd, ' not matched')

df_dealer_api_data = spark.createDataFrame(data=dealer_api_data['data'])
#df_dealer_api_data.write.format('parquet').mode('overwrite').save('s3://msil-inbound-crm-raw-non-prod/lookup/dealer_master/')

print('##############TASK-3-DATA-EXTRACTION-COMPLETED################')

###################################TASK-4-DATA-TRANSFORMATION#######################################

###################################
url_dealer_api = 'https://cf.marutisuzukisubscribe.com/api/common/vehicle/model-and-variants'
headers = {'x-api-key': 'rzEw0mlWmf3zZKkhA01VnxPrXss6rSO5YedEPpye'}
try:
    response = requests.get(url_dealer_api, headers=headers)
    res_model_json = response.json()
    model_json = res_model_json['data']
    df_modelmasterapi = spark.createDataFrame(data=model_json)
    df_modelmasterapi.createOrReplaceTempView('modelmasterapi')
except Exception as e:
    print ("Unable to prepare model master api look up table")
    raise Exception(e)
###Reading Leadbase,Accountbase,Systemuserbase,Dealermaster tables,ModelmasterAPI,Variantbasemigration and Modelmasterbasemigration


df_leadbase = spark.sql('''
select
    *
from
    inbound.leadbase
    where createdon>=to_timestamp('2023-01-01')
    and snapshot_dt = '2023-11-08'
''').withColumnRenamed('pcl_dealeruniquecode', 'pcl_dealeruniquecode_leadbase')\
    .withColumnRenamed('statecode', 'statecode_leadbase')\
    .withColumnRenamed('pcl_name', 'pcl_name_leadbase')\
    .withColumn("createdOn_leadbase",
    F.expr("from_unixtime(unix_timestamp(createdon) + 5*3600 + 30*60)"))\
    .filter(F.col('pcl_leadsource') != '66')

df_leadbase.createOrReplaceTempView('leadbase')
print (f"df_leadbase_count : {df_leadbase.count()}")

df_accountbasemigration = spark.sql('''
select
    *
from
    inbound.accountbasemigration
    where  snapshot_dt = '2023-11-08'
''')
df_accountbasemigration.createOrReplaceTempView('accountbasemigration')
print (f"df_accountbasemigration_count : {df_accountbasemigration.count()}")

df_systemuserbasemigration = spark.sql('''
select
    firstname agentId,
    fullname agentName,
    systemuserid
from
    inbound.systemuserbasemigration
    where  snapshot_dt = '2023-11-08'
''')
df_systemuserbasemigration.createOrReplaceTempView('systemuserbasemigration')
print (f"df_systemuserbasemigration_count : {df_systemuserbasemigration.count()}")

df_pcl_modelmasterbasemigration = spark.sql('''
select
    *
from
    inbound.pcl_modelmasterbasemigration
    where  snapshot_dt = '2023-11-08'
''').drop('pcl_modelid','createdon')
df_pcl_modelmasterbasemigration.createOrReplaceTempView('pcl_modelmasterbasemigration')
print (f"df_pcl_modelmasterbasemigration_count : {df_pcl_modelmasterbasemigration.count()}")



df_pcl_variantbase = spark.sql('''
select
    *
from
    inbound.pcl_variantbase
    where snapshot_dt = '2023-11-08'
''').select("pcl_variantname","pcl_variantcode","pcl_variantid")\
    .withColumnRenamed("pcl_variantcode","pcl_variantcode_vb")\
    .withColumnRenamed("pcl_variantid","pcl_variantid_vb")\
    .withColumn("manufacturingYear",F.lit(None).cast("string"))\
    .withColumn("temporaryRegistration",F.lit(None).cast("string"))\
    .withColumnRenamed("pcl_modelid","pcl_modelid_vb")

df_pcl_variantbase.createOrReplaceTempView('pcl_variantbase')
print (f"df_pcl_variantbase_count : {df_pcl_variantbase.count()}")




df_model_master_updated_parquet = spark.sql('''
select
    *
from
    inbound.model_master_updated_parquet
''').withColumnRenamed("channel","api_channel")
df_model_master_updated_parquet.createOrReplaceTempView('model_master_updated_parquet')
print (f"df_model_master_updated_parquet_count : {df_model_master_updated_parquet.count()}")




df_contact_dl = spark.sql('''
select
    *
from
    msil_mscrm_structured_dev.contact_dl
''').select("mobile_no" , "reference_number","first_name","middle_name",
"last_name","email_id")
           

df_contact_dl.createOrReplaceTempView('contact_dl')
print (f"df_contact_dl_count : {df_contact_dl.count()}")




df_dealermaster = spark.sql('''
select
    *
from
    inbound.dealer_master_updated3_parquet
''').select("category","type","name","forcd","mulcd","phone","ceomail","dealeruniquecd","outletcd",
            "addr1","addr2","addr3","citydesc","sallivedate", "citycd","statedesc","statecd","region",
            "regioncd","zone","mapcd","loccd","channel")\
    .withColumnRenamed("category","type1")\
    .withColumnRenamed("type","dealertype")\
    .withColumnRenamed("forcd","forcode_dealer")\
    .withColumnRenamed("mulcd","mulcode_dealer")\
    .withColumnRenamed("phone","contactnumber_dealer")\
    .withColumnRenamed("ceomail","email_dealer")\
    .withColumnRenamed("dealeruniquecd","uniquecode_dealer")\
    .withColumnRenamed("addr1","address1_dealer")\
    .withColumnRenamed("addr2","address2_dealer")\
    .withColumnRenamed("addr3","address3_dealer")\
    .withColumnRenamed("citydesc","city_dealer")\
    .withColumnRenamed("citycd","citycode_dealer")\
    .withColumnRenamed("statedesc","state_dealer")\
    .withColumnRenamed("statecd","statecode_dealer")\
    .withColumnRenamed("region","region_dealer")\
    .withColumnRenamed("regioncd","regioncode_dealer")\
    .withColumn("code", F.concat_ws("-", F.col("mapcd"), F.col("loccd"))).distinct()
df_dealermaster.createOrReplaceTempView('dealermaster')  
print (f"df_dealermaster_count : {df_dealermaster.count()}")
###Reading from StringMapBase table and mapping with Leadbase

df_stage1 = spark.sql('''
with
    pcl_prospectsubcategory_attr as (
    select
        distinct
            attributevalue,
            value
    from
        inbound.stringmapbase
    where
        AttributeName='pcl_prospectsubcategory'
        and snapshot_dt = '20231122'
    ),
    pcl_prospectcategory_attr as (
    select
        distinct
            attributevalue,
            value
    from
        inbound.stringmapbase
    where
        AttributeName='pcl_prospectcategory'
        and snapshot_dt = '20231122'
    ),
    pcl_purchaseoption_attr as (
    select
        distinct
            attributevalue,
            value
    from
        inbound.stringmapbase
    where
        AttributeName='pcl_purchaseoption'
        and snapshot_dt = '20231122'
    ),
    pcl_liketoexchangeoldcar_attr as (
    select
        distinct
            attributevalue,
            value
    from
        inbound.stringmapbase
    where
        AttributeName='pcl_liketoexchangeoldcar'
        and snapshot_dt = '20231122'
    ),
    pcl_purchaseinterestduration_attr as (
    select
        distinct
            attributevalue,
            value
    from
        inbound.stringmapbase
    where
        AttributeName='pcl_purchaseinterestduration'
        and snapshot_dt = '20231122'
    ),
    pcl_interestlevelrating_attr as (
    select
        distinct
            attributevalue,
            value
    from
        inbound.stringmapbase
    where
        AttributeName='pcl_interestlevelrating'
        and snapshot_dt = '20231122'
    ),
    pcl_leadsource_attr as (
    select
        distinct
            attributevalue,
            value
    from
        inbound.stringmapbase
    where
        AttributeName='pcl_leadsource'
        and snapshot_dt = '20231122'
    )

select
    leadbase.*,
    concat('PM', lpad(CAST((row_number() over(order by leadid)) as string), 9, '0')) prospectId,
    pcl_prospectsubcategory_attr.value prospectTypeSrc,
    pcl_prospectcategory_attr.value prospectcategory1,
    coalesce(pcl_purchaseoption_attr.value , 'Cash Purchase') pcl_purchase_option,
    pcl_purchaseoption_attr.attributevalue attributevalue_po,
    coalesce(pcl_liketoexchangeoldcar_attr.value, 'No') pcl_interestedinexchange_new,
    pcl_purchaseinterestduration_attr.value pcl_purchaseinterestduration_attr_value,
    CASE
        WHEN pcl_purchaseinterestduration_attr.value = '1-2 Months' THEN '1-2 months'
        WHEN pcl_purchaseinterestduration_attr.value = '2-3 Months' THEN '2-3 months'
        WHEN pcl_purchaseinterestduration_attr.value = 'After3 Months' THEN '>3 months'
        WHEN pcl_purchaseinterestduration_attr.value = '<= 1 Month' THEN '<1 month'
        WHEN pcl_purchaseinterestduration_attr.value IS NULL THEN '>3 months'
    END AS pcl_purchaseinterestduration_du,
    pcl_interestlevelrating_attr.value AS pcl_interestlevelrating_attr_value,
    CASE
        WHEN pcl_interestlevelrating_attr.value = 'Hot customer' THEN 'Hot'
        WHEN pcl_interestlevelrating_attr.value = 'Cold customer' THEN 'Cold'
        WHEN pcl_interestlevelrating_attr.value = 'Warm customer' THEN 'Warm'
        WHEN pcl_interestlevelrating_attr.value = 'DDC' THEN 'Deferred Decision'
        WHEN pcl_interestlevelrating_attr.value IS NULL THEN 'Deferred Decision'
    END AS pcl_interestlevelrating_ra,
    
    pcl_leadsource_attr.value pcl_leadsource_ls,
    systemuserbasemigration.agentId,
    systemuserbasemigration.agentName,
    
    accountbasemigration.pcl_forcode acc_pcl_forcode,
    accountbasemigration.pcl_outletcode acc_pcl_outletcode,
    accountbasemigration.pcl_dealercode acc_pcl_dealercode,
    accountbasemigration.pcl_dealeruniquecode acc_pcl_dealeruniquecode,
    accountbasemigration.accountid acc_accountid
from
    leadbase
left join
    accountbasemigration
    on (accountbasemigration.accountid=leadbase.pcl_dealerid)
left join
    systemuserbasemigration
    on (systemuserbasemigration.systemuserid=leadbase.createdby)
left join
    pcl_prospectsubcategory_attr
    on (leadbase.pcl_prospectsubcategory=pcl_prospectsubcategory_attr.attributevalue)
left join
    pcl_prospectcategory_attr
    on (leadbase.pcl_prospectcategory=pcl_prospectcategory_attr.attributevalue)
left join
    pcl_purchaseoption_attr
    on (leadbase.pcl_purchaseoption=pcl_purchaseoption_attr.attributevalue)
left join
    pcl_liketoexchangeoldcar_attr
    on (leadbase.pcl_liketoexchangeoldcar=pcl_liketoexchangeoldcar_attr.attributevalue)
left join
    pcl_purchaseinterestduration_attr
    on (leadbase.pcl_purchaseinterestduration=pcl_purchaseinterestduration_attr.attributevalue)
left join
    pcl_interestlevelrating_attr
    on (leadbase.pcl_interestlevelrating=pcl_interestlevelrating_attr.attributevalue)

left join
    pcl_leadsource_attr
    on (leadbase.pcl_leadsource=pcl_leadsource_attr.attributevalue)
    
''').withColumnRenamed("companyname","companyname_src")\
    .withColumn("acc_pcl_dealercode" , F.trim(F.col("acc_pcl_dealercode")))
    #.select("Leadid","prospectId","dmsEnquiryId","dmsEnquiryCreatedOn","dmsEnquiryExist","queryDescription",
    #          "carRegistrationNumber","lob","createdOn","companyName","createdby","pcl_prospectsubcategory","pcl_prospectcategory",
    #          "pcl_purchaseoption","pcl_liketoexchangeoldcar","pcl_purchaseinterestduration","pcl_interestlevelrating",
    #          "pcl_leadsource","pcl_contactid","pcl_variantid","pcl_dealercode","pcl_dealeremail","pcl_dealeruniquecode","pcl_dealerforcode","pcl_dealerid",
    #          "enquiryType","pcl_prospect","pcl_modelid","pcl_variantid")
    #.withColumn("prospectId", F.concat(F.expr("'P'"), F.lpad(F.expr("CAST(RAND() * POW(10, 10) AS STRING)"), 11, '0')))\
    #.withColumn("prospectId", F.expr("replace(prospectId, '.', '')"))\

#df_contact_dl_new = df_contact_dl.distinct().select("cdl_mobileNumber","reference_number","firstName","lastName","cdl_emailid","msilContact","customerType","modeOfCommunication")
df_stage1 = df_stage1.join(df_contact_dl,df_stage1["pcl_contactid"]==df_contact_dl["reference_number"] , how= "left")
df_stage1 = df_stage1.cache()
print (f"df_stage1_count : {df_stage1.count()}")



df_stage1.createOrReplaceTempView('df_stage1')
df_stage1 = spark.sql('''
select
    *,
    coalesce(mobile_no, '1234567890') cdl_mobileNumber,
    coalesce(first_name, 'DONTHIREDDY') cdl_firstName,
    coalesce(last_name, 'REDDY') cdl_lastName,
    coalesce(email_id, 'na@na.com') cdl_emailid
from
    df_stage1
    
   
''').withColumn("msilContact", F.concat_ws(" ", F.col("cdl_firstName"), F.col("cdl_lastName")))
           



df_stage1 = df_stage1.drop('leadqualitycode', 'prioritycode', 'industrycode', 'preferredcontactmethodcode',
    'salesstagecode', 'owningbusinessunit', 'subject', 'participatesinworkflow',
    'description', 'estimatedvalue', 'estimatedclosedate', 'revenue', 'numberofemployees',
    'donotphone', 'sic', 'donotfax', 'pager', 'statuscode', 'versionnumber', 'masterid',
    'donotsendmm', 'merged', 'donotbulkemail', 'lastusedincampaign', 'transactioncurrencyid',
    'timezoneruleversionnumber', 'utcconversiontimezonecode', 'importsequencenumber',
    'overriddencreatedon', 'exchangerate', 'estimatedamount', 'estimatedamount_base',
    'revenue_base', 'stageid', 'decisionmaker', 'need', 'budgetamount', 'qualificationcomments',
    'qualifyingopportunityid', 'schedulefollowup_qualify', 'confirminterest', 'parentaccountid',
    'originatingcaseid', 'schedulefollowup_prospect', 'entityimageid', 'parentcontactid',
    'initialcommunication', 'salesstage', 'budgetstatus', 'processid', 'purchasetimeframe',
    'purchaseprocess', 'relatedobjectid', 'evaluatefit', 'budgetamount_base', 'traversedpath',
    'slainvokedid', 'pcl_3hrcustomerconnectstatus', 'pcl_acrmifalreadyboughtthevehicle',
    'pcl_acrmmodelid', 'pcl_addressconfirmed', 'pcl_advisornamescript', 'pcl_agegroup',
    'pcl_agentnamescript', 'pcl_agentscript1', 'pcl_agentscript10', 'pcl_agentscript11',
    'pcl_agentscript12', 'pcl_agentscript13', 'pcl_agentscript14', 'pcl_agentscript15',
    'pcl_agentscript16', 'pcl_agentscript17', 'pcl_agentscript18', 'pcl_agentscript19',
    'pcl_agentscript2', 'pcl_agentscript20', 'pcl_agentscript21', 'pcl_agentscript22',
    'pcl_agentscript23', 'pcl_agentscript24', 'pcl_agentscript25', 'pcl_agentscript26',
    'pcl_agentscript27', 'pcl_agentscript28', 'pcl_agentscript29', 'pcl_agentscript3',
    'pcl_agentscript30', 'pcl_agentscript31', 'pcl_agentscript32', 'pcl_agentscript33',
    'pcl_agentscript34', 'pcl_agentscript35', 'pcl_agentscript36', 'pcl_agentscript37',
    'pcl_agentscript38', 'pcl_agentscript39', 'pcl_agentscript4', 'pcl_agentscript40',
    'pcl_agentscript41', 'pcl_agentscript42', 'pcl_agentscript43', 'pcl_agentscript44',
    'pcl_agentscript45', 'pcl_agentscript46', 'pcl_agentscript48', 'pcl_agentscript49',
    'pcl_agentscript5', 'pcl_agentscript50', 'pcl_agentscript51', 'pcl_agentscript52',
    'pcl_agentscript53', 'pcl_agentscript54', 'pcl_agentscript55', 'pcl_agentscript56',
    'pcl_agentscript57', 'pcl_agentscript58', 'pcl_agentscript59', 'pcl_agentscript6',
    'pcl_agentscript60', 'pcl_agentscript61', 'pcl_agentscript62', 'pcl_agentscript63',
    'pcl_agentscript64', 'pcl_agentscript65', 'pcl_agentscript66', 'pcl_agentscript67',
    'pcl_agentscript68', 'pcl_agentscript69', 'pcl_agentscript7', 'pcl_agentscript70',
    'pcl_agentscript71', 'pcl_agentscript72', 'pcl_agentscript73', 'pcl_agentscript74',
    'pcl_agentscript75', 'pcl_agentscript76', 'pcl_agentscript77', 'pcl_agentscript78',
    'pcl_agentscript79', 'pcl_agentscript8', 'pcl_agentscript9', 'pcl_alternateareacodeacrm',
    'pcl_alternateofficelandlinenumberacrm', 'pcl_authoriseddealership', 'pcl_averagekms',
    'pcl_boughtacar', 'pcl_brouchure', 'pcl_calledfrom', 'pcl_callorigin', 'pcl_calltableid',
    'pcl_campaignid', 'pcl_carbought', 'pcl_carbought1', 'pcl_carbought1id', 'pcl_carboughtacrmid',
    'pcl_carboughtid', 'pcl_changecityscript', 'pcl_changedmodelacrmid', 'pcl_changedmodelid',
    'pcl_changedvariantacrmid', 'pcl_changedvariantid', 'pcl_changeinmodelinterested',
    'pcl_cityid', 'pcl_companyid', 'pcl_competitormodelid', 'pcl_competitorvariantid',
    'pcl_configid', 'pcl_convenienttimescript', 'pcl_cpurdealerscript', 'pcl_cpurdealerscript1',
    'pcl_custnamescript', 'pcl_customermodelacrm', 'pcl_customermodelscript',
    'pcl_customermodelscript1', 'pcl_customermodelscript2', 'pcl_dateandtimefortestdrive',
    'pcl_datetimefortestdrive', 'pcl_datetimeforvisit', 'pcl_dealeracrmid', 'pcl_dealercodeacrm',
    'pcl_dealercontacted', 'pcl_dealernamescript', 'pcl_dealershipscript', 'pcl_dealertypeacrmid',
    'pcl_dealertypeid', 'pcl_dealeruniquecode_leadbase', 'pcl_didyouvisitthedealership',
    'pcl_disclaimerscript', 'pcl_disclaimerscript1', 'pcl_disclaimerscript2', 'pcl_disqualificationreason',
    'pcl_isthisrighttime', 'pcl_kbcityid', 'pcl_kbmodelid', 'pcl_kbregionid', 'pcl_kilometer',
    'pcl_km', 'pcl_lastintaractiondate', 'pcl_nexadealeracrmid', 'pcl_noofcarsinterestedin',
    'pcl_noofcarsowned', 'pcl_nri', 'pcl_nricity', 'pcl_nricountryid', 'pcl_nristateid',
    'pcl_numberofmonths', 'pcl_occupationsubtype', 'pcl_occupationtype', 'pcl_offerid',
    'pcl_othercarconsidered1id', 'pcl_othercarconsidered2id', 'pcl_otherconsideration1id',
    'pcl_otherconsideration2id', 'pcl_otheroccupation', 'pcl_otherreason', 'pcl_othersource',
    'pcl_otherstate', 'pcl_ownedvehicletype', 'pcl_residencelandlineareacode',
    'pcl_residencelandlineareacode_acrm', 'pcl_residencelandlinecountrycode',
    'pcl_residencelandlinecountrycode_acrm', 'pcl_residencelandlinenumber',
    'pcl_residencelandlinenumber_acrm', 'pcl_righttime', 'pcl_script', 'pcl_script1',
    'pcl_smskeyfield', 'pcl_stillusingthevehicle', 'pcl_stillusingthevehicleacrm',
    'pcl_targetmodel', 'pcl_targetmodelacrmid', 'pcl_targetmodelid', 'pcl_targetmodelmarutiid',
    'pcl_targetmodelscript', 'pcl_targetmodelscript1', 'pcl_targetvariantacrmid',
    'pcl_targetvariantid', 'pcl_targetvariantmarutiid', 'pcl_territoryid', 'pcl_testdrive',
    'pcl_testdrivefeature', 'pcl_testdriveinterest', 'pcl_testdriverequest', 'pcl_title',
    'pcl_toberemoved', 'pcl_tollfreehelplinenumber', 'pcl_tsmcontactid', 'pcl_tsmemail',
    'pcl_tsmid', 'pcl_vehicleyearscript', 'pcl_verifiedmobilenumber', 'pcl_verifiedmobilenumber3',
    'pcl_verifiedmobilenumber4', 'pcl_websiteurl', 'pcl_wheredidyouhearaboutusreadonly',
    'pcl_registratioinnumberscript', 'pcl_relationshipwithbeneficiary', 'pcl_planingtobuycar',
    'pcl_pricelistid', 'pcl_productfeature', 'pcl_profession', 'pcl_modelscript', 'pcl_modeltype',
    'pcl_month', 'pcl_monthlykm', 'pcl_monthname1', 'isprivate', 'fax', 'isautocreate',
    'modifiedonbehalfby', 'createdonbehalfby', 'followemail', 'timespentbymeonemailandmeetings',
    'yomimiddlename', 'yomilastname', 'jobtitle', 'salutation', 'donotemail', 'owneridtype',
    'customeridyominame', 'slaid', 'onholdtime', 'lastonholdtime', 'pcl_customercityscript',
    'pcl_customeremailid1', 'pcl_customeremailid2', 'pcl_dealeremailacrm', 'pcl_dealerid',
    'pcl_dob', 'pcl_duration', 'pcl_durationacrm', 'pcl_emailacrm', 'pcl_faxnumber', 'pcl_fueltype',
    'pcl_ifbmc', 'pcl_immediateprospect', 'pcl_marketreachtool', 'pcl_membersinfamily',
    'pcl_prospect_agegroup_dd', 'pcl_quotationid', 'pcl_reasonforloss', 'pcl_year', 'pcl_year1id',
    'pcl_yearcount', 'pcl_yearid', 'pcl_yearscript', 'pcl_zoneacrmid', 'pcl_changecityacrmid',
    'pcl_customercityid', 'pcl_mobilecountrycode_acrm', 'pcl_mobilenumber_acrm',
    'pcl_mobileareacodeacrm', 'pcl_customerfirstname_acrm', 'pcl_customermiddlename_acrm',
    'pcl_customerlastnameacrm', 'pcl_purchasedealerscript', 'pcl_purchasedealerscript1',
    'donotpostalmail', 'pcl_beneficiaryaddress1', 'pcl_beneficiaryname',
    'pcl_beneficiarysphonenumber1', 'pcl_beneficiarysphonenumber1areacode',
    'pcl_beneficiarysphonenumber1countrycode', 'pcl_beneficiarysphonenumber2',
    'pcl_beneficiarysphonenumber2areacode', 'pcl_beneficiarysphonenumber2countrycode','pcl_phonenumber3',
'pcl_phonenumber3areacode','pcl_phonenumber3countrycode')





df_stage2 = df_stage1.withColumn("prospectType",F.when(F.col("prospectTypeSrc")=="Suzuki Connect","Enquiry Suzuki Connect")\
                       .when(F.col("prospectTypeSrc")=="MGP","Enquiry MSGA & MSGP")\
                       .when(F.col("prospectTypeSrc")=="MGA","Enquiry MSGA & MSGP")\
                       .when(F.col("prospectTypeSrc")=="True Value","Enquiry True Value")\
                       .when(F.col("prospectTypeSrc")=="True Value Sell","Enquiry True Value")\
                       .when(F.col("prospectTypeSrc")=="Models/Variants","Enquiry New Car")\
                       .when(F.col("prospectTypeSrc")=="Prices","Enquiry New Car")\
                       .when(F.col("prospectTypeSrc")=="Colours","Enquiry New Car")\
                       .when(F.col("prospectTypeSrc")=="Driving School","Enquiry MSDS")\
                       .when(F.col("prospectTypeSrc")=="Add Campaigns","Enquiry New Car")\
                       .when(F.col("prospectTypeSrc")=="Schemes","Enquiry New Car")\
                       .when(F.col("prospectTypeSrc")=="Delivery Time/Availability","Enquiry New Car")\
                       .when(F.col("prospectTypeSrc")=="CSD Sales","Enquiry New Car")\
                       .when(F.col("prospectTypeSrc")=="Test Drive","Enquiry New Car")\
                       .when(F.col("prospectTypeSrc")=="Finance","Enquiry New Car")\
                       .when(F.col("prospectTypeSrc")=="Insurance","Enquiry New Car")\
                       .when(F.col("prospectTypeSrc")=="Extended Warranty","Enquiry New Car")) 

df_stage2 = df_stage2.filter(col("prospectType").isNotNull())


df_stage2 = df_stage2.withColumn("prospectCode",F.when(F.col("prospectType")=='Enquiry Suzuki Connect',"PR005")\
                                .when(F.col("prospectType")=='Enquiry MSGA & MSGP',"PR003")\
                                .when(F.col("prospectType")=='Enquiry MSDS',"PR004")\
                                .when(F.col("prospectType")=='Enquiry True Value',"PR002") 
                                .when(F.col("prospectType")=='Enquiry New Car',"PR001"))
                               

#df_stage2.select("prospectType").distinct().show()

df_stage2.createOrReplaceTempView('stage2')

#BUY sell logic 
df_stage2 = spark.sql('''
select
    *,
    case 
        when prospectType='Enquiry True Value' and prospectTypeSrc<>'True Value Sell' then 'Buy'
        when prospectType='Enquiry True Value' and prospectTypeSrc='True Value Sell' then 'Sell'
        else Null
    end as buySell
from
    stage2
''').withColumn("enquiryType", (F.when((F.col("buySell")=="Sell"),"Sell Only")))

df_stage2 = df_stage2.withColumn("leadSource", F.when(F.col("pcl_leadsource")==9,"Suzuki Connect Inbound")\
                    .otherwise(F.col("pcl_leadsource_ls")))


#df_stage2.groupBy(F.col('prospectType')).count().show(truncate=False)

#######################################################################################################################

df_stage2.createOrReplaceTempView('stage2')
df_stage2= spark.sql('''
select 
    *,
    case
        when prospectType='Enquiry New Car'
        then 
            array(
                struct('QM004' questionId, array(coalesce(pcl_interestedinexchange_new, '')) response), 
                struct('QM005' questionId, array(coalesce(pcl_purchase_option, '')) response), 
                struct('QM006' questionId, array(coalesce(pcl_purchaseinterestduration_du, ''), coalesce(pcl_interestlevelrating_ra, '')) response)
            )
        when prospectType='Enquiry MSGA & MSGP'
        then 
            array(
                struct('QM011' questionId, array(coalesce(pcl_purchaseinterestduration_du, ''), coalesce(pcl_interestlevelrating_ra, '')) response)
            )
        when prospectType='Enquiry MSDS'
        then 
            array(
                struct('QM012' questionId , array(coalesce(pcl_purchaseinterestduration_du, ''), coalesce(pcl_interestlevelrating_ra, '')) response)
            )
        when prospectType='Enquiry Suzuki Connect'
        then 
            array(
                struct('QM006' questionId, array(coalesce(pcl_purchaseinterestduration_du, ''), coalesce(pcl_interestlevelrating_ra, '')) response)
            )
        when prospectType = 'Enquiry True Value'
        then 
            case
                when buysell = 'Buy'
                then array(
                    struct('QM004' questionId, array(coalesce(pcl_interestedinexchange_new, '')) response), 
                           struct('QM005' questionId, array(coalesce(pcl_purchase_option, '')) response), 
                           struct('QM010' questionId, array(coalesce(pcl_purchaseinterestduration_du, ''), coalesce(pcl_interestlevelrating_ra, '')) response)
                )
                when buysell = 'Sell'
                then array(
                    struct('QM004' questionId, array(coalesce(pcl_interestedinexchange_new, '')) response), 
                           struct('QM005' questionId, array('') response), 
                           struct('QM010' questionId, array('>3 months', 'Deferred Decision') response)
                )
           end
        else Null
    end as new_response
from
    stage2
''')

df_stage2.count()
#df_stage2.printSchema()


###Condition to get TrueValue and Sales Prospect from Leadbase table(stage2) 
     # &
### Creation of mul_code and for_code from acc_pcl_dealeruniquecode from( JOINED accountbase+leadbase table)
df_stage2.createOrReplaceTempView('stage2')

df_stage3 = spark.sql('''
select
    *,
    case
        when trim(prospectcategory1)='Other Services' and trim(prospectType)='Enquiry True Value'
        then 'TV'
        else 'Sales'
        end as lob_name,
    case
        when trim(prospectcategory1)='Other Services' and trim(prospectType)='Enquiry True Value'
        then substring(substring_index(substring_index(acc_pcl_dealeruniquecode, '-', 2), '-', -1),1,4)
        else acc_pcl_dealercode
        end as mulcode_self,
    case
        when trim(prospectcategory1)='Other Services' and trim(prospectType)='Enquiry True Value'
        then substring_index(substring_index(acc_pcl_dealeruniquecode, '-', 3), '-', -1)
        else substring_index(substring_index(acc_pcl_dealeruniquecode, '-', 3), '-', -1)
        end as forcode_self
from
    stage2
''')
df_stage3.createOrReplaceTempView('stage3')
df_stage3.count()

###SALES
df_stage3 = df_stage3.withColumn("2NDmul_code",when((col("mulcode_self").isNull()) & (col("acc_pcl_dealeruniquecode").isNotNull()), expr("substring_index(substring_index(acc_pcl_dealeruniquecode, '-', 1), '-', -1)"))).withColumn("2NDfor_code", when((col("mulcode_self").isNull()) & (col("acc_pcl_dealeruniquecode").isNotNull()), expr("substring_index(substring_index(acc_pcl_dealeruniquecode, '-', -2), '-', 1)")))
df_stage3.createOrReplaceTempView('stage3')

df_stage3_sales=spark.sql('''
select
    *
from (
    select
        *,
        row_number() over(partition by leadid order by sallivedate desc) rn
    from
        stage3
    left join
        (select distinct * from dealermaster where dealertype in ('S' ,'3S','2S')) dm
        on dm.mulcode_dealer=stage3.mulcode_self or dm.mulcode_dealer = stage3.acc_pcl_dealercode
        and
        dm.forcode_dealer = stage3.forcode_self
    where
        lob_name='Sales'
)
where
    rn=1
''')


#####Fetching nulls and notnulls after 1st JOIN Operation




df_stage3_sales_not_null_first_join = df_stage3_sales.filter("mulcode_dealer is not null").drop('rn')
df_stage3_sales_null_first_join = df_stage3_sales.filter("mulcode_dealer is null").drop('rn')

print (df_stage3_sales_not_null_first_join.count())
print (df_stage3_sales_null_first_join.count())

df_stage3_sales_null_first_join = df_stage3_sales_null_first_join.drop('type1', 'dealertype', 'name', 'forcode_dealer', 'mulcode_dealer', 'contactnumber_dealer', 'email_dealer', 'uniquecode_dealer', 'outletcd', 'address1_dealer', 'address2_dealer', 'address3_dealer', 'city_dealer', 'sallivedate', 'citycode_dealer', 'state_dealer', 'statecode_dealer', 'region_dealer', 'regioncode_dealer', 'zone', 'mapcd', 'loccd', 'channel', 'code')

df_stage3_sales_null_first_join.createOrReplaceTempView('stage3_sales_null_first_join')


######2nd JOIN operation on nullset to reduce nulls

####Join Leadbase-Nullset with dealer(type=Sales) on mulcode and forcode




df_stage3_sales_null_second_join=spark.sql('''
select
    *
from (
    select
        *,
        row_number() over(partition by leadid order by sallivedate desc) rn
    from
        stage3_sales_null_first_join
    left join
        (select distinct * from dealermaster where dealertype in ('S', '3S','2S')) dm
        on dm.mulcode_dealer=stage3_sales_null_first_join.mulcode_self or dm.mulcode_dealer = stage3_sales_null_first_join.acc_pcl_dealercode
    where
        lob_name='Sales'
)
where
    rn=1
''')


# ####Fetchig nulls and not nulls after 2nd JOIN operation

df_stage3_sales_null_second_join_not_null = df_stage3_sales_null_second_join.filter("mulcode_dealer is not null").drop('rn')
df_stage3_sales_null_second_join_null = df_stage3_sales_null_second_join.filter("mulcode_dealer is null").drop('rn')

#df_stage3_tv_null_second_join_null.filter("mulcode_src is null").show(10)
print(df_stage3_sales_null_second_join_null.count())


# #df_stage3_sales_null_second_join_null.select("acc_pcl_dealeruniquecode","mulcode_src", "forcode_src").distinct().show(100)



df_stage3_sales_null_second_join_null = df_stage3_sales_null_second_join_null.drop('type1', 'dealertype', 'name', 'forcode_dealer', 'mulcode_dealer', 'contactnumber_dealer', 'email_dealer', 'uniquecode_dealer', 'outletcd', 'address1_dealer', 'address2_dealer', 'address3_dealer', 'city_dealer', 'sallivedate', 'citycode_dealer', 'state_dealer', 'statecode_dealer', 'region_dealer', 'regioncode_dealer', 'zone', 'mapcd', 'loccd', 'channel', 'code')

df_stage3_sales_null_second_join_null.createOrReplaceTempView('stage3_sales_null_second_join')


# ######3nd JOIN operation on nullset to reduce nulls

# ####Join Leadbase-Nullset with dealer(type=Sales) on mulcode and forcode




df_stage3_sales_null_third_join=spark.sql('''
select
    *
from (
    select
        *,
        row_number() over(partition by leadid order by sallivedate desc) rn
    from
        stage3_sales_null_second_join
    left join
        (select distinct * from dealermaster where dealertype in ('S', '3S','2S')) dm
        on dm.mulcode_dealer=stage3_sales_null_second_join.2NDmul_code or dm.mulcode_dealer = stage3_sales_null_second_join.acc_pcl_dealercode
        and
        dm.forcode_dealer = stage3_sales_null_second_join.2NDfor_code
    where
        lob_name='Sales'
)
where
    rn=1
''')




# #####Fetching nulls and notnulls after 4tht JOIN Operation




df_stage3_sales_not_null_third_join = df_stage3_sales_null_third_join.filter("mulcode_dealer is not null").drop('rn')
df_stage3_sales_null_third_join = df_stage3_sales_null_third_join.filter("mulcode_dealer is null").drop('rn')

#print (df_stage3_sales_not_null_third_join.count())
print (df_stage3_sales_null_third_join.count())

df_stage3_sales_null_third_join = df_stage3_sales_null_third_join.drop('type1', 'dealertype', 'name', 'forcode_dealer', 'mulcode_dealer', 'contactnumber_dealer', 'email_dealer', 'uniquecode_dealer', 'outletcd', 'address1_dealer', 'address2_dealer', 'address3_dealer', 'city_dealer', 'sallivedate', 'citycode_dealer', 'state_dealer', 'statecode_dealer', 'region_dealer', 'regioncode_dealer', 'zone', 'mapcd', 'loccd', 'channel', 'code')


df_stage3_sales_null_third_join.createOrReplaceTempView('stage3_sales_null_third_join')

# ######4th JOIN operation on nullset to reduce nulls

# ####Join Leadbase-Nullset with dealer(type=Sales) on mulcode and forcode

df_stage3_sales_null_fourth_join=spark.sql('''
select
    *
from (
    select
        *,
        row_number() over(partition by leadid order by sallivedate desc) rn
    from
        stage3_sales_null_third_join
    left join
        (select distinct * from dealermaster where dealertype in ('S', '3S','2S')) dm
        on dm.mulcode_dealer=stage3_sales_null_third_join.2NDmul_code or dm.mulcode_dealer = stage3_sales_null_third_join.acc_pcl_dealercode
    where
        lob_name='Sales'
)
where
    rn=1
''')


# ####Fetchig nulls and not nulls after 4TH JOIN operation

df_stage3_sales_null_fourth_join_not_null = df_stage3_sales_null_fourth_join.filter("mulcode_dealer is not null").drop('rn')
df_stage3_sales_null_fourth_join_null = df_stage3_sales_null_fourth_join.filter("mulcode_dealer is null").drop('rn')

#df_stage3_tv_null_fourth_join_null.filter("mulcode_src is null").show(10)
print(df_stage3_sales_null_fourth_join_null.count())


#df_stage3_sales_null_second_join_null.select("acc_pcl_dealeruniquecode","mulcode_src", "forcode_src").distinct().show(100)

####Passing default mulcode and forcode on remaining nullset of Leadbase(lefttable)

df_stage3_sales_null_fourth_join_null_lit = df_stage3_sales_null_fourth_join_null.withColumn('2NDmul_code', F.lit('9967'))\
                                    .withColumn('2NDfor_code', F.lit('95'))\
                                    .drop('type1', 'dealertype', 'name', 'forcode_dealer', 'mulcode_dealer', 'contactnumber_dealer', 'email_dealer', 'uniquecode_dealer', 'outletcd', 'address1_dealer', 'address2_dealer', 'address3_dealer', 'city_dealer', 'sallivedate', 'citycode_dealer', 'state_dealer', 'statecode_dealer', 'region_dealer', 'regioncode_dealer', 'zone', 'mapcd', 'loccd', 'channel', 'code')

df_stage3_sales_null_fourth_join_null_lit.createOrReplaceTempView('stage3_sales_null_fourth_join_null_lit')



####LeftJoin on DealerMaster table and passing record with mulcode=9967 and forcode=95 to all such Nullset 


df_stage3_sales_null_final_join_with_default = spark.sql('''
select
    *
from (
    select
        *,
        row_number() over(partition by leadid order by sallivedate desc) rn
    from
        stage3_sales_null_fourth_join_null_lit
    left join
        (select distinct * from dealermaster where dealertype in ('S', '3S','2S')) dm
        on dm.mulcode_dealer=stage3_sales_null_fourth_join_null_lit.2NDmul_code
    where
        lob_name='Sales'
)
where
    rn=1
''').drop('rn')






####Union of 3 set-----NOTNULL after 1s join ,NOTNULL after 2nd join , Final Null with Defaultvalues

df_stage4_sales = df_stage3_sales_not_null_first_join.unionAll(df_stage3_sales_null_second_join_not_null).unionAll(df_stage3_sales_not_null_third_join).unionAll(df_stage3_sales_null_fourth_join_not_null).unionAll(df_stage3_sales_null_final_join_with_default)
print (df_stage4_sales.count())
#df_stage4_sales.filter("mulcode_dealer is null").count()

###Window function to eradicte duplication

###Join Leadbase with Dealermaster(type=TV) on mulcode

df_stage3_tv=spark.sql('''
select
    *
from (
    select
        *,
        row_number() over(partition by leadid order by sallivedate desc) rn
    from
        stage3
    left join
        (select distinct * from dealermaster where dealertype='TV') dm
        on dm.mulcode_dealer=stage3.mulcode_self or dm.mulcode_dealer = stage3.acc_pcl_dealercode
    where
        lob_name='TV'
)
where
    rn=1
''')


#####Fetching nulls and notnulls after 1st JOIN Operation




df_stage3_tv_not_null_first_join = df_stage3_tv.filter("mulcode_dealer is not null").drop('rn')
df_stage3_tv_null_first_join = df_stage3_tv.filter("mulcode_dealer is null").drop('rn')

print (df_stage3_tv_not_null_first_join.count())
print (df_stage3_tv_null_first_join.count())

df_stage3_tv_null_first_join = df_stage3_tv_null_first_join.drop('type1', 'dealertype', 'name', 'forcode_dealer', 'mulcode_dealer', 'contactnumber_dealer', 'email_dealer', 'uniquecode_dealer', 'outletcd', 'address1_dealer', 'address2_dealer', 'address3_dealer', 'city_dealer', 'sallivedate', 'citycode_dealer', 'state_dealer', 'statecode_dealer', 'region_dealer', 'regioncode_dealer', 'zone', 'mapcd', 'loccd', 'channel', 'code')

df_stage3_tv_null_first_join.createOrReplaceTempView('stage3_tv_null_first_join')


######2nd JOIN operation on nullset to reduce nulls

####Join Leadbase-Nullset with dealer(type=Sales) on mulcode and forcode




df_stage3_tv_null_second_join=spark.sql('''
select
    *
from (
    select
        *,
        row_number() over(partition by leadid order by sallivedate desc) rn
    from
        stage3_tv_null_first_join
    left join
        (select distinct * from dealermaster where dealertype in ('S', '3S','2S')) dm
        on dm.mulcode_dealer=stage3_tv_null_first_join.mulcode_self or dm.mulcode_dealer = stage3_tv_null_first_join.acc_pcl_dealercode
    where
        lob_name='TV'
)
where
    rn=1
''')


####Fetchig nulls and not nulls after 2nd JOIN operation

df_stage3_tv_null_second_join_not_null = df_stage3_tv_null_second_join.filter("mulcode_dealer is not null").drop('rn')
df_stage3_tv_null_second_join_null = df_stage3_tv_null_second_join.filter("mulcode_dealer is null").drop('rn')

#df_stage3_tv_null_second_join_null.filter("mulcode_src is null").show(10)
print(df_stage3_tv_null_second_join_null.count())

#df_stage3_tv_null_second_join_null.select("acc_pcl_dealeruniquecode","mulcode_src", "forcode_src").distinct().show(100)

####Passing default mulcode and forcode on remaining nullset of Leadbase(lefttable)

df_stage3_tv_null_second_join_null = df_stage3_tv_null_second_join_null.withColumn('mulcode_self', F.lit('9967'))\
                                    .withColumn('forcode_self', F.lit('95'))\
                                    .drop('type1', 'dealertype', 'name', 'forcode_dealer', 'mulcode_dealer', 'contactnumber_dealer', 'email_dealer', 'uniquecode_dealer', 'outletcd', 'address1_dealer', 'address2_dealer', 'address3_dealer', 'city_dealer', 'sallivedate', 'citycode_dealer', 'state_dealer', 'statecode_dealer', 'region_dealer', 'regioncode_dealer', 'zone', 'mapcd', 'loccd', 'channel', 'code')

df_stage3_tv_null_second_join_null.createOrReplaceTempView('stage3_tv_null_second_join_null')



####LeftJoin on DealerMaster table and passing record with mulcode=9967 and forcode=95 to all such Nullset 


df_stage3_tv_null_final_join_with_default = spark.sql('''
select
    *
from (
    select
        *,
        row_number() over(partition by leadid order by sallivedate desc) rn
    from
        stage3_tv_null_second_join_null
    left join
        (select distinct * from dealermaster where dealertype in ('S', '3S','2S')) dm
        on dm.mulcode_dealer=stage3_tv_null_second_join_null.mulcode_self
    where
        lob_name='TV'
)
where
    rn=1
''').drop('rn')

####Union of 3 set-----NOTNULL after 1s join ,NOTNULL after 2nd join , Final Null with Defaultvalues

df_stage4_tv = df_stage3_tv_not_null_first_join.unionAll(df_stage3_tv_null_second_join_not_null).unionAll(df_stage3_tv_null_final_join_with_default)
print (df_stage4_tv.count())

#tv join with sales i.e. union
df_stage5_union = df_stage4_sales.unionAll(df_stage4_tv)
df_stage5_union.count()

#operation on model master api table only for correcting  MODEL &VARIANT values
df_modelmasterapi = spark.sql('''
with
    stg1 as (
        select
            distinct
                modelDesc,
                modelCd
        from
            modelmasterapi
    ),
    stg2 as (
        select
            modelDesc,
            'suffix' suffix
        from
            stg1
        group by
            modelDesc
        having
            count(*)>1
    ),
    stg3 as (
    select
        modelmasterapi.*,
        case
            when suffix='suffix' then modelCd
            else ''
        end as suffixvalue   
    from
        modelmasterapi
    left join
        stg2
        on modelmasterapi.modelDesc=stg2.modelDesc
    )
    select
        channel, colorType, ecolorCd, ecolorDesc, enquiryYN, fuelType, fuelTypeDesc, make, makeCd, modelCd, modelDesc, subscriptionYN, svarCode, validYN, variantCd, variantDesc,
        case
            when suffixvalue<>'' then concat(modelDesc, ' (' , suffixvalue, ')')
            else modelDesc
        end as newmodelDesc
    from
        stg3
''')
df_modelmasterapi.createOrReplaceTempView('modelmasterapi')

df_modelmasterapivariant = spark.sql('''
with
    stg1 as (
        select
            distinct
                variantDesc,
                variantCd
        from
            modelmasterapi
    ),
    stg2 as (
        select
            variantDesc,
            'suffix' suffix
        from
            stg1
        group by
            variantDesc
        having
            count(*)>1
    ),
    stg3 as (
    select
        modelmasterapi.*,
        case
            when suffix='suffix' then variantCd
            else ''
        end as suffixvalue   
    from
        modelmasterapi
    left join
        stg2
        on modelmasterapi.variantDesc=stg2.variantDesc
    )
    select
        channel, colorType, ecolorCd, ecolorDesc, enquiryYN, fuelType, fuelTypeDesc, make, makeCd, modelCd, modelDesc, subscriptionYN, svarCode, validYN, variantCd, variantDesc, newmodelDesc,
        case
            when suffixvalue<>'' then concat(variantDesc, ' (' , suffixvalue, ')')
            else variantDesc
        end as newvariantDesc
    from
        stg3
''').withColumnRenamed("channel","api_channel")

#MODEL API VALIDATION

df_stage5_union.createOrReplaceTempView('df_stage5_union')

df_stage6_model = spark.sql('''

Select *
from
    df_stage5_union
left join
    pcl_modelmasterbasemigration
    on (pcl_modelmasterbasemigration.pcl_modelmasterId=df_stage5_union.pcl_modelid)''')

#print(df_stage6_model.count())

####VARIANT

df_stage6_model.createOrReplaceTempView('df_stage6_model')

df_stage7_variant = spark.sql('''

Select *
from
    df_stage6_model
left join
    pcl_variantbase
    on (pcl_variantbase.pcl_variantid_vb=df_stage6_model.pcl_variantid)''')

df_stage7_variant.createOrReplaceTempView('df_stage7_variant')

#print(df_stage7_variant.count())



df_stage8_modelmaster_validation = df_modelmasterapivariant.select("variantcd","api_channel","modelcd","newmodeldesc","newvariantdesc").distinct()
df_stage8_modelmaster_validation = df_stage7_variant.join(df_stage8_modelmaster_validation, (df_stage7_variant.pcl_variantcode_vb==df_stage8_modelmaster_validation.variantcd),'left')
df_stage8_modelmaster_validation = df_stage8_modelmaster_validation.withColumn("model_api",F.lit(F.col("newmodeldesc")))\
                                                                    .withColumn("variant_api",F.lit(F.col("newvariantdesc")))\
                                                                    .drop("newmodeldesc","newvariantdesc")

df_stage8_modelmaster_validation.createOrReplaceTempView('df_stage8_modelmaster_validation')  

#type column maruti/non-maruti

df_stage9 = df_stage8_modelmaster_validation.withColumn("type",when(F.col("prospectType")=='Enquiry True Value',"Non-Maruti")\
                                                        .otherwise("NA"))
                                             
df_stage10 = df_stage9.withColumn("channel",when(F.col("type")=="Non-Maruti","True Value")\
                                  .otherwise(F.col("channel")))\
                      .withColumn("pcl_name",when((F.col("pcl_name").isNull()),"NA")\
                                                        .otherwise(F.col("pcl_name")))\
                      .withColumn("pcl_variantname",when((F.col("pcl_variantname").isNull()),"NA")\
                                                        .otherwise(F.col("pcl_variantname")))

#df_stage10 = df_stage10.withColumn("pcl_name",F.expr("substring(pcl_name, 1, 2000)"))\
#                      .withColumn("pcl_variantname",F.expr("substring(pcl_variantname, 1, 2000)"))

#putting oriinal model and variant name in modelapi for enquiry true value cases 

df_stage11 = df_stage10.withColumn("model_api",when(F.col("prospectType")=='Enquiry True Value',col("pcl_name"))\
                                   .otherwise(col("model_api")))

df_stage12_type = df_stage11.withColumn("variant_api",when(F.col("prospectType")=='Enquiry True Value',col("pcl_variantname"))\
                                   .otherwise(col("variant_api")))\
                            .withColumn("model_api",when((F.col("model_api").isNull()),'NA')\
                            .otherwise(col("model_api")))\
                            .withColumn("variant_api",when((F.col("variant_api").isNull()),'NA')\
                            .otherwise(col("variant_api")))

#channel renaming
df_stage13_channel = df_stage12_type.withColumn("channel",when((F.col("channel")=="NRM"),"Arena")\
                                       .when(F.col("channel")=="EXC","NEXA")\
                                       .when(F.col("channel")=="COM","Commercial")\
                                       .otherwise(F.col("channel")))

df_stage14_carregister = df_stage13_channel.withColumn("pcl_carregistrationnumber",when(F.col("prospectType")=='Enquiry Suzuki Connect',"TEMP1234")\
                                                        .otherwise("NA"))
                                                        
#VOC
df_stage15_voc= df_stage14_carregister.withColumn("column1", F.when(col("channel")=='NEXA',F.concat_ws("--->",F.col("pcl_remarks"),F.lit("Source_Model---"), F.col("pcl_name"), F.lit("CarRegNo---"), F.col("pcl_carregistrationnumber"),F.lit("Source_Variant---"),F.col("pcl_variantname")))\
                             .when(col("channel").isin(['Arena','Commercial']),F.concat_ws("--->",F.col("pcl_prospectremarks"), F.lit("Source_Model---"),F.col("pcl_name"),F.lit("CarRegNo---"),F.col("pcl_carregistrationnumber"),F.lit("Source_Variant---"), F.col("pcl_variantname")))\
                             .when(col("channel")=='MDS',F.concat_ws("--->",F.col("pcl_remarks"),F.col("pcl_prospectremarks"),F.lit("Source_Model---"), F.col("pcl_name"), F.lit("CarRegNo---"), F.col("pcl_carregistrationnumber"),F.lit("Source_Variant---"),F.col("pcl_variantname")))\
                              .when(col("channel")=='True Value',F.concat_ws("--->",F.col("pcl_remarks"),F.col("pcl_prospectremarks"),F.lit("Source_Model---"), F.col("pcl_name"), F.lit("CarRegNo---"), F.col("pcl_carregistrationnumber"),F.lit("Source_Variant---"),F.col("pcl_variantname"))))

# making query description and voc
df_stage16 = df_stage15_voc.withColumn("queryDescription",F.when(F.col("prospectType").isin(["Enquiry True Value","Enquiry MSGA & MSGP","Enquiry MSDS","Enquiry Suzuki Connect"]),F.col("column1")))\
                       .withColumn("voc",F.when(F.col("prospectType") == "Enquiry New Car",F.col("column1")))

df_stage16 = df_stage16.withColumn("queryDescription", F.expr("substring(queryDescription, 1, 2000)"))\
                       .withColumn("voc", F.expr("substring(voc, 1, 2000)"))  
                       
##channel correction as per souvik List

df_stage16.createOrReplaceTempView('df_stage16')
df_channel_corrected_df = spark.sql("""
select *,
coalesce(tmp_channel,channel) corrected_channel
from
(
SELECT 
*,
CASE 
    WHEN model_api IN ('ALTO (AL)', 'ALTO 800', 'ALTO K10 (AK)', 'ALTO K10 (AM)', 'CELERIO', 'DZIRE (DI)', 'DZIRE (DR)', 'EECO (R)', 'EECO (VR)', 'ERTIGA', 'ESTEEM (N)', 'GYPSY (G)', 'GYPSY (GY)', 'M 800 (CR)', 'NEW ALTO K10', 'NEW A-STAR', 'NEW BREZZA', 'NEW CELERIO', 'NEW ERTIGA', 'NEW SWIFT (SM)', 'NEW SWIFT (SR)', 'NEW SWIFT DZIRE', 'NEW WAGON-R (WM)', 'OMNI (OM)', 'RITZ (D)', 'RITZ (RZ)', 'S-PRESSO', 'SWIFT', 'SWIFT DZIRE (DZ)', 'SX4 (F)', 'SX4 (SX)', 'VITARA BREZZA ', 'WAGON R', 'WagonR', 'ZEN', 'ZEN ESTILO (M)', 'ZEN ESTILO (ZE)')
    THEN 'Arena'
    WHEN model_api IN ('BALENO (BA)', 'BALENO (E)', 'CIAZ', 'FRONX', 'GRAND VITARA (GV)', 'Ignis', 'JIMNY', 'NEW BALENO', 'S-CROSS', 'S-CROSS (SC)', 'S-CROSS (SS)', 'XL6') and prospectType <>'Enquiry True Value'
    THEN 'NEXA'
    WHEN model_api IN ('SUPER CARRY')
    THEN 'Commercial'
    WHEN prospectType IN ('Enquiry True Value')
    THEN 'True Value'
    WHEN channel IN ('MDS')
    THEN 'MDS'
    ELSE NULL
END AS tmp_channel
FROM df_stage16 )
""").drop('tmp_channel')

#passing default values to model & variant


df_stage17 = df_channel_corrected_df.withColumn("model_api",
    F.when((F.col("corrected_channel") == "Arena") & (col("model_api") == 'NA') & (col("variant_api") == 'NA'), "CIAZ")
    .when((F.col("corrected_channel") == "NEXA") & (col("model_api") == "NA") & (col("variant_api") == "NA"), "S-CROSS")
    .when((F.col("corrected_channel") == "Commercial") & (col("model_api") == "NA") & (col("variant_api") == "NA"), "SUPER CARRY")
    .otherwise(col("model_api"))
).withColumn("variant_api",
    F.when((F.col("corrected_channel") == "Arena") &  (col("variant_api") == "NA"), "MARUTI CIAZ SMART HYBRID ALPHA 1.5L 5MT (CIR4FZ2)")
    .when((F.col("corrected_channel") == "NEXA") &  (col("variant_api") == "NA"), "MARUTI S-CROSS SMART HYBRID ALPHA 1.5L 5MT")
    .when((F.col("corrected_channel") == "Commercial")  & (col("variant_api") == "NA"), "MARUTI SUPER CARRY CHASSIS 1.2L 5MT (CAR4AS4)")
    .otherwise(col("variant_api"))
).withColumn("modelcd",
    F.when((F.col("corrected_channel") == "Arena") & (col("model_api") == 'CIAZ') & (col("variant_api") == "MARUTI CIAZ SMART HYBRID ALPHA 1.5L 5MT (CIR4FZ2)"),"CI")
    .when((F.col("corrected_channel") == "NEXA") &  (col("model_api") == 'S-CROSS') & (col("variant_api") == "MARUTI S-CROSS SMART HYBRID ALPHA 1.5L 5MT"),"SS")
    .when((F.col("corrected_channel") == "Commercial")  & (col("model_api") == 'SUPER CARRY') & (col("variant_api") == "MARUTI SUPER CARRY CHASSIS 1.2L 5MT (CAR4AS4)"),"CA")
    .otherwise(col("modelcd"))
).withColumn("variantcd",
    F.when((F.col("corrected_channel") == "Arena") & (col("model_api") == 'CIAZ') & (col("variant_api") == "MARUTI CIAZ SMART HYBRID ALPHA 1.5L 5MT (CIR4FZ2)"),"CIR4FZ2")
    .when((F.col("corrected_channel") == "NEXA") &  (col("model_api") == 'S-CROSS') & (col("variant_api") == "MARUTI S-CROSS SMART HYBRID ALPHA 1.5L 5MT"),"SSR4AZ2")
    .when((F.col("corrected_channel") == "Commercial")  & (col("model_api") == 'SUPER CARRY') & (col("variant_api") == "MARUTI SUPER CARRY CHASSIS 1.2L 5MT (CAR4AS4)"),"CAR4AS4")
    .otherwise(col("variantcd"))
)

df_stage17_lob = df_stage17.withColumn("lob",F.when(F.col("prospectType")=="Enquiry True Value","True Value")\
                                       .when(F.col("prospectType")=='Enquiry Suzuki Connect',"Suzuki Connect")\
                                       .when(F.col("prospectType")=='Enquiry MSDS',"MSDS")\
                                       .when((F.col("prospectType")=='Enquiry New Car') & ((F.col("corrected_channel")=="NEXA") | (F.col("corrected_channel") == "Arena")), "Sales")\
                                       .when((F.col("prospectType")=='Enquiry New Car') & (F.col("corrected_channel")=="Commercial"),"Commercial")\
                                       .when((F.col("prospectType")=='Enquiry New Car') & (F.col("corrected_channel")=="MDS"),"MSDS")\
                                       .when((F.col("prospectType")=='Enquiry MSGA & MSGP') & (F.col("type1")== 'DDL'), "Accessories")\
                                       .when((F.col("prospectType")=='Enquiry MSGA & MSGP') & (F.col("type1")== 'DDT'), "Spares")\
                                       .when((F.col("prospectType")=='Enquiry MSGA & MSGP') & (F.col("type1")== 'DMM'),"Sales"))
                                       
#S,2s,3s,Tv----Name alteration

df_stage17_dealertype = df_stage17_lob.withColumn(
    "dealertype",
    when((col("dealertype") == "S"), "Sales Dealer")
    .when(col("dealertype") == "3S", "SALES + SERVICE + SPARES DEALER")
    .when(col("dealertype") == "TV", "True Value")
    .when(col("dealertype") == "2S", "Service + Spares Dealer")
    .otherwise(col("dealertype")))   

df_stage18 = df_stage17_dealertype.withColumn("carColor",F.lit("NA"))\
                       .withColumn("modeOfCommunication",F.lit("NA"))\
                       .withColumn("buyerType",F.lit("NA"))\
                       .withColumn("primaryUsage",F.lit("NA"))\
                       .withColumn("customerProfile",F.lit("NA"))\
                       .withColumn("licenseAvailable",F.lit("NA"))\
                       .withColumn("usageArea",F.lit("NA"))\
                       .withColumn("avgLoadCarrier",F.lit("NA"))\
                       .withColumn("averageRunningDay",F.lit("NA"))\
                       .withColumn("accessoriesInterested",F.lit("NA"))\
                       .withColumn("accessoriesPartNumber",F.lit("NA"))\
                       .withColumn("sparePartInterested",F.lit("NA"))\
                       .withColumn("sparePartNumber",F.lit("NA"))\
                       .withColumn("followUpDate",F.lit("NA"))\
                       .withColumn("action",F.lit("NA"))\
                       .withColumn("stage",F.lit("NA"))\
                       .withColumn("sourceCampaign",F.lit("Inbound"))\
                       .withColumn("countryCode",F.lit("NA"))\
                       .withColumn("alternateContactNumber",F.lit("NA"))\
                       .withColumn("colorCode",F.lit("NA"))\
                       .withColumn("customerType", F.lit("NA"))\
                       .withColumn("dmsEnquiryId", F.lit("NA"))\
                       .withColumn("dmsEnquiryCreatedOn", F.lit("NA"))\
                       .withColumn("dmsEnquiryExist", F.lit(False))\
                       .withColumn("createdByDefault", F.lit("vivekgupta04@maruticrm.onmicrosoft.com"))\
                       .withColumn("AdminCreatedon",date_format(col('createdOn_leadbase'), 'dd/MM/yyyy'))\
                       .withColumn("SecondCreatedon",date_format(to_timestamp(col('createdOn_leadbase'), 'yyyy-MM-dd HH:mm:ss'), 'dd/MM/yyyy HH:mm:ss'))\
                       .withColumn("createdonDB",from_utc_timestamp(col('createdOn_leadbase'), 'UTC'))\
                       .withColumn("uniquecode_dealer" , F.concat_ws("-",F.col("mulcode_dealer"),F.col("forcode_dealer"),F.col("outletcd")))


df_final = df_stage18.select("leadid","prospectId", "createdon",
"prospectCode", 
"prospectType", 
"agentId", 
"agentName",
"createdByDefault",
"SecondCreatedon",                             
"dmsEnquiryId", 
"AdminCreatedon",
"createdonDB",                             
"dmsEnquiryCreatedOn", 
"dmsEnquiryExist",
"lob",                             
"leadSource", 
"buySell", 
"enquiryType", 
#"customerDetails", 
"msilContact", 
"cdl_firstName", 
"middle_name", 
"cdl_lastName", 
"cdl_emailId", 
"countryCode", 
"cdl_mobileNumber", 
"alternateContactNumber", 
"customerType", 
"queryDescription",  
"pcl_carregistrationnumber", 
"modeOfCommunication",                             
#"vehicleDetails", 
"corrected_channel", 
"model_api",  
"variant_api", 
"variantcd", 
"type", 
"manufacturingYear", 
"temporaryRegistration",
"colorCode" ,
"modelcd",                             
#"dealerDetails", 
"type1", 
"dealertype", 
"name", 
"code", 
"forcode_dealer", 
"mulcode_dealer",
"outletcd",                                                          
"contactnumber_dealer", 
"email_dealer", 
"uniquecode_dealer", 
"address1_dealer", 
"address2_dealer", 
"address3_dealer", 
"city_dealer", 
"citycode_dealer", 
"state_dealer", 
"statecode_dealer", 
"region_dealer", 
"regioncode_dealer", 
"zone",
"loccd",                              
#"purchaseDetails", 
#"interests",  
"new_response", 
"buyerType", 
"primaryUsage", 
"customerProfile", 
"licenseAvailable", 
"usageArea", 
"avgLoadCarrier", 
"averageRunningDay", 
"accessoriesInterested", 
"accessoriesPartNumber", 
"sparePartInterested", 
"sparePartNumber", 
"carColor", 
#"enrollmentDetails", 
#"interests",  
"voc", 
#"customerFollowUp", 
"followUpDate", 
"action", 
"stage", 
#"administration", 
"createdon_leadbase", 
"sourceCampaign",  
"companyname_src"
)

df_final = df_final.cache()
#df_final = df_final.limit(2)

#df_final.createOrReplaceTempView('df_final')

###################################
df = df_final.withColumn('snapshot_dt', F.current_timestamp())
data_count = df.count()
print (f"data_count : {data_count}")
df.write.format('parquet').mode('overwrite').save(target_write_path)

print('##############TASK-4-DATA-TRANSFORMATION-COMPLETED################')

###################################TASK-7-REFRESH-ATHENA#######################################

run_crawler(crawler=crawler, database=database, target_table=target_table, dataset_date=DATASET_DATE)

print('##############TASK-7-REFRESH-ATHENA-COMPLETED################')

print('##############JOB-COMPLETED-SUCCESSFULLY################')
job.commit()
