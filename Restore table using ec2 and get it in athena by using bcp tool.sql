>>>>>>>>>>>>

We got our tables in BCP format(binary) in S3
For restoration into parquet and for getting it in athena------

Go to ex2 instance
Sudo su
Su Ubuntu
Pwd---to chk the current directory
Cd ~ ---root path
Cd workdir/Data migration/nano filename.sql
---create a SQL file using nano editor(copy paste code)

Go to SQL server--we have command
 Use msil_mscrm----- to use this particular database
 Select count* from filename.sql--- 0
Now run the SQL server command with file name

Now run restore command using BCP tool with BCP command

Table is restored

Go to glue job
Read file using jdbc
Write file into S3 path --bcp path in S3 with datasetst partition


Run crawler with S3 path as where we want it to be created

Go to Athena run and chk table






process to restore any table
 
1. get BCP data of that table in S3 bucket
path s3://msil-inbound-crm-raw-non-prod/sample/BCP/BCP/<file_name>
 
2. Connect EC2 instance InboundCRM_RDS_msSQL->connect-> session manager->connect
3. sudo su-> su ubuntu
4. cd /home/ubuntu/workdir/DATA
5. copy BCP file in above directory
aws s3 cp <s3 path of BCP table filr path>  <file_name with which we want to save>
example- ubuntu@ip-10-0-0-4:~/workdir/DATA$ aws s3 cp s3://msil-inbound-crm-raw-non-prod/sample/BCP/BCP/pcl_variantBaseBCP variantBaseBCP
 
6. Create DDL /.sql file of same table using original one after removing all references and constraints.
eg refer variantBase.sql or any .sql file
use nano editor or vi for pasting ddl into the file with .sql extension


Go into sql server using below command>>>>

sqlcmd -S inbound-crm-dev-mssql.caphsxy1o5sy.ap-south-1.rds.amazonaws.com -U admininboundcrm -P 'N:v!puo!TclJ#S?-ABMQ$6ZPZG>r'
 
7. Create table using above DDL file.
Example-
sqlcmd -S inbound-crm-dev-mssql.caphsxy1o5sy.ap-south-1.rds.amazonaws.com -U admininboundcrm -P '&7a]&Ie79w:Gg%&>.XTH8UntL[%M' -i variantBase.sql
Get pwd from glue notebook followed by sql file name


 
8. restore data
 
bcp [MSIL_MSCRM].[dbo].[pcl_variantBase] IN variantBaseBCP -S inbound-crm-dev-mssql.caphsxy1o5sy.ap-south-1.rds.amazonaws.com -U admininboundcrm -P '&7a]&Ie79w:Gg%&>.XTH8UntL[%M'

or
bcp [MSIL_MSCRM].[dbo].[pcl_countryBase] IN /home/ubuntu/workdir/MSIL_Inbound_Data_Migration/DataMigration/Data/bcp_20231130/pcl_countryBase -S inbound-crm-dev-mssql.caphsxy1o5sy.ap-south-1.rds.amazonaws.com -U admininboundcrm -P 'N:v!puo!TclJ#S?-ABMQ$6ZPZG>r'
 
--> [dbo].[pcl_variantBase] - change with the database and name provided in DDL while creating
--> variantBaseBCP - replace with BCP file name we copied.
 
9. Connect SQL terminal to check data of the table.
sqlcmd -S inbound-crm-dev-mssql.caphsxy1o5sy.ap-south-1.rds.amazonaws.com -U admininboundcrm -P '&7a]&Ie79w:Gg%&>.XTH8UntL[%M'
 
use MSIL_MSCRM_20230922
go
select * from variantBase
go
 
10. open AWS glue notebook jb_mssqlserver_to_s3- comment code and change with your code
example - df_pcl_variantBase = spark.read.format('jdbc').options(url=url_mssql, user=user, password=password, dbtable="pcl_variantBase").load()
df_pcl_variantBase.printSchema()
df_pcl_variantBase.write.format('parquet').mode('overwrite').save('s3://msil-inbound-crm-raw-non-prod/source/parquet/pcl_variantBase')
 
Save -> Run
 
This will create a parquet file for the binary data.
 
 
11. Once RUN , open Crawler
Make duplicate of any previous crawler which has created any table in athena.
eg crm-inbound-dealer-master-crawler - > action-> create duplicate
Change name of crawler-> Next
Edit Data Source-> paste path of s3 bucket from step 10 -> Next
Select AWS glue- role 2 role -> Next
select inbound as database-> next ->create crawler-> run crawler
 
12. Once Crawler has ran , check athena for table existance







Sl No	DB	TABLE
1	MSIL_MSCRM	pcl_cityBase
2	MSIL_MSCRM	ContactBase
3	MSIL_MSCRM	pcl_stateBase
4	MSIL_MSCRM	SystemUserBase
5	MSIL_MSCRM	BusinessUnitBase
6	MSIL_MSCRM	CampaignBase
7	MSIL_MSCRM	LeadBase
8	MSIL_MSCRM	OpportunityBase
9	MSIL_MSCRM	SLABaseIds
10	MSIL_MSCRM	OwnerBase
11	MSIL_MSCRM	pcl_brandBase
12	MSIL_MSCRM	pcl_calltableBase
13	MSIL_MSCRM	pcl_configsettingBase
14	MSIL_MSCRM	pcl_countryBase
15	MSIL_MSCRM	pcl_dealertypeBase
16	MSIL_MSCRM	pcl_internalcontactBase
17	MSIL_MSCRM	pcl_modelmasterBase
18	MSIL_MSCRM	pcl_msilpricelistBase
19	MSIL_MSCRM	pcl_offerBase
20	MSIL_MSCRM	pcl_regionBase
21	MSIL_MSCRM	TerritoryBase
22	MSIL_MSCRM	pcl_yearBase
23	MSIL_MSCRM	TransactionCurrencyBase
24	MSIL_MSCRM	pcl_vehicledetailbase
25	MSIL_MSCRM	accountbase
26	MSIL_MSCRM	pcl_variantbase
27	MSIL_MSCRM	stringmapbase
28	MSIL_MSCRM	pcl_moscaseBase
29	MSIL_MSCRM	pcl_mcallchartinfoBase
30	MSIL_MSCRM	pcl_moscasecategoryBase
31	MSIL_MSCRM	pcl_problemmasterBase
32	MSIL_MSCRM	pcl_moscasesatusBase
33	MSIL_MSCRM	pcl_moscaselogBase
34	MSIL_MSCRM	pcl_mcalldtclistBase
35	MSIL_MSCRM	pcl_mcalleventtriggerdeailBase
36	MSIL_MSCRM	IncidentBase
37	MSIL_MSCRM	pcl_manualescalationBase
38	MSIL_MSCRM	pcl_smstemplateBase
39	MSIL_MSCRM	pcl_smsBase
40	MSIL_MSCRM	pcl_smslogBase
41	MSIL_MSCRM	pcl_mcallnotificationlistBase
All files except leadbase n contactbase, pls start restoring as per req.