
--Table Creation Using openCSVSerDE
use xmldb;
CREATE EXTERNAL TABLE etl_risk_survey(
YearStart string,
YearEnd string,
LocationAbbr string,
LocationDesc string,
Datasource string,
Class string,
Topic string,
Question string,
Data_Value_Unit string,
Data_Value_Type string,
Data_Value string,
Data_Value_Alt string,
Data_Value_Footnote_Symbol string,
Data_Value_Footnote string,
Low_Confidence_Limit string,
High_Confidence_Limit string,
Sample_Size string,
Total  string,
Age_years  string,
Education  string,
Gender  string,
Income  string,
Race_Ethnicity  string,
GeoLocation  string,
ClassID  string,
TopicID  string,
QuestionID  string,
DataValueTypeID  string,
LocationID  string,
StratificationCategory1  string,
Stratification1  string,
StratificationCategoryId1  string,
StratificationID1  string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   'separatorChar' = ',',
   'quoteChar' = '\"',
   'escapeChar' = '\\'
   )
STORED AS TEXTFILE
LOCATION '/data/hive/'
tblproperties("skip.header.line.count"="1")


====================================================================================================================================
====================================================================================================================================
====================================================================================================================================




















Refere Code :

CREATE EXTERNAL TABLE `mydb`.`mytable`(
    `product_name` string,
    `brand_id` string,
    `brand` string,
    `color` string,
    `description` string,
    `sale_price` string)
PARTITIONED BY (
    `seller_id` string)
ROW FORMAT SERDE
    'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    'separatorChar' = '\t',
    'quoteChar' = '"',
    'escapeChar' = '\\')
STORED AS INPUTFORMAT
    'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
    'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
    'hdfs://namenode.com:port/data/mydb/mytable'
TBLPROPERTIES (
    'serialization.null.format' = '',
    'skip.header.line.count' = '1')
	
