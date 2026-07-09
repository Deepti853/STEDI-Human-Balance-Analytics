CREATE EXTERNAL TABLE `customer_landing`(
  serialNumber STRING,
  shareWithPublicAsOfDate BIGINT,
  birthday STRING,
  registrationDate BIGINT,
  shareWithResearchAsOfDate BIGINT,
  customerName STRING,
  email STRING,
  lastUpdateDate BIGINT,
  phone STRING,
  shareWithFriendsAsOfDate BIGINT
)
ROW FORMAT SERDE 
  'org.openx.data.jsonserde.JsonSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://stedi-deepti/customer_landing/'
TBLPROPERTIES (
  'classification'='json')
