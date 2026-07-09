CREATE EXTERNAL TABLE `accelerometer_landing`(
  user STRING,
  timestamp BIGINT,
  x DOUBLE,
  y DOUBLE,
  z DOUBLE
)
ROW FORMAT SERDE 
  'org.openx.data.jsonserde.JsonSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://stedi-deepti/accelerometer_landing/'
TBLPROPERTIES (
  'classification'='json')
