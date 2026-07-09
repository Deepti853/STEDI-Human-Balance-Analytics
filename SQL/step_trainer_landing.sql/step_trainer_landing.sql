CREATE EXTERNAL TABLE `step_trainer_landing`(
  sensorReadingTime BIGINT,
  serialNumber STRING,
  distanceFromObject INT
)
ROW FORMAT SERDE 
  'org.openx.data.jsonserde.JsonSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://stedi-deepti/step_trainer_landing/'
TBLPROPERTIES (
  'classification'='json')
