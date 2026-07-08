import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1783530718185 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="AWSGlueDataCatalog_node1783530718185")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1783530689399 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_trusted", transformation_ctx="AWSGlueDataCatalog_node1783530689399")

# Script generated for node SQL Query
SqlQuery3334 = '''
select 
  step.sensorReadingTime,
  step.serialNumber,
  step.distanceFromObject,
  acc.timestamp,
  acc.user,
  acc.x,
  acc.z 
from step
inner join acc
on step.sensorReadingTime = acc.timestamp

'''
SQLQuery_node1783530737786 = sparkSqlQuery(glueContext, query = SqlQuery3334, mapping = {"acc":AWSGlueDataCatalog_node1783530718185, "step":AWSGlueDataCatalog_node1783530689399}, transformation_ctx = "SQLQuery_node1783530737786")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1783530737786, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1783530622071", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1783530937299 = glueContext.getSink(path="s3://stedi-deepti/machine_learning_curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1783530937299")
AmazonS3_node1783530937299.setCatalogInfo(catalogDatabase="stedi",catalogTableName="machine_learning_curated")
AmazonS3_node1783530937299.setFormat("glueparquet", compression="snappy")
AmazonS3_node1783530937299.writeFrame(SQLQuery_node1783530737786)
job.commit()