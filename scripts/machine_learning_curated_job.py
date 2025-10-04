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

# Script generated for node step_trainer_trusted data
step_trainer_trusteddata_node1759551362315 = glueContext.create_dynamic_frame.from_catalog(database="d609", table_name="step_trainer_trusted", transformation_ctx="step_trainer_trusteddata_node1759551362315")

# Script generated for node accelerometer_trusted data
accelerometer_trusteddata_node1759551363606 = glueContext.create_dynamic_frame.from_catalog(database="d609", table_name="accelerometer_trusted", transformation_ctx="accelerometer_trusteddata_node1759551363606")

# Script generated for node SQL Query
SqlQuery1697 = '''
SELECT st.sensorReadingTime, st.serialNumber, st.distanceFromObject,
       a.x, a.y, a.z
FROM d609.step_trainer_trusted st
INNER JOIN d609.accelerometer_trusted a
ON st.sensorReadingTime = a.timestamp
'''
SQLQuery_node1759551458861 = sparkSqlQuery(glueContext, query = SqlQuery1697, mapping = {"a":accelerometer_trusteddata_node1759551363606, "st":step_trainer_trusteddata_node1759551362315}, transformation_ctx = "SQLQuery_node1759551458861")

# Script generated for node machine_learning
EvaluateDataQuality().process_rows(frame=SQLQuery_node1759551458861, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1759550962727", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
machine_learning_node1759551744903 = glueContext.getSink(path="s3://mestri-bucket/curated/machine_learning/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="machine_learning_node1759551744903")
machine_learning_node1759551744903.setCatalogInfo(catalogDatabase="d609",catalogTableName="machine_learning_curated")
machine_learning_node1759551744903.setFormat("json")
machine_learning_node1759551744903.writeFrame(SQLQuery_node1759551458861)
job.commit()