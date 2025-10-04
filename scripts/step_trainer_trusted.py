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

# Script generated for node step_trainer_landing
step_trainer_landing_node1759550434265 = glueContext.create_dynamic_frame.from_catalog(database="d609", table_name="step_trainer_landing", transformation_ctx="step_trainer_landing_node1759550434265")

# Script generated for node customer_curated_data
customer_curated_data_node1759550435652 = glueContext.create_dynamic_frame.from_catalog(database="d609", table_name="customer_curated", transformation_ctx="customer_curated_data_node1759550435652")

# Script generated for node SQL Query
SqlQuery769 = '''
SELECT st.*
FROM d609.step_trainer_landing st
INNER JOIN d609.customer_curated c
ON st.serialNumber = c.serialNumber
'''
SQLQuery_node1759550699204 = sparkSqlQuery(glueContext, query = SqlQuery769, mapping = {"c":customer_curated_data_node1759550435652, "st":step_trainer_landing_node1759550434265}, transformation_ctx = "SQLQuery_node1759550699204")

# Script generated for node step_trainer_trusted_data
EvaluateDataQuality().process_rows(frame=SQLQuery_node1759550699204, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1759548310702", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
step_trainer_trusted_data_node1759550886896 = glueContext.getSink(path="s3://mestri-bucket/Trusted/step_trainer_trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="step_trainer_trusted_data_node1759550886896")
step_trainer_trusted_data_node1759550886896.setCatalogInfo(catalogDatabase="d609",catalogTableName="step_trainer_trusted")
step_trainer_trusted_data_node1759550886896.setFormat("json")
step_trainer_trusted_data_node1759550886896.writeFrame(SQLQuery_node1759550699204)
job.commit()