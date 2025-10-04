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

# Script generated for node customer_trusted
customer_trusted_node1759548383723 = glueContext.create_dynamic_frame.from_catalog(database="d609", table_name="customer_trusted", transformation_ctx="customer_trusted_node1759548383723")

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1759548392679 = glueContext.create_dynamic_frame.from_catalog(database="d609", table_name="accelerometer_trusted", transformation_ctx="accelerometer_trusted_node1759548392679")

# Script generated for node customer_curated
SqlQuery1722 = '''
SELECT DISTINCT c.*
FROM d609.customer_trusted c
INNER JOIN d609.accelerometer_trusted a
ON c.email = a.user
'''
customer_curated_node1759548500543 = sparkSqlQuery(glueContext, query = SqlQuery1722, mapping = {"a":accelerometer_trusted_node1759548392679, "c":customer_trusted_node1759548383723}, transformation_ctx = "customer_curated_node1759548500543")

# Script generated for node customer_curated
EvaluateDataQuality().process_rows(frame=customer_curated_node1759548500543, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1759548310702", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
customer_curated_node1759548666845 = glueContext.getSink(path="s3://mestri-bucket/curated/customer_curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="customer_curated_node1759548666845")
customer_curated_node1759548666845.setCatalogInfo(catalogDatabase="d609",catalogTableName="customer_curated")
customer_curated_node1759548666845.setFormat("json")
customer_curated_node1759548666845.writeFrame(customer_curated_node1759548500543)
job.commit()