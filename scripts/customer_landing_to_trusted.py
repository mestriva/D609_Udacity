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

# Script generated for node Amazon S3
AmazonS3_node1759512750054 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://mestri-bucket/landing/customers/"], "recurse": True}, transformation_ctx="AmazonS3_node1759512750054")

# Script generated for node SQL Query
SqlQuery1641 = '''
SELECT * 
    FROM d609.customer_landing
    WHERE shareWithResearchAsOfDate IS NOT NULL;
'''
SQLQuery_node1759512801113 = sparkSqlQuery(glueContext, query = SqlQuery1641, mapping = {"myDataSource":AmazonS3_node1759512750054}, transformation_ctx = "SQLQuery_node1759512801113")

# Script generated for node Trusted_customers
EvaluateDataQuality().process_rows(frame=SQLQuery_node1759512801113, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1759508714161", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
Trusted_customers_node1759512904206 = glueContext.getSink(path="s3://mestri-bucket/Trusted/customer_trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="Trusted_customers_node1759512904206")
Trusted_customers_node1759512904206.setCatalogInfo(catalogDatabase="d609",catalogTableName="customer_trusted")
Trusted_customers_node1759512904206.setFormat("json")
Trusted_customers_node1759512904206.writeFrame(SQLQuery_node1759512801113)
job.commit()