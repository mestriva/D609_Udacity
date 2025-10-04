import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

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

# Script generated for node accelerometer_landing
accelerometer_landing_node1759514341044 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://mestri-bucket/landing/accelerometer/"], "recurse": True}, transformation_ctx="accelerometer_landing_node1759514341044")

# Script generated for node customer_trusted
customer_trusted_node1759534846196 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://mestri-bucket/Trusted/customer_trusted/"], "recurse": True}, transformation_ctx="customer_trusted_node1759534846196")

# Script generated for node Join
Join_node1759527406813 = Join.apply(frame1=accelerometer_landing_node1759514341044, frame2=customer_trusted_node1759534846196, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1759527406813")

# Script generated for node drop_customer_columns
drop_customer_columns_node1759537980468 = DropFields.apply(frame=Join_node1759527406813, paths=["registrationdate", "customername", "birthday", "sharewithfriendsasofdate", "sharewithpublicasofdate", "lastupdatedate", "email", "serialnumber", "phone", "sharewithresearchasofdate"], transformation_ctx="drop_customer_columns_node1759537980468")

# Script generated for node accelerometer_trusted
EvaluateDataQuality().process_rows(frame=drop_customer_columns_node1759537980468, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1759520320439", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
accelerometer_trusted_node1759527573696 = glueContext.getSink(path="s3://mestri-bucket/Trusted/accelerometer_trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="accelerometer_trusted_node1759527573696")
accelerometer_trusted_node1759527573696.setCatalogInfo(catalogDatabase="d609",catalogTableName="accelerometer_trusted")
accelerometer_trusted_node1759527573696.setFormat("json")
accelerometer_trusted_node1759527573696.writeFrame(drop_customer_columns_node1759537980468)
job.commit()