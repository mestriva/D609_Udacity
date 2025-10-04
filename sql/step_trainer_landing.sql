-- Step Trainer Landing Table
CREATE EXTERNAL TABLE `step_trainer_landing` (
    `sensorreadingtime` BIGINT COMMENT 'from deserializer',
    `serialnumber` STRING COMMENT 'from deserializer',
    `distancefromobject` DOUBLE COMMENT 'from deserializer'
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://mestri-bucket/landing/step_trainer'
TBLPROPERTIES (
    'classification' = 'json',
    'transient_lastDdlTime' = '1759463907'
);