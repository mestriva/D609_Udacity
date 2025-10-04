
-- Accelerometer Landing Table
CREATE EXTERNAL TABLE `accelerometer_landing` (
    `user` STRING COMMENT 'from deserializer',
    `timestamp` BIGINT COMMENT 'from deserializer',
    `x` DOUBLE COMMENT 'from deserializer',
    `y` DOUBLE COMMENT 'from deserializer',
    `z` DOUBLE COMMENT 'from deserializer'
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://mestri-bucket/landing/accelerometer'
TBLPROPERTIES (
    'classification' = 'json',
    'transient_lastDdlTime' = '1759553529'
);