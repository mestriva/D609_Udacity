-- Customer Landing Table
CREATE EXTERNAL TABLE `customer_landing` (
    `customername` STRING COMMENT 'from deserializer',
    `email` STRING COMMENT 'from deserializer',
    `phone` BIGINT COMMENT 'from deserializer',
    `birthday` STRING COMMENT 'from deserializer',
    `serialnumber` STRING COMMENT 'from deserializer',
    `registrationdate` BIGINT COMMENT 'from deserializer',
    `lastupdatedate` BIGINT COMMENT 'from deserializer',
    `sharewithresearchasofdate` BIGINT COMMENT 'from deserializer',
    `sharewithpublicasofdate` BIGINT COMMENT 'from deserializer',
    `sharewithfriendsasofdate` BIGINT COMMENT 'from deserializer'
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://mestri-bucket/landing/customers'
TBLPROPERTIES (
    'classification' = 'json',
    'transient_lastDdlTime' = '1759511764'
);
