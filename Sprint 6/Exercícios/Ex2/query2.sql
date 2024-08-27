-- Creating csv table
CREATE EXTERNAL TABLE IF NOT EXISTS meubanco.nomes_csv (
    nome STRING,
    sexo STRING,
    total INT,
    ano INT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
    'serialization.format' = ',',
    'field.delim' = ','
)
LOCATION 's3://gustavcampos-pb-6-exercicios/data';
