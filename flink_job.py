import os

from dotenv import load_dotenv
from pyflink.table import EnvironmentSettings, StreamTableEnvironment

load_dotenv()

bootstrap_server = os.getenv("CONFLUENT_BOOTSTRAP_SERVER")
api_key = os.getenv("CONFLUENT_API_KEY")
api_secret = os.getenv("CONFLUENT_API_SECRET")

env_settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
t_env = StreamTableEnvironment.create(environment_settings=env_settings)

t_env.get_config().set(
    "pipeline.jars",
    "file:///opt/flink/usrlib/flink-sql-connector-kafka-3.0.1-1.18.jar",
)

# Definiçaõ de colunas


source_ddl = f"""
    CREATE TABLE network_source (
        `Source IP` STRING,
        `Destination IP` STRING,
        `Destination Port` INT,
        `Protocol` INT,
        `Flow Duration` BIGINT,
        `Label` STRING,
        `ts` AS PROCTIME()
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'network_traffic',
        'properties.bootstrap.servers' = '{bootstrap_server}',
        'properties.group.id' = 'flink-analysis-group',
        'properties.security.protocol' = 'SASL_SSL',
        'properties.sasl.mechanism' = 'PLAIN',
        -- Ajuste crucial: Adicionado o prefixo do pacote shaded que o conector do Flink usa internamente
        'properties.sasl.jaas.config' = 'org.apache.flink.kafka.shaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"{api_key}\" password=\"{api_secret}\";',
        'scan.startup.mode' = 'earliest-offset',
        'format' = 'json',
        'json.ignore-parse-errors' = 'true'
    )
"""

t_env.execute_sql(source_ddl)

print("Submetendo Job de análise para cluster flink...")


query = """
    SELECT
        Label,
        COUNT(*) as total_flows,
        AVG(`Flow Duration`) as avg_duration
    FROM network_source
    GROUP BY Label
"""

result_table = t_env.sql_query(query)

result_table.execute().print()
