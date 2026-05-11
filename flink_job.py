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


# Janelas de tempos flink
query = """
    SELECT
        window_start,
        window_end,
        `Source IP`,
        COUNT(*) as total_requisicoes,

        -- 1. NOSSA DETECÇÃO (Algoritmo em Tempo Real)
        CASE
            WHEN COUNT(*) > 20 THEN 'SUSPEITO (DDoS)'
            ELSE 'NORMAL'
        END as deteccao_algoritmo,

        -- 2. GABARITO DO DATASET (O que realmente era)
        -- Contamos quantos pacotes na janela tinham labels diferentes de 'BENIGN' (ou semelhantes a DDoS)
        SUM(CASE WHEN `Label` <> 'BENIGN' AND `Label` IS NOT NULL THEN 1 ELSE 0 END) as pacotes_ataque_reais,

        -- 3. VERIFICAÇÃO DE ACURÁCIA
        -- Se o algoritmo alertou E o dataset tinha pacotes maliciosos na janela: Sucesso!
        CASE
            WHEN COUNT(*) > 20 AND SUM(CASE WHEN `Label` <> 'BENIGN' THEN 1 ELSE 0 END) > 0
                THEN 'Verdadeiro Positivo'
            WHEN COUNT(*) > 20 AND SUM(CASE WHEN `Label` <> 'BENIGN' THEN 1 ELSE 0 END) = 0
                THEN 'Falso Positivo (Alerta Falso)'
            WHEN COUNT(*) <= 20 AND SUM(CASE WHEN `Label` <> 'BENIGN' THEN 1 ELSE 0 END) > 0
                THEN ' Falso Negativo (Ataque Não Detectado)'
            ELSE 'Normal Confirmado'
        END as validacao_sistema

    FROM TABLE(
        HOP(TABLE network_source, DESCRIPTOR(ts), INTERVAL '1' SECOND, INTERVAL '3' SECONDS)
    )
    GROUP BY window_start, window_end, `Source IP`
    -- Filtramos para focar em IPs que estão movimentando a rede
    HAVING COUNT(*) > 10
"""

result_table = t_env.sql_query(query)

result_table.execute().print()
