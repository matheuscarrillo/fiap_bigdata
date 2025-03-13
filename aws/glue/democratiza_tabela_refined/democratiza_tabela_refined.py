import sys
import logging
import boto3
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.functions import current_date, date_format


# configura o Logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

# parâmetros de execução do Glue (se houver)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# configura o SparkContext e o GlueContext
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# inicializando o job Glue
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# obtenção da série temporal do preço do petróleo bruto
raw_data_path = "s3://fiap-projeto-math-refined/b3/"
df = spark.read.parquet(raw_data_path)

# log do schema do DataFrame
logger.info("Schema do DataFrame:")
df.printSchema()

# log de count e log do número de registros no DataFrame
record_count = df.count()
logger.info(f"Número de registros no DataFrame: {record_count}")

# cliente do boto3 para Glue
glue_client = boto3.client("glue")

database_name = "workspace_db"
table_name = "tb_acoes_b3_refined"
table_location = raw_data_path

# verifica se o banco de dados existe e crie se necessário
try:
    glue_client.get_database(Name=database_name)
except glue_client.exceptions.EntityNotFoundException:
    glue_client.create_database(
        DatabaseInput={'Name': database_name}
    )
    logger.info(f"Banco de dados '{database_name}' criado no Glue Catalog.")


# definição da tabela
table_input = {
    'Name': table_name,
    'StorageDescriptor': {
        'Columns': [
            {"Name": "acao", "Type": "string"},
            {"Name": "tipo", "Type": "string"},
            {"Name": "qtd_teorica", "Type": "bigint"},
            {"Name": "part_pct", "Type": "double"},
            {"Name": "pct_part_anterior", "Type": "double"},
            {"Name": "ult_data", "Type": "date"},
            {"Name": "diferenca_pct", "Type": "double"},
            {"Name": "diferenca_dias", "Type": "int"},
            {"Name": "qtd_dias", "Type": "int"},
            {"Name": "soma_qtd_teorica", "Type": "bigint"},
            {"Name": "max_qtd_teorica", "Type": "bigint"},
            {"Name": "min_qtd_teorica", "Type": "bigint"},
        ],
        'Location': table_location,
        'InputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
        'OutputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
        'Compressed': False,
        'SerdeInfo': {
            'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe',
            'Parameters': {'serialization.format': '1'}
        }
    },
    'PartitionKeys': [{"Name": "date", "Type": "date"}, {"Name": "papel", "Type": "string"}],
    'TableType': 'EXTERNAL_TABLE'
}

# cria ou atualiza a tabela no Glue Catalog
try:
    glue_client.get_table(DatabaseName=database_name, Name=table_name)
    logger.info(f"Tabela '{table_name}' já existe no Glue Catalog. Atualizando tabela...")
    glue_client.update_table(DatabaseName=database_name, TableInput=table_input)
    logger.info(f"Tabela '{table_name}' atualizada no Glue Catalog.")
except glue_client.exceptions.EntityNotFoundException:
    glue_client.create_table(DatabaseName=database_name, TableInput=table_input)
    logger.info(f"Tabela '{table_name}' criada no Glue Catalog.")

logger.info(f"Tabela '{table_name}' disponível no Athena.")

# executa MSCK REPAIR TABLE para descobrir partições
repair_table_query = f"MSCK REPAIR TABLE {database_name}.{table_name}"
logger.info(f"Executando comando: {repair_table_query}")
spark.sql(repair_table_query)
logger.info(f"Comando MSCK REPAIR TABLE executado com sucesso para a tabela '{table_name}'.")

# finaliza o job Glue
job.commit()