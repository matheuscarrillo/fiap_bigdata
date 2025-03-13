import sys
import logging
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

from pyspark.sql.functions import *
from pyspark.sql.window import Window

# configura o Logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

logger.info("lendo tabela inbound")
df = spark.read.parquet("s3://fiap-projeto-math-inbound/b3/")
df.show(1)

logger.info("renomeando o nome das colunas")
df_transformed = df.withColumnRenamed("codigo", "papel")
df_transformed = df_transformed.withColumnRenamed("part", "part_pct")

logger.info("tratando campo em texto, transformando para valor numérico")
df_transformed = df_transformed.withColumn("qtd_teorica",regexp_replace(col("qtd_teorica"), "\\.", "").cast("bigint")) # Remove pontos e converte para inteiro


logger.info("criando agrupamento de contagem, soma, máximo e mínimo")
df_grouped = df_transformed.groupBy("papel").agg(
    count("*").alias("qtd_dias"),   # Contagem de ocorrências
    sum("qtd_teorica").alias("soma_qtd_teorica"), # Soma das qtd_teorica
    max("qtd_teorica").alias("max_qtd_teorica"),  # Máximo qtd_teorica
    min("qtd_teorica").alias("min_qtd_teorica")   # Mínimo qtd_teorica
)

logger.info("Convertendo a data para o formato correto")
df_transformed = df_transformed.withColumn("date", to_date(col("date"), "dd-MM-yy"))

logger.info("Convertendo pct_part de string com vírgula para número decimal")
df_transformed = df_transformed.withColumn("part_pct", regexp_replace(col("part_pct"), ",", ".").cast("double"))


logger.info("Criando uma janela para pegar a data anterior para cada papel")
window_spec = Window.partitionBy("papel").orderBy(col("date").asc())

logger.info("Calculando o valor anterior de pct_part usando lag")
df_transformed = df_transformed.withColumn("pct_part_anterior", lag("part_pct").over(window_spec))

logger.info("Calculando o valor anterior de data usando lag")
df_transformed = df_transformed.withColumn("ult_data", lag("date").over(window_spec))

logger.info("Calculando a diferença do campo pct_part entre a data mais recente e a anterior")
df_transformed = df_transformed.withColumn("diferenca_pct", col("part_pct") - col("pct_part_anterior"))

logger.info("Calculando a diferença em dias entre a data mais recente e a anterior")
df_transformed = df_transformed.withColumn("diferenca_dias", datediff(col("date"), col("ult_data")))

logger.info("cruzando a base transformada com a base agrupada")
df_joined = df_transformed.join(df_grouped, on="papel", how="left")

df_joined = df_joined.filter(col("papel") != 'Quantidade Teórica Total')

df_joined.show(3)

logger.info("Definindo o caminho no S3")
bucket_name = "fiap-projeto-math-refined"
output_path = f"s3://{bucket_name}/b3"

logger.info("Salvando os dados particionados por 'date' e 'papel' no S3")
df_joined.write.mode("overwrite").partitionBy("date", "papel").parquet(output_path)

logger.info("Dados salvos com sucesso no S3!")

job.commit()