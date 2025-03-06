import os
import boto3
import pandas as pd
import numpy as np
from selenium import webdriver
from selenium.webdriver.common.by import By
import time
import yaml
import re
from selenium.webdriver.chrome.options import Options
from io import BytesIO

config =  yaml.safe_load(open('config.yaml', 'rb'))

download_dir = config.get('path')

# options.add_argument("--headless")  # Executa em modo invisível

# Configurar Chrome para baixar arquivos automaticamente
chrome_options = webdriver.ChromeOptions()
prefs = {
    "download.default_directory": download_dir,  # Define a pasta de destino
    "download.prompt_for_download": False,  # Não perguntar onde salvar
    "download.directory_upgrade": True,
    "safebrowsing.enabled": True  # Evita bloqueio de downloads
}
chrome_options.add_experimental_option("prefs", prefs) 

# options.add_argument("--headless")  # Executa em modo invisível
driver = webdriver.Chrome(options=chrome_options)

driver.get("https://sistemaswebb3-listados.b3.com.br/indexPage/day/IBOV?language=pt-br")

# Espera o carregamento da página (pode ser ajustado)
driver.implicitly_wait(2)

# Encontrar as linhas da tabela
driver.find_element(By.XPATH, '//*[@id="divContainerIframeB3"]/div/div[1]/form/div[2]/div/div[2]/div/div/div[1]/div[2]/p/a').click()

time.sleep(3)

driver.quit()

s3 = boto3.client(
    "s3",
    aws_access_key_id= config.get('aws_access_key_id'),
    aws_secret_access_key= config.get('aws_secret_access_key'),
    region_name="us-east-1"  # Defina a região apropriada
)

bucket_name_inbound = config.get('bucket_name')
bucket_name_raw = config.get('bucket_name_raw')

# Função para extrair a data do nome do arquivo
def extract_date_from_filename(filename):
    match = re.search(r'IBOVDia_(\d{2}-\d{2}-\d{2})\.csv', filename)
    if match:
        return match.group(1).replace("-", "-")  # YY-MM-DD
    return None

# Processar os arquivos baixados
for arquivo in os.listdir(download_dir):

    try:
        caminho_completo = os.path.join(download_dir, arquivo)

        if os.path.isfile(caminho_completo) and arquivo.endswith(".csv"):
            s3.upload_file(caminho_completo, bucket_name_inbound, f"b3/{arquivo}")
            print(f"Upload de {arquivo} concluído!")
            # Extrair a data do nome do arquivo
            file_date = extract_date_from_filename(arquivo)
            if not file_date:
                print(f"Erro ao extrair data do arquivo: {arquivo}")
                break

            # Converter CSV para Parquet
            df = pd.read_csv(
                caminho_completo,
                delimiter=";",
                encoding="ISO-8859-1",
                on_bad_lines="skip",
                skiprows=2,  # Pula a linha do título e cabeçalho
                names=["Código", "Ação", "Tipo", "Qtde. Teórica", "Part. (%)"], 
                dtype={"Código": str, "Ação": str, "Tipo": str, "Qtde. Teórica": str, "Part. (%)": str},
                usecols=[0, 1, 2, 3, 4]  # Considera apenas as colunas esperadas
            )
           
            parquet_filename = arquivo.replace(".csv", ".parquet")
            buffer = BytesIO()
            df.to_parquet(buffer, engine='pyarrow', index=False)

            # Caminho no S3 (particionado pela data)
            parquet_path = f"b3/date={file_date}/{parquet_filename}"
            buffer.seek(0)
            s3.upload_fileobj(buffer, bucket_name_raw, parquet_path)
            print(f"Upload de {parquet_filename} concluído em {parquet_path}")
    except Exception as e:
        print(f"Erro ao processar arquivo {arquivo}: {e}")
        raise