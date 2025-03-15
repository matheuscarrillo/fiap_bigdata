import os
import boto3
import pandas as pd
import numpy as np
from selenium import webdriver
from selenium.webdriver.common.by import By
import time
import re
from selenium.webdriver.chrome.options import Options
from io import BytesIO

import pandas as pd
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager
import os


# Diretório temporário no ECS
download_dir = "/tmp"

# Configurar opções do Chrome para ambiente ECS
print("Iniciando execução do driver...")  # Log de depuração
options = webdriver.ChromeOptions()
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
options.add_argument("--headless")
options.add_argument("--start-maximized")
options.add_experimental_option(
    "prefs",
    {
        "download.default_directory": download_dir,
        "excludeSwitches": ["enable-automation"],
        "useAutomationExtension": False,
    },
)
options.add_argument("--disable-blink-features")
options.add_argument("--disable-blink-features=AutomationControlled")
print("Configurando o ChromeDriver...") 
driver= webdriver.Chrome(
    service=Service(ChromeDriverManager().install()), options=options
)

try:
    # Acessar a página
    print("Acessando a página...") 
    driver.get("https://sistemaswebb3-listados.b3.com.br/indexPage/day/IBOV?language=pt-br")

    # Esperar o carregamento da página
    driver.implicitly_wait(2)

    # Baixar o arquivo
    print("Baixando  o arquivo...") 
    driver.find_element(By.XPATH, '//*[@id="divContainerIframeB3"]/div/div[1]/form/div[2]/div/div[2]/div/div/div[1]/div[2]/p/a').click()
    
    time.sleep(3)  # Esperar download
except Exception as e:
        print(f"Erro no Selenium: {e}")
        raise
finally:
    print("Finalizou sucesso")
    driver.quit()  # Fechar o navegador

s3 = boto3.client(
    "s3",
    aws_access_key_id= os.getenv('MY_SECRET_KEY'), 
    aws_secret_access_key= os.getenv('MY_SECRET_ACCESS_KEY'),
    region_name="us-east-1"
)

bucket_name_inbound = "inbound-209112358514"
bucket_name_raw = "raw-209112358514"

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
            
            df.columns = ['codigo', 'acao', 'tipo', 'qtd_teorica', 'part']
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

# return {"status": "Processo concluído!"}
