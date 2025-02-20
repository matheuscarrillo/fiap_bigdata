import os
import boto3
from selenium import webdriver
from selenium.webdriver.common.by import By
import time
import yaml

config = yaml.safe_load(open('config.yaml', 'rb'))

download_dir = config.get('path')

options.add_argument("--headless")  # Executa em modo invisível

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
    aws_access_key_id=config.get('aws_access_key_id'),
    aws_secret_access_key=config.get('aws_secret_access_key'),
    region_name="us-east-1"  # Defina a região apropriada
)

bucket_name = config.get('bucket_name')

for arquivo in os.listdir(download_dir):
    caminho_completo = os.path.join(download_dir, arquivo)
    # print(caminho_completo)
    if os.path.isfile(caminho_completo):
        s3.upload_file(caminho_completo, bucket_name, f"data/raw/{arquivo}")
        print(f"Upload de {arquivo} concluído!")