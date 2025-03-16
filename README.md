# Pipeline Batch Bovespa

# Requisitos

Certifique-se de que os seguintes pré-requisitos estão instalados:

1. **Python 3.12**
2. **Google Chrome** e **ChromeDriver** compatíveis com sua versão do navegador.
3. Bibliotecas Python:
   - `requirements.txt`

## **Dependências**
O script requer as seguintes bibliotecas Python:

- `selenium`
- `pandas`
- `numpy`
- `boto3`
- `webdriver-manager`
- `pyarrow`

Instale todas as dependências executando:

```sh
pip install selenium pandas numpy boto3 webdriver-manager pyarrow
```

## Arquitetura do Projeto
![Arquitetura](https://github.com/user-attachments/assets/1dfa3402-f5f0-4f1b-8478-29ade95331e4)


# Extração de Dados da B3

## Descrição
Este script **scrap_b3.py** realiza a extração de uma tabela do site da **B3** utilizando **Selenium** (Web Scraping). Em seguida, processa os dados e os salva na **AWS S3** em dois formatos:
- **CSV** (arquivo bruto extraído do site)
- **Parquet** (com algumas transformações e particionado por data)

Existe um **EventBridge** agendado para executar, diariamente às 12h, uma tarefa **ECS Fargate** responsável por coletar dados do site da **B3**. Essa tarefa utiliza uma imagem Docker armazenada no Amazon ECR, garantindo escalabilidade e execução sem necessidade de gerenciamento de servidores.

## **Execução do Script**
### **1. Inicialização do Web Scraping**
O script inicia configurando o **ChromeDriver** para rodar no modo **headless** (sem interface gráfica) e desativando algumas funcionalidades para melhorar a compatibilidade com execução em ambientes na nuvem.

### **2. Upload do CSV para a AWS S3**
Após o download, o script realiza o upload do arquivo CSV para um **bucket S3** definido na variável `bucket_name_inbound`.

### **3. Processamento dos Dados e Conversão para Parquet**
- Lê o arquivo CSV e formata as colunas esperadas.
- Renomeia as colunas para um formato padronizado.
- Converte o DataFrame para **Parquet**.

### **4. Upload do Parquet Particionado para a AWS S3**
Os dados processados são armazenados no bucket **raw**, particionados pela data extraída do nome do arquivo.

### **Resultados**
Após a execução, os arquivos estarão disponíveis no Amazon S3:

CSV bruto: ```s3://inbound-209112358514/b3/IBOVDia_YY-MM-DD.csv```
Parquet processado: ```s3://raw-209112358514/b3/date=YY-MM-DD/IBOVDia_YY-MM-DD.parquet```


# Processamento e Democratização dos Dados

## Processamento dos Dados

Este projeto utiliza o AWS Glue para processar e transformar dados provenientes de arquivos armazenados em um bucket do Amazon S3. A seguir, detalhamos o processo realizado pelo script de transformação de dados. Esse processo é acionado por uma função Lambda que executa a transformação sempre que um novo arquivo é gerado no S3.

### Descrição Geral

Este script realiza a leitura de dados brutos em formato Parquet de um bucket S3, aplica uma série de transformações, agrupa as informações, calcula comparativos com o dia anterior e, em seguida, grava os dados processados de volta no S3.

### Fluxo do Processo

1. **Leitura de Dados**
   - O script começa lendo dados do S3, especificamente do bucket `raw-209112358514` e pasta `b3/`. Ele lê arquivos no formato Parquet.

2. **Renomeando de Colunas**
   - As colunas do DataFrame são renomeadas para tornar os nomes mais amigáveis e fáceis de trabalhar.

3. **Tratamento de Dados**
   - Alguns campos são tratados:
     - O campo `Qtde. Teórica` é limpo removendo pontos e convertido para tipo `bigint`.
     - O campo `Part. (%)` é transformado, trocando vírgulas por pontos, e convertido para o tipo `double`.
     - O campo de data é convertido para o formato adequado para comparações.

4. **Agrupamento de Dados**
   - Realiza-se um agrupamento dos dados por papel, calculando as seguintes métricas:
     - Contagem de ocorrências (`qtd_dias`).
     - Soma das quantidades teóricas (`soma_qtd_teorica`).
     - Máximo (`max_qtd_teorica`) e Mínimo (`min_qtd_teorica`) das quantidades teóricas.

5. **Cálculos de Janela**
   - A transformação utiliza uma janela de dados para calcular valores anteriores para cada papel:
     - Calcula o valor anterior do campo `part_pct` usando a função `lag()`.
     - Calcula a data anterior para cada papel.
     - A diferença do valor de `part_pct` entre as datas mais recentes e anteriores é calculada.
     - A diferença em dias entre a data atual e a anterior também é computada.

6. **Join com Dados Agrupados**
   - A base transformada é cruzada com os dados agrupados para adicionar as métricas calculadas aos registros.

7. **Filtragem de Dados**
   - Registros com o valor 'Quantidade Teórica Total' no campo `papel` são filtrados para evitar dados irrelevantes.

8. **Gravação de Dados Processados**
   - Após as transformações, os dados são gravados de volta no S3, particionados por `date` e `papel` no caminho especificado dentro do bucket `refined-209112358514`.

## Democratização dos Dados

### Descrição Geral

O script realiza a leitura de dados armazenados em formato Parquet no S3, verifica a existência do banco de dados e da tabela no AWS Glue Catalog, cria ou atualiza a tabela, e em seguida executa uma reparação de partições no Athena. Esse processo garante que os dados processados anteriormente fiquem acessíveis para consultas SQL no Athena.

### Fluxo do Processo

1. **Leitura de Dados**
   - O script lê os dados Parquet armazenados no caminho especificado dentro do bucket S3, onde os dados processados anteriormente são armazenados.

2. **Configuração do Logger**
   - Um logger é configurado para capturar e registrar informações sobre o processo, como o schema dos dados, o número de registros lidos e qualquer outro evento importante.

3. **Contagem de Registros**
   - O número de registros no DataFrame é contado e registrado nos logs.

4. **Verificação e Criação de Banco de Dados no Glue Catalog**
   - O script verifica se o banco de dados já existe no Glue Catalog. Caso contrário, ele cria um novo banco de dados para armazenar a tabela.

5. **Criação ou Atualização de Tabela**
   - O script define a tabela, especificando o esquema (tipos de dados e colunas) e o local dos dados no S3.
   - Caso a tabela já exista, ela é atualizada; se não existir, ela é criada no Glue Catalog.

6. **Reparo de Partições no Athena**
   - Após a criação ou atualização da tabela, o script executa o comando `MSCK REPAIR TABLE` no Athena para descobrir novas partições baseadas nas colunas de particionamento (`date` e `papel`), garantindo que os dados sejam devidamente registrados para consulta.

7. **Tabela Disponível no Athena**
   - Após a execução do reparo, a tabela está pronta para ser consultada diretamente no Athena.

## Requisitos

- AWS Glue para processamento de dados.
- AWS Lambda para acionar o Glue.
- Permissões adequadas de leitura e gravação para os buckets S3 envolvidos.
- O ambiente deve ser configurado com o PySpark e as bibliotecas do AWS Glue para execução do script.

## Fluxo de Execução

- O arquivo é carregado no S3 e uma função Lambda é acionada automaticamente.
- O Glue é executado com o script de transformação, realizando o processamento descrito.
- Os resultados são armazenados no S3 no formato Parquet, com particionamento por data e papel.

## Detalhes de Implementação

A função Lambda invoca o AWS Glue assim que o arquivo é gerado no S3, iniciando o processo de transformação descrito no script. O código é estruturado para realizar as seguintes etapas de processamento:

1. Leitura dos dados.
2. Tratamento de colunas.
3. Agrupamento e cálculos de métricas.
4. Cálculos de janela para valores anteriores.
5. Cruzamento de dados e filtragem.
6. Gravação dos dados no S3 com particionamento.

O caminho de saída para os dados no S3 é configurado como `s3://refined-209112358514/b3`, onde os dados são armazenados em formato Parquet.

# Estrutura do Repositório

```plaintext
.
├── scrap_b3.py
├── DockerFile
├── requirements.txt
└── aws/
    ├── glue
       ├── democratiza_tabela_refined.py
       ├── processing.py
       ├── main.py
    ├── lambda
       └── lambda_call_glue.py
    ├── eventbridge
       └── schedule_ecs_fargate.json

```
