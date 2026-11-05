# Databricks notebook source
# MAGIC %md
# MAGIC # Prática: Medalhão Bronze com PySpark e dados da Olist
# MAGIC
# MAGIC Neste notebook vamos criar uma camada **Bronze** usando o dataset público de e-commerce da Olist. 
# MAGIC
# MAGIC **Objetivos:** ingerir dados bruto(csv, parquet, ...) e preservar uma camada bruta dos dados. Nessa camada, buscamos manter os dados brutos originais. 

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Preparação do ambiente e dos dados
# MAGIC
# MAGIC 1. Obtenha o **Brazilian E-Commerce Public Dataset by Olist** no [Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce).
# MAGIC 2. Extraia os CSVs e carregue os nove arquivos listados abaixo em um diretório `olist` de um **Unity Catalog Volume**.
# MAGIC 3. Atualize `DATA_DIR` na próxima célula com o caminho real do Volume. Por exemplo, `/Volumes/<catalogo>/<schema>/<volume>/olist` .
# MAGIC
# MAGIC O dataset contém dados de pedidos, itens, pagamentos, avaliações, clientes, vendedores, produtos, geolocalização e tradução de categorias. 

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Importar bibliotecas 
# MAGIC

# COMMAND ----------

from pyspark.sql import functions as F



# COMMAND ----------

# MAGIC %md
# MAGIC Difinindo diretório, o catálogo e o Schema: 

# COMMAND ----------

## definindo diretorio e schema
DATA_DIR = '/Volumes/workspace/ecommerce/olist'
CATALOG = spark.sql('SELECT current_catalog()').first()[0] 
SCHEMA = 'ecommerce' 


# COMMAND ----------

# MAGIC %md
# MAGIC Criando um Schema caso não exista

# COMMAND ----------

## Se não existe um Schema ainda, podemos criar um com o código abaixo
spark.sql(f'CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`')


# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

## Funcao para referenciar a tabela 
def table_ref(name):
    return f'`{CATALOG}`.`{SCHEMA}`.`{name}`'

print(f'Catálogo: {CATALOG} | Schema: {SCHEMA}')
print(f'Origem dos CSVs: {DATA_DIR}')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze: ingestão dos dados de origem
# MAGIC
# MAGIC Bronze preserva uma cópia rastreável da origem. Incluímos o caminho do arquivo e o instante de ingestão como metadados. O modo `overwrite` torna a execução deste material repetível; em um pipeline de produção, a estratégia de carga deve considerar histórico, incrementalidade e idempotência.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Carregando o csv importado em Volumes

# COMMAND ----------

raw_df = spark.read.option("header", True).csv("/Volumes/workspace/ecommerce/olist/olist_customers_dataset.csv")
display(raw_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Leitura de datasets com spark

# COMMAND ----------

# Definindo o arquivo CSV e seu diretório
filename = 'olist_customers_dataset.csv'
path = f"{DATA_DIR.rstrip('/')}/{filename}"

# Leitura do CSV com metadados de ingestão (ver célula markdown anterior)
raw_df = (
    spark.read.option('header', 'true')
    .option('inferSchema', 'false')
    .csv(path)
    .withColumn('_source_file', F.col('_metadata.file_path'))
    .withColumn('_ingested_at', F.current_timestamp())
)

display(raw_df)

# COMMAND ----------

raw_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Contagem da quantidade de linhas da tabela 

# COMMAND ----------

raw_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Salvando tabela Bronze no formato Delta 

# COMMAND ----------

## Definir o nome lógico e o nome da tabela bronze
logical_name = 'customers'
target = f'bronze_{logical_name}'

## SAlvando a tabela em formato delta
(
raw_df.write.format('delta').mode('overwrite')
.option('overwriteSchema', 'true').saveAsTable( table_ref(target) )
)



# COMMAND ----------

# MAGIC %md
# MAGIC ### Inspecionando a Bronze
# MAGIC
# MAGIC Observe o esquema textual e os metadados de origem. `display` mostra uma amostra interativa sem trazer a tabela inteira para a memória local do driver.

# COMMAND ----------

orders_bronze = spark.read.table(table_ref('bronze_customers'))
orders_bronze.printSchema()
display(orders_bronze.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercicios 
# MAGIC
# MAGIC 1. Criar a Tabela bronze para o arquivo olist_order_items_dataset.csv
# MAGIC 2. Criar a Tabela bronze para o arquivo olist_order_payments_dataset.csv 
# MAGIC 3. Criar a Tabela bronze para o arquivo olist_products_dataset.csv