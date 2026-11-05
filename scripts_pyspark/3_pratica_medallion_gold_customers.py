# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Prática: pipeline Medalhão Gold com PySpark e dados da Olist
# MAGIC
# MAGIC Neste notebook vamos criar uma camada **Gold** usando o dataset público de e-commerce da Olist.
# MAGIC
# MAGIC Objetivos: Aplicar a camada Gold aos datasets do Olist. Nessa camada o objetivo é gerar nosso dataset final para uso da área de negócio. por exemplos, datasets resumidos com métricas para alimentar relatórios e dashboards.  

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Importar bibliotecas 
# MAGIC

# COMMAND ----------

from pyspark.sql import functions as F


## definindo diretorio e schema
DATA_DIR = '/Volumes/workspace/ecommerce/olist'
CATALOG = spark.sql('SELECT current_catalog()').first()[0] 
SCHEMA = 'ecommerce' 


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Funções criadas
# MAGIC

# COMMAND ----------

## Funcao para referenciar a tabela 
def table_ref(name):
    return f'`{CATALOG}`.`{SCHEMA}`.`{name}`'

print(f'Catálogo: {CATALOG} | Schema: {SCHEMA}')
print(f'Origem dos CSVs: {DATA_DIR}')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold: tabelas analíticas
# MAGIC
# MAGIC Gold organiza resultados por perguntas de negócio. Os dados de clientes vêm da tabela Silver em granularidade de cliente, com um registro por cliente, preservando estado e cidade sem gerar duplicidades.

# COMMAND ----------

df_cliente = spark.read.table(table_ref('silver_customers'))
df_cliente.display()


# COMMAND ----------

from pyspark.sql import Window

total_clientes = F.sum('qtd_clientes').over(Window.partitionBy())

gold_cliente_estado = (
    df_cliente.groupBy('customer_state')
    .agg(
        F.countDistinct('customer_id').alias('qtd_clientes')
    )
    .withColumn(
        'pct_clientes',
        F.round( F.col('qtd_clientes') / total_clientes, 4)
    )
    )


# COMMAND ----------

## ordenar : 

gold_cliente_estado.sort(F.desc('qtd_clientes')).display()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Criando minha tabela Gold Clientes

# COMMAND ----------

## Definir o nome lógico e o nome da tabela bronze
logical_name = 'customers'
target = f'gold_{logical_name}'

gold_cliente_estado.write.format('delta') \
    .mode('overwrite') \
    .option('overwriteSchema', 'true') \
    .saveAsTable(table_ref(target))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Visualizando a tabela criada 

# COMMAND ----------

df_gold_cliente = spark.read.table(table_ref('gold_customers'))
df_gold_cliente.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exploração e consulta das tabelas Gold
# MAGIC
# MAGIC Use as tabelas para explorar padrões e formular novas perguntas. Os valores representam o recorte deste dataset público e não devem ser interpretados como indicadores atuais do mercado brasileiro.

# COMMAND ----------

spark.sql(f'''
    SELECT *
    FROM {table_ref('gold_customers')}
    LIMIT 10
''').show(truncate=False)



# COMMAND ----------

# MAGIC %md
# MAGIC ## Desafio Gold 
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC 1. Gerar uma visão de negócio para o dataset olist_order_payments e exportar o resultado no formato gold.
# MAGIC
# MAGIC 2. Gerar uma visão de negócio para o dataset olist_order_items e exportar o resultado no formato gold. 
# MAGIC