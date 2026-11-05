# Databricks notebook source
# MAGIC %md
# MAGIC # Prática: Medalhão Silver com PySpark e dados da Olist
# MAGIC
# MAGIC Neste notebook vamos criar uma camada **Silver** usando o dataset público de e-commerce da Olist. 
# MAGIC
# MAGIC **Objetivos:** Aplicar a camada Silver aos datasets do Olist. Nessa camada o objetivo é realizar todo o pré-processamento necessário para garantir uma boa qualidade aos nossos dados. 

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Importar bibliotecas 
# MAGIC

# COMMAND ----------

from pyspark.sql import functions as F



# COMMAND ----------

# MAGIC %md
# MAGIC #### Difinindo diretório, o catálogo e o Schema: 

# COMMAND ----------

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
# MAGIC ## Silver: tipagem, limpeza e integração
# MAGIC
# MAGIC Silver contém dados preparados para reuso. Nesta etapa vamos remover registros sem chaves essenciais, deduplicar entidades, converter tipos e traduzir categorias. Também agregaremos pagamentos por pedido antes de qualquer junção em nível de item, evitando multiplicar valores de receita.

# COMMAND ----------


df_customers = spark.read.table(table_ref('bronze_customers')) \
    .dropDuplicates(['customer_id']) \
    .filter(F.col('customer_id').isNotNull())



# COMMAND ----------

# MAGIC %md
# MAGIC #### Adicionando uma nova variavel : Regiao 

# COMMAND ----------

df_customers = df_customers.withColumn("customer_region", F.when(F.col("customer_state").isNull(), "nao_informado")\
                                         .when( F.col("customer_state").isin("AC", "AP", "AM", "PA", "RO", "RR", "TO"), "norte")\
                                         .when( F.col("customer_state").isin("AL", "BA", "CE", "MA", "PB", "PE", "PI", "RN", "SE"), "nordeste")\
                                         .when( F.col("customer_state").isin("DF", "GO", "MT", "MS"), "centro_oeste")\
                                         .when( F.col("customer_state").isin("ES", "MG", "RJ", "SP"), "sudeste")\
                                         .when( F.col("customer_state").isin("PR", "RS", "SC"), "sul")\
                                        .otherwise("outros")  )
df_customers.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exportar para formato delta
# MAGIC

# COMMAND ----------

## Definir o nome lógico e o nome da tabela bronze
logical_name = 'customers'
target = f'silver_{logical_name}'

df_customers.write.format('delta') \
    .mode('overwrite') \
    .option('overwriteSchema', 'true') \
    .saveAsTable(table_ref(target))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Carregando tabela criada 

# COMMAND ----------

df_silver_customers = spark.read.table(table_ref('silver_customers'))
display(df_silver_customers)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Verificações iniciais de qualidade
# MAGIC
# MAGIC Podemos criar funções que avaliam a qualidade dos dados. Por exemplo, verificar a qualidade das chaves (keys)

# COMMAND ----------

## Qualidade por chave

def key_quality(df, keys):
    keys = [keys] if isinstance(keys, str) else keys
    results = []
    for key in keys:
        null_rows = df.filter(F.col(key).isNull()).count()
        duplicate_key_groups = (
            df.groupBy(key).count().filter(F.col('count') > 1).count()
        )
        results.append({
            'key': key,
            'null_rows': null_rows,
            'duplicate_key_groups': duplicate_key_groups,
        })
    return results


# COMMAND ----------


quality_results = key_quality(
    spark.read.table(table_ref('silver_customers')),
    ['customer_id', 'customer_unique_id'],
)

df_qualidade = spark.createDataFrame(quality_results)
display(df_qualidade)



# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercicios 
# MAGIC
# MAGIC 1. Criar a Tabela silver ajustando e adicionando variáveis de acordo com seu objetivo de análise. Considere o arquivo olist_order_items_dataset.csv
# MAGIC 2. Criar a Tabela silver ajustando e adicionando variáveis de acordo com seu objetivo de análise. Considere o arquivo olist_order_payments_dataset.csv 
# MAGIC 3. Criar a Tabela silver ajustando e adicionando variáveis de acordo com seu objetivo de análise. Considere o arquivo olist_products_dataset.csv