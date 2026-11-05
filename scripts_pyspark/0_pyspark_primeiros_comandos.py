# Databricks notebook source
# MAGIC %md
# MAGIC ## Pyspark no Databricks
# MAGIC
# MAGIC Principais Comandos para manipulação de dados usando pyspark

# COMMAND ----------

# MAGIC %md
# MAGIC Carregando os pacotes 

# COMMAND ----------

# Create SparkSession from builder
import pyspark
import plotly.express as px

from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import functions as F
from pyspark.sql import Window

# COMMAND ----------

# DBTITLE 1,Create SparkSession

spark = SparkSession.builder \
                    .appName('tutorial-spark') \
                    .getOrCreate()

# COMMAND ----------

print(spark)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Leitura dos datasets

# COMMAND ----------

# MAGIC %md
# MAGIC ## Items

# COMMAND ----------

df_items = spark.read.option("header", True).csv("/Volumes/workspace/ecommerce/olist/olist_order_items_dataset.csv")
print( "Quantidade de linhas :", df_items.count() )
display(df_items)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Alterando o tipo de cada variável

# COMMAND ----------

# MAGIC %md
# MAGIC Os principais tipos que podemos usar no método `cast` do PySpark são:
# MAGIC
# MAGIC - `string`
# MAGIC - `int` 
# MAGIC - `long`
# MAGIC - `float`
# MAGIC - `double`
# MAGIC - `decimal`
# MAGIC - `boolean`
# MAGIC - `date`
# MAGIC - `timestamp`
# MAGIC - `binary`

# COMMAND ----------

df_items = df_items.withColumn('price', F.col('price').cast('double') )
df_items = df_items.withColumn('freight_value', F.col('freight_value').cast('double') )
df_items = df_items.withColumn('shipping_limit_date', F.col('shipping_limit_date').cast('timestamp') )
df_items.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Adicionar novas colunas 

# COMMAND ----------

# DBTITLE 1,Criando uma nova variavel, razão do frete sobre o preço
df_items = df_items.withColumn("razao_frete_preco", F.round( F.col('freight_value')/F.col('price'), 4 ))
df_items.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Criando nova variavel no pyspark

# COMMAND ----------



df_items = df_items.withColumn("faixa_preco", F.when(F.col("price").isNull(), "nao_informado")\
                                         .when( F.col("price") < 100, "ticket_baixo")\
                                         .when((F.col("price") >= 100) & (F.col("price") < 500), "ticket_medio")\
                                         .when((F.col("price") >= 500) & (F.col("price") < 1000), "ticket_alto")\
                                         .when(F.col("price") >= 1000, 'ticket_muito_alto')\
                                        .otherwise("outros")  )
df_items.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ### renomear o nome das colunas 

# COMMAND ----------

# Renomeando as colunas para português
df_items = df_items.withColumnRenamed("order_id", "id_pedido") \
                   .withColumnRenamed("order_item_id", "id_item_pedido") \
                   .withColumnRenamed("product_id", "id_produto") \
                   .withColumnRenamed("seller_id", "id_vendedor") \
                   .withColumnRenamed("shipping_limit_date", "data_limite_envio") \
                   .withColumnRenamed("price", "preco") \
                   .withColumnRenamed("freight_value", "valor_frete")

df_items.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Análises descritivas 

# COMMAND ----------

descritiva_items = df_items.groupBy("id_pedido")\
        .agg(
        F.countDistinct("id_item_pedido").alias("qtde_itens"),
        F.sum("preco").alias("total_venda"),
        F.mean("preco").alias("preco_medio_itens"),
        F.percentile_approx("preco", 0.5).alias("preco_mediano_itens"),
        F.stddev_pop("preco").alias("desvio_padrao_itens"),
        F.max("preco").alias("preco_max_itens"),
        F.min("preco").alias("preco_min_itens"),
        F.sum("valor_frete").alias("total_frete") ) 

# COMMAND ----------

descritiva_items.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Calculando as metricas de vendas

# COMMAND ----------

descritiva_items.agg( 
                     F.min("qtde_itens" ).alias('qtd_minima_items'), 
                     F.max("qtde_itens" ).alias('qtd_maxima_items'), 
                     F.mean("qtde_itens" ).alias('qtd_media_items'), 
                     F.mean("total_venda" ).alias('total_venda_media'), 
                     F.mean("preco_medio_itens" ).alias('preco_medio_itens_media')  ).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Payments 

# COMMAND ----------

df_payments = spark.read.option("header", True).csv("/Volumes/workspace/ecommerce/olist/olist_order_payments_dataset.csv")
print( "Quantidade de linhas :", df_payments.count() )
display(df_payments)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Order 

# COMMAND ----------

df_order = spark.read.option("header", True).csv("/Volumes/workspace/ecommerce/olist/olist_orders_dataset.csv")
print( "Quantidade de linhas :", df_order.count() )
display(df_order)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Customers 

# COMMAND ----------

df_customer = spark.read.option("header", True).csv("/Volumes/workspace/ecommerce/olist/olist_customers_dataset.csv")
print( "Quantidade de linhas :", df_customer.count() )
display(df_customer)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Adicionando uma nova variavel : Regiao 

# COMMAND ----------


df_customer = df_customer.withColumn("customer_region", F.when(F.col("customer_state").isNull(), "nao_informado")\
                                         .when( F.col("customer_state").isin("AC", "AP", "AM", "PA", "RO", "RR", "TO"), "norte")\
                                         .when( F.col("customer_state").isin("AL", "BA", "CE", "MA", "PB", "PE", "PI", "RN", "SE"), "nordeste")\
                                         .when( F.col("customer_state").isin("DF", "GO", "MT", "MS"), "centro_oeste")\
                                         .when( F.col("customer_state").isin("ES", "MG", "RJ", "SP"), "sudeste")\
                                         .when( F.col("customer_state").isin("PR", "RS", "SC"), "sul")\
                                        .otherwise("outros")  )
df_customer.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Analise descritiva: Quantos clientes por estado e regiao ? 

# COMMAND ----------

df_customer.groupBy( 'customer_state' ).agg( F.countDistinct("customer_id" ).alias('qtde_clientes') ).display()

df_customer.groupBy( 'customer_region' ).agg( F.countDistinct("customer_id" ).alias('qtde_clientes') ).display()




# COMMAND ----------

# MAGIC %md
# MAGIC ## Join entre duas tabelas 
# MAGIC
# MAGIC

# COMMAND ----------

# Join entre customers e orders pela coluna customer_id

df_order_customers = df_order.join(df_customer, on=(df_order["customer_id"] == df_customer["customer_id"]), how="inner")
#nomes das chaves iguais: df_order_customers = df_order.join(df_customer, on="customer_id", how="inner")

print("Quantidade de linhas após o join :", df_order_customers.count())
df_order_customers.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Removendo a coluna duplicada após o join 
# MAGIC

# COMMAND ----------

df_order_customers = df_order_customers.drop(df_customer["customer_id"])

print("Colunas após a remoção:", df_order_customers.columns)
df_order_customers.display()

# COMMAND ----------

# DBTITLE 1,Gráfico de barras: clientes por estado
import plotly.express as px

# transformando para pandas
clientes_por_estado = df_customer.groupBy("customer_state") \
    .agg(F.countDistinct("customer_id").alias("qtde_clientes")) \
    .orderBy(F.desc("qtde_clientes")) \
    .toPandas()

# Criando o grafico de barras
fig = px.bar(
    clientes_por_estado,
    x="customer_state",
    y="qtde_clientes",
    title="Quantidade de Clientes por Estado",
    labels={
        "customer_state": "Estado",
        "qtde_clientes": "Quantidade de Clientes"
    },
    color_discrete_sequence=['darkblue']
)

fig.update_layout(
    xaxis_title="Estado",
    yaxis_title="Quantidade de Clientes",
    title_x=0.5
)
fig.update_yaxes(showticklabels=False)
fig.update_layout( { "plot_bgcolor": "rgba(0, 0, 0, 0)"})
fig.show()