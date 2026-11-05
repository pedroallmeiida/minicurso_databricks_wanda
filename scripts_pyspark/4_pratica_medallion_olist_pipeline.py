# Databricks notebook source
# MAGIC %md
# MAGIC # Prática: pipeline Medallion com PySpark e dados da Olist
# MAGIC
# MAGIC Neste notebook vamos percorrer as camadas **Bronze → Silver → Gold** usando o dataset público de e-commerce da Olist. As células foram pensadas para execução em um workspace Databricks com Spark e Delta Lake.
# MAGIC
# MAGIC **Objetivos:** ingerir CSVs, preservar uma camada bruta, tipar e integrar dados com PySpark, criar tabelas Delta e produzir tabelas analíticas sem multiplicar valores em joins.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Preparação do ambiente e dos dados
# MAGIC
# MAGIC 1. Obtenha o **Brazilian E-Commerce Public Dataset by Olist** no [Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce).
# MAGIC 2. Extraia os CSVs e carregue os nove arquivos listados abaixo em um diretório `olist` de um **Unity Catalog Volume**.
# MAGIC 3. Atualize `DATA_DIR` na próxima célula com o caminho real do Volume. O exemplo `/Volumes/<catalogo>/<schema>/<volume>/olist` é um marcador, não um caminho pronto.
# MAGIC
# MAGIC O dataset contém dados de pedidos, itens, pagamentos, avaliações, clientes, vendedores, produtos, geolocalização e tradução de categorias. A geolocalização será preservada na Bronze; não será necessária para as análises deste exercício.

# COMMAND ----------

from pyspark.sql import functions as F


# COMMAND ----------

## definindo diretorio e schema
DATA_DIR = '/Volumes/workspace/ecommerce/olist'
CATALOG = spark.sql('SELECT current_catalog()').first()[0]
SCHEMA = 'ecommerce'

# COMMAND ----------

spark.sql(f'CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`')


# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------


def table_ref(name):
    return f'`{CATALOG}`.`{SCHEMA}`.`{name}`'

print(f'Catálogo: {CATALOG} | Schema: {SCHEMA}')
print(f'Origem dos CSVs: {DATA_DIR}')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Conferência dos arquivos
# MAGIC
# MAGIC Antes de iniciar, verificamos se os nove CSVs esperados estão no Volume. A ingestão da Bronze mantém os valores como texto para evitar inferências de tipo prematuras.

# COMMAND ----------

files = {
    'customers': 'olist_customers_dataset.csv',
    'geolocation': 'olist_geolocation_dataset.csv',
    'order_items': 'olist_order_items_dataset.csv',
    'order_payments': 'olist_order_payments_dataset.csv',
    'order_reviews': 'olist_order_reviews_dataset.csv',
    'orders': 'olist_orders_dataset.csv',
    'products': 'olist_products_dataset.csv',
    'sellers': 'olist_sellers_dataset.csv',
    'category_translation': 'product_category_name_translation.csv',
}

available_files = {entry.name.rstrip('/') for entry in dbutils.fs.ls(DATA_DIR)}
missing_files = sorted(set(files.values()) - available_files)
if missing_files:
    raise FileNotFoundError(
        f'Arquivos ausentes em {DATA_DIR}: {missing_files}. '
        'Confira o upload e ajuste DATA_DIR.'
    )
print(f'{len(files)} arquivos encontrados.')




# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Bronze: ingestão dos dados de origem
# MAGIC
# MAGIC Bronze preserva uma cópia rastreável da origem. Incluímos o caminho do arquivo e o instante de ingestão como metadados. O modo `overwrite` torna a execução deste material repetível; em um pipeline de produção, a estratégia de carga deve considerar histórico, incrementalidade e idempotência.

# COMMAND ----------

bronze_counts = {}
for logical_name, filename in files.items():
    path = f"{DATA_DIR.rstrip('/')}/{filename}"
    raw_df = (
        spark.read.option('header', 'true')
        .option('inferSchema', 'false')
        .option('mode', 'PERMISSIVE')
        .csv(path)
        .withColumn('_source_file', F.col('_metadata.file_path'))
        .withColumn('_ingested_at', F.current_timestamp())
    )
    target = f'bronze_{logical_name}'
    (raw_df.write.format('delta').mode('overwrite')
     .option('overwriteSchema', 'true').saveAsTable(table_ref(target)))
    bronze_counts[target] = spark.read.table(table_ref(target)).count()

for name, row_count in bronze_counts.items():
    print(f'{name}: {row_count:,} registros')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Inspecionando a Bronze
# MAGIC
# MAGIC Observe o esquema textual e os metadados de origem. `display` mostra uma amostra interativa sem trazer a tabela inteira para a memória local do driver.

# COMMAND ----------

orders_bronze = spark.read.table(table_ref('bronze_orders'))
orders_bronze.printSchema()
display(orders_bronze.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Silver: tipagem, limpeza e integração
# MAGIC
# MAGIC Silver contém dados preparados para reuso. Nesta etapa vamos remover registros sem chaves essenciais, deduplicar entidades, converter tipos e traduzir categorias. Também agregaremos pagamentos por pedido antes de qualquer junção em nível de item, evitando multiplicar valores de receita.

# COMMAND ----------

orders = (
    spark.read.table(table_ref('bronze_orders'))
    .dropDuplicates(['order_id'])
    .filter(F.col('order_id').isNotNull() & F.col('customer_id').isNotNull())
    .withColumn('order_purchase_timestamp', F.to_timestamp('order_purchase_timestamp'))
    .withColumn('order_approved_at', F.to_timestamp('order_approved_at'))
    .withColumn('order_delivered_customer_date', F.to_timestamp('order_delivered_customer_date'))
    .withColumn('order_purchase_date', F.to_date('order_purchase_timestamp'))
    .withColumn('year_month', F.date_format('order_purchase_timestamp', 'yyyy-MM'))
)
customers = (
    spark.read.table(table_ref('bronze_customers'))
    .dropDuplicates(['customer_id'])
    .filter(F.col('customer_id').isNotNull())
)
products = (
    spark.read.table(table_ref('bronze_products'))
    .dropDuplicates(['product_id'])
    .filter(F.col('product_id').isNotNull())
    .withColumn('product_weight_g', F.col('product_weight_g').cast('double'))
)
sellers = (
    spark.read.table(table_ref('bronze_sellers'))
    .dropDuplicates(['seller_id'])
    .filter(F.col('seller_id').isNotNull())
)

items = (
    spark.read.table(table_ref('bronze_order_items'))
    .dropDuplicates(['order_id', 'order_item_id'])
    .filter(
        F.col('order_id').isNotNull()
        & F.col('order_item_id').isNotNull()
        & F.col('product_id').isNotNull()
    )
    .withColumn('price', F.col('price').cast('decimal(12,2)'))
    .withColumn('freight_value', F.col('freight_value').cast('decimal(12,2)'))
    .withColumn('shipping_limit_date', F.to_timestamp('shipping_limit_date'))
)

translations = (
    spark.read.table(table_ref('bronze_category_translation'))
    .dropDuplicates(['product_category_name'])
)
products_translated = products.join(translations, 'product_category_name', 'left')

## funcao para salvar tabelas
def write_delta(df, name):
    (
    df.write
     .format('delta')
     .mode('overwrite')
     .option('overwriteSchema', 'true')
     .saveAsTable(table_ref(name))
     )
    

write_delta(orders, 'silver_orders')
write_delta(customers, 'silver_customers')
write_delta(items, 'silver_order_items')
write_delta(products_translated, 'silver_products')
write_delta(sellers, 'silver_sellers')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver: pagamentos e avaliações em nível de pedido
# MAGIC
# MAGIC Um pedido pode ter várias parcelas/pagamentos e mais de um registro de avaliação. Mantemos essas entidades em tabelas próprias e agregamos os pagamentos por `order_id`. Não as juntamos diretamente às linhas de itens, pois essa relação um-para-muitos pode duplicar a receita.

# COMMAND ----------

payments = (
    spark.read.table(table_ref('bronze_order_payments'))
    .filter(F.col('order_id').isNotNull())
    .withColumn('payment_value', F.col('payment_value').cast('decimal(12,2)'))
    .groupBy('order_id')
    .agg(
        F.sum('payment_value').alias('payment_total'),
        F.count('*').alias('payment_records'),
        F.countDistinct('payment_type').alias('payment_types'),
    )
)

reviews = (
    spark.read.table(table_ref('bronze_order_reviews'))
    .filter(F.col('order_id').isNotNull())
    .filter(F.col('review_score').rlike('^[1-5]$'))
    .withColumn('review_score', F.col('review_score').cast('int'))
    .filter(F.col('review_score').between(1, 5))
)

write_delta(payments, 'silver_order_payments')
write_delta(reviews, 'silver_order_reviews')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver: conjunto enriquecido de itens
# MAGIC
# MAGIC A tabela abaixo tem granularidade de **item do pedido**. Cada linha representa um item; por isso preço e frete somados nessa tabela representam valores em nível de item. Pagamentos e avaliações permanecem fora deste join.

# COMMAND ----------

silver_items_enriched = (
    items.join(orders, 'order_id', 'inner')
    .join(customers.select('customer_id', 'customer_unique_id', 'customer_state'), 'customer_id', 'left')
    .join(
        products_translated.select('product_id', 'product_category_name', 'product_category_name_english'),
        'product_id', 'left',
    )
    .join(sellers.select('seller_id', 'seller_state'), 'seller_id', 'left')
    .select(
        'order_id', 'order_item_id', 'customer_id', 'customer_unique_id',
        'customer_state', 'order_status', 'order_purchase_timestamp',
        'order_purchase_date', 'year_month', 'order_delivered_customer_date',
        'product_id', 'product_category_name', 'product_category_name_english',
        'seller_id', 'seller_state', 'price', 'freight_value',
    )
)
write_delta(silver_items_enriched, 'silver_items_enriched')
display(silver_items_enriched.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verificações iniciais de qualidade
# MAGIC
# MAGIC As regras são exemplos didáticos: em um projeto real, elas devem ser acordadas com quem conhece o domínio. A avaliação fica distribuída no Spark; não trazemos os dados completos ao driver.

# COMMAND ----------

def key_quality(df, keys):
    keys = [keys] if isinstance(keys, str) else keys
    null_condition = F.col(keys[0]).isNull()
    for key in keys[1:]:
        null_condition = null_condition | F.col(key).isNull()
    null_keys = df.filter(null_condition).count()
    duplicate_keys = df.groupBy(*keys).count().filter(F.col('count') > 1).count()
    return {
        'key': '+'.join(keys),
        'null_rows': null_keys,
        'duplicate_key_groups': duplicate_keys,
    }

quality_results = [
    key_quality(spark.read.table(table_ref('silver_orders')), 'order_id'),
    key_quality(spark.read.table(table_ref('silver_customers')), 'customer_id'),
    key_quality(
        spark.read.table(table_ref('silver_order_items')),
        ['order_id', 'order_item_id'],
    ),
]
display(spark.createDataFrame(quality_results))



# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Gold: tabelas analíticas
# MAGIC
# MAGIC Gold organiza resultados por perguntas de negócio. Receita de itens vem da tabela Silver em granularidade de item. Pagamentos e avaliações são analisados em suas próprias granularidades para evitar duplicidades.

# COMMAND ----------

delivered_items = spark.read.table(table_ref('silver_items_enriched')).filter(
    F.col('order_status') == 'delivered'
)

gold_revenue_by_state = (
    delivered_items.groupBy('customer_state')
    .agg(
        F.countDistinct('order_id').alias('delivered_orders'),
        F.round(F.sum('price'), 2).alias('item_revenue'),
        F.round(F.sum('freight_value'), 2).alias('freight_revenue'),
    )
    .orderBy(F.desc('item_revenue'))
)
write_delta(gold_revenue_by_state, 'gold_revenue_by_state')
display(gold_revenue_by_state)

# COMMAND ----------

gold_monthly_category_revenue = (
    delivered_items.groupBy('year_month', 'product_category_name_english')
    .agg(
        F.round(F.sum('price'), 2).alias('item_revenue'),
        F.countDistinct('order_id').alias('orders'),
        F.count('*').alias('items_sold'),
    )
    .orderBy('year_month', F.desc('item_revenue'))
)
write_delta(gold_monthly_category_revenue, 'gold_monthly_category_revenue')
display(gold_monthly_category_revenue.limit(20))

# COMMAND ----------

gold_top_sellers = (
    delivered_items.groupBy('seller_id', 'seller_state')
    .agg(
        F.round(F.sum('price'), 2).alias('item_revenue'),
        F.countDistinct('order_id').alias('delivered_orders'),
        F.count('*').alias('items_sold'),
    )
    .orderBy(F.desc('item_revenue'))
)
write_delta(gold_top_sellers, 'gold_top_sellers')
display(gold_top_sellers.limit(10))

# COMMAND ----------

# Cada avaliação é relacionada ao pedido e cliente, sem join com linhas de itens.
gold_reviews_by_state = (
    spark.read.table(table_ref('silver_order_reviews')).alias('r')
    .join(
        spark.read.table(table_ref('silver_orders')).select('order_id', 'customer_id').alias('o'),
        'order_id', 'inner',
    )
    .join(
        spark.read.table(table_ref('silver_customers')).select('customer_id', 'customer_state'),
        'customer_id', 'left',
    )
    .groupBy('customer_state')
    .agg(
        F.round(F.avg('review_score'), 2).alias('average_review_score'),
        F.count('*').alias('review_records'),
    )
    .orderBy(F.desc('average_review_score'))
)
write_delta(gold_reviews_by_state, 'gold_reviews_by_state')
display(gold_reviews_by_state)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Exploração e consulta das tabelas Gold
# MAGIC
# MAGIC Use as tabelas para explorar padrões e formular novas perguntas. Os valores representam o recorte deste dataset público e não devem ser interpretados como indicadores atuais do mercado brasileiro.

# COMMAND ----------

spark.sql(f'''
    SELECT customer_state, delivered_orders, item_revenue, freight_revenue
    FROM {table_ref('gold_revenue_by_state')}
    ORDER BY item_revenue DESC
    LIMIT 10
''').show(truncate=False)

display(spark.read.table(table_ref('gold_monthly_category_revenue')))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Exercicios
# MAGIC
# MAGIC 1. Compare receita de itens e valores agregados de pagamentos por pedido, documentando diferenças e hipóteses.
# MAGIC 2. Calcule o tempo entre compra e entrega por estado e discuta como tratar pedidos ainda não entregues.
# MAGIC
# MAGIC