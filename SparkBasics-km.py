#!/usr/bin/env python
# coding: utf-8

# # Spark basics
# 
# This tutorial shows how to work with Spark in the local mode (spark-submit --master local).<br/>
# It uses your machine resources, so it will be significantly slower than using a dataproc cluster (see `SparkCluster.ipynb`).<br/>
# We only recommend using Spark this way if the job is very small and doesn't require a lot of resources.

# ### Make your Spark available in Notebook (required every time after kernel reset)

# In[ ]:


import findspark
findspark.init()


# # Use your Spark

# ### Setup configuration variables
# !IMPORTANT! *temp_bucket_name* bucket has to be created before running the calculations below. It needs to be in **the same** GC location.

# In[ ]:


app_name = 'km-notebook-app'
project_id = 'sc-10067-search-analytics-prod'
temp_bucket_name = 'km-bucket-050701'
bq_table_in = 'sc-9366-nga-prod.search_logs.search_logs'
bq_table_out = 'sc-10067-search-analytics-prod.km_workspace.listing_spark'


# If you have not created the bucket yet, you can use examples from **`/xwing-helpers-repository/tutorials/storage/`** folder or run following code snipper:

# In[ ]:


from google.cloud import storage

client = storage.Client()
print("Client created using default project: {}".format(client.project))

# Replace the string below with location code where the new bucket will be created
location_code = "europe-west1" 
temp_bucket_name = "km-bucket-test"

def create_bucket(bucket_name, location_code):
    """Creates a new bucket."""
    # bucket_name = "your-new-bucket-name"
    # location_code = "gcs region name"

    storage_client = storage.Client()
    
    bucket = storage.Bucket(storage_client, bucket_name)
    bucket.storage_class = "REGIONAL"
    bucket.create(location = location_code)

    print("Bucket {} created".format(bucket.name))
    
create_bucket(temp_bucket_name, location_code)


# ### Create SparkSession
# #### import SparkSession

# In[ ]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import year


# #### configuration and creation of spark session:
# * .config('temporaryGcsBucket', temp_bucket_name) - registration of temp bucket for future big query writes
# * ._jsc.hadoopConfiguration().set('fs.gs.impl', 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem') - setting GCS as default file store for spark
# * ._jsc.hadoopConfiguration().set('fs.gs.project.id', project_id) - setting project_id

# In[ ]:


spark = SparkSession \
  .builder \
  .appName(app_name) \
  .config('spark.datasource.bigquery.temporaryGcsBucket', temp_bucket_name) \
  .master("local") \
  .getOrCreate()

spark._jsc.hadoopConfiguration().set('fs.gs.impl', 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem')
spark._jsc.hadoopConfiguration().set('fs.gs.project.id', project_id)


# ## read data from Big Query

# estimate cost of fething data from bigquery

# In[ ]:


query = """
    SELECT column1, column2, columnN FROM `{}` WHERE DATE(_PARTITIONTIME) = "YYYY-MM-DD"
""".format(bq_table_in)

from google.cloud import bigquery

job_config = bigquery.QueryJobConfig()
job_config.dry_run = True
job_config.use_query_cache = False
query_job = bigquery.Client().query(
  (query),
    location="EU",
    job_config=job_config
)
data_amt = query_job.total_bytes_processed
print("{} GB will be processed which is roughly {} USD cost".format(round(data_amt / 1024**3,2), round(data_amt / 1024**4 * 5,2)))


# In[ ]:


query = """
SELECT right(service_id, 2) AS marketplace,
offer.position_in_context as position,
count(offer.clicks[SAFE_OFFSET(0)]) as clicks
FROM `sc-9366-nga-prod.search_logs.search_logs`, unnest(offer_listing.offers_with_ads) AS offer
WHERE TIMESTAMP_TRUNC(_PARTITIONTIME, DAY) = TIMESTAMP("2024-09-15")
and offer_listing.page = 1
and search_phrase.raw is not null
and offer_listing.sorting = "RELEVANCE_DESC"
and tag.raw = 'ProductListing'
and offer.position_in_context is not null
and offer.clicks is not null
group by marketplace, position
order by marketplace, position
""".format(bq_table_in)

from google.cloud import bigquery

job_config = bigquery.QueryJobConfig()
job_config.dry_run = True
job_config.use_query_cache = False
query_job = bigquery.Client().query(
  (query),
    location="EU",
    job_config=job_config
)
data_amt = query_job.total_bytes_processed
print("{} GB will be processed which is roughly {} USD cost".format(round(data_amt / 1024**3,2), round(data_amt / 1024**4 * 5,2)))


# ### read data using Storage Read API

# In[ ]:


# spark_df = spark.read.format('bigquery') \
#   .option('table', bq_table_in) \
#   .option('filter', "date < '1959-08-21'") \
#   .load()

spark_df = spark.read.format('bigquery') \
  .option('table', bq_table_in) \
  .load()


# #### run calculation on read data

# In[ ]:


spark_df.count()


# In[ ]:


#spark_df.take(10)


# In[ ]:


#new_df = spark_df.withColumn('year', year(col('date')))


# In[ ]:





# In[ ]:


from pyspark.sql import functions as F

exploded_df = spark_df \
    .withColumn('offer', F.explode('offer_listing.offers_with_ads'))

filtered_df = exploded_df.filter(
    (F.col('_PARTITIONTIME') >= F.lit('2024-09-15 00:00:00')) & 
    (F.col('_PARTITIONTIME') < F.lit('2024-09-16 00:00:00')) &  
    (F.col('offer_listing.page') == 1) &
    (F.col('search_phrase.raw').isNotNull()) &
    (F.col('offer_listing.sorting') == "RELEVANCE_DESC") &
    (F.col('tag.raw') == 'ProductListing') &
    (F.col('offer.position_in_context').isNotNull()) &
    (F.col('offer.clicks').isNotNull())
)


# In[ ]:


transformed_df = filtered_df.withColumn(
    'marketplace', F.expr("right(service_id, 2)")
).withColumn(
    'position', F.col('offer.position_in_context')
)

aggregated_df = transformed_df.groupBy(
    'marketplace', 'position'
).agg(
    F.count(F.col('offer.clicks')[0]).alias('clicks')
)


# In[ ]:


sorted_df = aggregated_df.orderBy('marketplace', 'position')


# In[ ]:


sorted_df.show()


# #### write calculated data into Big Query dataset
# NOTE: under the hood data is temporarily stored in *temp_bucket_name* and eventually copied into BQ table, that is why the same location is needed

# In[ ]:


sorted_df.write.format('bigquery') \
  .option('table', bq_table_out) \
  .save()


# ### read data using query
# NOTE: the dataset is needed for materialization the query results. Connector reads the data from the temp table written into that dataset.

# In[ ]:


spark.conf.set('viewsEnabled','true')
spark.conf.set('materializationDataset','DATASET')

sql = '''
  SELECT name, count(*) as cnt from {}
  group by name
  '''.format(bq_table_in)
df = spark.read.format('bigquery').load(sql)
df.show()


# ##### NOTE: now you can play with dataframe read that way in normal manner
