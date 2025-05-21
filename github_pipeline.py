import os
import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import HeaderLinkPaginator
from itertools import islice

#extract
os.environ['EXTRACT__WORKERS'] = '3'
#os.environ["EXTRACT__DATA_WRITER__FILE_MAX_ITEMS"] = "100000"
#normalize
os.environ['NORMALIZE__WORKERS'] = '5'
os.environ['NORMALIZE__DATA_WRITER__BUFFER_MAX_ITEMS'] = '5000'
os.environ["NORMALIZE__DATA_WRITER__FILE_MAX_ITEMS"] = "5000"
#load
os.environ['LOAD__WORKERS'] = '5'

def yield_chunks(iterable, chunk_size=10):
    iterator = iter(iterable)
    while chunk := islice(iterator, chunk_size):  # <--- we slice data into chunks
        yield chunk


@dlt.source
def jaffle_source():
    client = RESTClient(
            base_url="https://jaffle-shop.scalevector.ai/api/v1",
            paginator=HeaderLinkPaginator(),
    )

    @dlt.resource(table_name="jaffle_customers", write_disposition="merge", primary_key="id", parallelized=True)
    def jaffle_customers():
      for attempt in range(10):
        try:
          for page in client.paginate("customers"):
            yield page
        except Exception as e:
          pass
        else:
          break

    #@retry(wait_random_min=1000, wait_random_max=2000, stop_max_attempt_number=10)
    @dlt.resource(table_name="jaffle_orders", write_disposition="merge", primary_key="id", parallelized=True)
    def jaffle_orders():
      params = {
            "page_size": 5000 #with the limit set to 100 the pipeline ran for approximately 10 minutes
        }
      for attempt in range(10):
        try:
          for page in client.paginate("orders", params=params):
            yield page
        except Exception as e:
          pass
        else:
          break
         
      #yield from yield_chunks(client.paginate("orders", params=params), chunk_size=10) #yielding chunks seems to overburden the api, causing excessively long load time, execution was stopped after 37 minutes

    @dlt.resource(table_name="jaffle_products", write_disposition="merge", primary_key="sku", parallelized=True)
    def jaffle_products():
      for attempt in range(10):
        try:
          for page in client.paginate("products"):
            yield page
        except Exception as e:
          pass
        else:
          break

    return jaffle_customers,jaffle_orders,jaffle_products

pipeline = dlt.pipeline(
    pipeline_name="jaffle_shop_pipeline",
    destination="duckdb",
    dataset_name="jaffle_stage",
    dev_mode=True,
)

load_info = pipeline.run(jaffle_source())
print(pipeline.last_trace)