import polars as pl
from fuzzywuzzy import process, fuzz

def fuzzymergy(gateway, terms):
   wow = terms.collect()['org_name'].to_list()
   test_str = 'SONORAN SLEEP CENTER'
   print(process.extract(test_str, wow, limit=1)[0][0])
   print(process.extract(test_str, wow, limit=1)[0][1])

   result = (
      gateway
      .collect()
      .with_columns(
         pl.col('licensee_clean').map_elements(lambda x: process.extract(x, wow, limit=1)[0][0]).alias('match'),
         pl.col('licensee_clean').map_elements(lambda x: process.extract(x, wow, limit=1)[0][1]).alias('score')
      )
    )
   return result


terms = (
    pl.scan_csv("data/terms.csv", infer_schema_length=10000)
    .select('org_name')
)


gateway = (
    pl.scan_csv('data/gateway_connections.csv', infer_schema_length=10000)
    .with_columns(
        pl.col('Licensee').str.replace_all(r'\([^)]*\)', '').str.to_uppercase().str.strip_chars(' )').alias('licensee_clean'),
    )
    .select('licensee_clean')
)

fuzzymergy(gateway=gateway, terms=terms).sort('score',descending=False).write_csv('data/checkywecky.csv')