import pandas as pd

df = pd.read_parquet("data/mart/fact_sales.parquet")
print(df.head())
df.info()