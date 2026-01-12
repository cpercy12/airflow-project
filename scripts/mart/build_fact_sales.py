import os
import pandas as pd

# -----------------------------
# Paths (PARQUET)
# -----------------------------
HEADER_PATH = "data/processed/sales/sales_clean.parquet"
DETAIL_PATH = "data/processed/sales/salesorderdetail_clean.parquet"
CUSTOMER_PATH = "data/processed/customer/customers.parquet"
PRODUCT_PATH = "data/processed/products/product_clean.parquet"

OUTPUT_PATH = "data/mart/fact_sales.parquet"

# Création du dossier de sortie si absent
os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

# -----------------------------
# 1. Lecture des fichiers parquet
# -----------------------------
header = pd.read_parquet(HEADER_PATH)
detail = pd.read_parquet(DETAIL_PATH)
customer = pd.read_parquet(CUSTOMER_PATH)
product = pd.read_parquet(PRODUCT_PATH)

# -----------------------------
# 2. Jointure Header ↔ Detail
# -----------------------------
fact = header.merge(
    detail,
    on="SalesOrderID",
    how="inner"
)

# -----------------------------
# 3. Jointure Customer
# -----------------------------
fact = fact.merge(
    customer,
    on="CustomerID",
    how="left",
    suffixes=("", "_customer")
)

# -----------------------------
# 4. Jointure Product
# -----------------------------
fact = fact.merge(
    product,
    on="ProductID",
    how="left",
    suffixes=("", "_product")
)

# -----------------------------
# 5. Sélection des colonnes utiles
# -----------------------------
fact = fact[
    [
        "SalesOrderID",
        "SalesOrderDetailID",
        "OrderDate",
        "CustomerID",
        "ProductID",
        "OrderQty",
        "UnitPrice",
        "UnitPriceDiscount",
        "LineTotal",
        "TotalDue",
        "Name",
        "AccountNumber"
    ]
]

# -----------------------------
# 6. Sauvegarde en Parquet
# -----------------------------
fact.to_parquet(OUTPUT_PATH, index=False)

print(f"fact_sales créé en parquet : {fact.shape[0]} lignes")