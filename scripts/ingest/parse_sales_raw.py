import pandas as pd

RAW_PATH = "data/raw/SalesOrderHeader.csv"
OUTPUT_PATH = "data/staging/sales/sales_structured.csv"

colspecs = [
    (0, 6),      # SalesOrderID
    (6, 29),     # OrderDate
    (29, 52),    # DueDate
    (52, 75),    # ShipDate
    (75, 78),    # OrderQty
    (78, 90),    # SalesOrderNumber
    (90, 100),   # ProductID
    (100, 115),  # CustomerID
    (115, 135),  # UnitPrice
    (135, 160),  # TotalDue
    (160, 196),  # RowGuid
    (196, 219)   # ModifiedDate
]

columns = [
    "SalesOrderID",
    "OrderDate",
    "DueDate",
    "ShipDate",
    "OrderQty",
    "SalesOrderNumber",
    "ProductID",
    "CustomerID",
    "UnitPrice",
    "TotalDue",
    "RowGuid",
    "ModifiedDate"
]

df = pd.read_fwf(
    RAW_PATH,
    colspecs=colspecs,
    names=columns
)

print("Colonnes reconstruites :", df.columns.tolist())
print(df.head())

df.to_csv(OUTPUT_PATH, index=False)

print("Fichier structuré créé avec succès")

