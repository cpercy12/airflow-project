import os
import pandas as pd

# Chemins
INPUT_PATH = "data/raw/Product.csv"
OUTPUT_PATH = "data/processed/products/product_clean.parquet"

# Création du dossier de sortie si absent
os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

# 1. Chargement
df = pd.read_csv(INPUT_PATH)

print("Colonnes chargées :", df.columns.tolist())
print(df.head())

# 2. Conversion des types
numeric_cols = [
    "ProductID",
    "SafetyStockLevel",
    "ReorderPoint",
    "StandardCost",
    "ListPrice",
    "Weight",
    "DaysToManufacture",
    "ProductSubcategoryID",
    "ProductModelID"
]
for col in numeric_cols:
    df[col] = pd.to_numeric(df[col], errors="coerce")

date_cols = ["SellStartDate", "SellEndDate", "DiscontinuedDate"]
for col in date_cols:
    df[col] = pd.to_datetime(df[col], format="%Y-%m-%d %H:%M:%S.%f", errors="coerce")

# 3. Nettoyage des chaînes
df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)

# 4. Suppression éventuelle des doublons
df = df.drop_duplicates(subset=["ProductID"])
df = df[df["Name"].notna()]

# 5. Sauvegarde en Parquet
df.to_parquet(OUTPUT_PATH, index=False)

print("Product nettoyé et sauvegardé avec succès")
print("Nombre de lignes finales :", len(df))
 