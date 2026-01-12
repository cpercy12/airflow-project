import os
import pandas as pd

# Chemins
INPUT_PATH = "data/raw/SalesOrderDetail.csv"
OUTPUT_PATH = "data/processed/sales/salesorderdetail_clean.parquet"

# Création du dossier de sortie si absent
os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

# 1. Chargement
df = pd.read_csv(INPUT_PATH)

print("Colonnes chargées :", df.columns.tolist())
print(df.head())

# 2. Conversion des types
numeric_cols = [
    "SalesOrderID",
    "SalesOrderDetailID",
    "OrderQty",
    "ProductID",
    "UnitPrice",
    "UnitPriceDiscount",
    "LineTotal"
]
for col in numeric_cols:
    df[col] = pd.to_numeric(df[col], errors="coerce")

date_cols = ["ModifiedDate"]
for col in date_cols:
    df[col] = pd.to_datetime(df[col], format="%Y-%m-%d %H:%M:%S.%f", errors="coerce")

# 3. Nettoyage des chaînes
df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)

# 4. Suppression des lignes invalides
df = df.dropna(subset=["SalesOrderID", "ProductID", "OrderQty"])

# 5. Suppression des doublons
df = df.drop_duplicates(subset=["SalesOrderID", "SalesOrderDetailID"])

# -----------------------------
# 5. Vérification cohérence métier
# -----------------------------
df["expected_linetotal"] = (
    df["UnitPrice"] * df["OrderQty"] * (1 - df["UnitPriceDiscount"])
)

# Tolérance arrondis
df = df[
    (df["LineTotal"] - df["expected_linetotal"]).abs() < 0.01
]

df = df.drop(columns=["expected_linetotal"])

# -----------------------------

# 6. Sauvegarde en Parquet
df.to_parquet(OUTPUT_PATH, index=False)

print("SalesOrderDetail nettoyé et sauvegardé avec succès")
print("Nombre de lignes finales :", len(df))