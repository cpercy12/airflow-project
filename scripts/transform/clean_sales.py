import os
import pandas as pd


INPUT_PATH = "data/raw/SalesOrderHeader.csv"
OUTPUT_PATH = "data/processed/sales/sales_clean.parquet"

# Création du dossier de sortie si absent
os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

# 1. Chargement avec suppression des espaces invisibles
df = pd.read_csv(INPUT_PATH)
df.columns = df.columns.str.strip()
print("Colonnes chargées :", df.columns.tolist())
print(df.head())

# 2. Conversion des types (dates)
date_cols = ["OrderDate", "DueDate", "ShipDate", "ModifiedDate"]
for col in date_cols:
    df[col] = pd.to_datetime(df[col], errors="coerce")

# 3. Conversion des colonnes numériques
numeric_cols = ["SubTotal", "TaxAmt", "Freight", "TotalDue"]
for col in numeric_cols:
    df[col] = pd.to_numeric(df[col], errors="coerce")

# 4. Vérifier l'existence des colonnes OrderQty, UnitPrice, ProductID
# Dans SalesOrderHeader il n'y a pas OrderQty ou ProductID, donc on ne les traite pas ici

# 5. Suppression des lignes invalides pour les colonnes critiques
critical_cols = ["SalesOrderID", "CustomerID", "OrderDate", "TotalDue"]
df = df.dropna(subset=critical_cols)

# 6. Règles de qualité simples
df = df[df["TotalDue"] >= 0]

# 7. Suppression des doublons sur la clé primaire SalesOrderID
df = df.drop_duplicates(subset=["SalesOrderID"])

# 8. Sauvegarde en Parquet
df.to_parquet(OUTPUT_PATH, index=False)

print("Nettoyage terminé avec succès")
print("Nombre de lignes finales :", len(df))

if __name__ == "__main__":
    print("clean_sales exécuté correctement")

   