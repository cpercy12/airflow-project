import os
import pandas as pd

# Chemins
RAW_PATH = "data/raw/Customer.csv"
OUTPUT_PATH = "data/processed/customer/customers.parquet"

# Créer le dossier de sortie si nécessaire
os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

# Définir les colonnes exactes
columns = [
    "CustomerID",
    "PersonID",
    "StoreID",
    "TerritoryID",
    "AccountNumber",
    "RowGuid",
    "ModifiedDate"
]

# Lecture du fichier TSV (tab-separated)
df = pd.read_csv(
    RAW_PATH,
    sep="\t",
    header=None,
    names=columns,
    engine="python"
)

# Vérification
print("Colonnes détectées :", df.columns.tolist())
print(df.head())

# Nettoyage des chaînes
df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)

# Conversion numérique sécurisée
numeric_cols = ["CustomerID", "PersonID", "StoreID", "TerritoryID"]
for col in numeric_cols:
    df[col] = pd.to_numeric(df[col], errors="coerce")

# Conversion dates
df["ModifiedDate"] = pd.to_datetime(      
    df["ModifiedDate"],
    format="%Y-%m-%d %H:%M:%S.%f",
    errors="coerce"
)

# 4. Vérification des valeurs critiques
df = df[df["AccountNumber"].notna()]

# 5. Suppression des doublons
df = df.drop_duplicates(subset=["CustomerID"])

# Sauvegarde en Parquet
df.to_parquet(OUTPUT_PATH, index=False)

print("Customers nettoyé et sauvegardé avec succès")