import os
import pandas as pd

RAW_PATH = "data/raw/Product.csv"
OUTPUT_PATH = "data/staging/products/Product_structured.csv"

# Créer le dossier de sortie si nécessaire
os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

# Lecture du fichier (TAB-separated, pas CSV classique)
df = pd.read_csv(
    RAW_PATH,
    sep="\t",
    engine="python"
)

# Vérification
print("Colonnes détectées :", df.columns.tolist())
print(df.head())

# Nettoyage des chaînes
df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)

# Conversion numérique
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
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

# Conversion dates AdventureWorks
date_cols = ["SellStartDate", "SellEndDate", "DiscontinuedDate"]

for col in date_cols:
    if col in df.columns:
        df[col] = pd.to_datetime(
            df[col],
            format="%Y-%m-%d %H:%M:%S.%f",
            errors="coerce"
        )

# Sauvegarde
df.to_csv(OUTPUT_PATH, index=False)

print("Product structuré créé avec succès")