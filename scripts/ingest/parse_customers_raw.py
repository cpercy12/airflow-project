import os
import pandas as pd

# Chemins
RAW_PATH = "data/raw/Customer.csv"  # adapte le nom exact si besoin
OUTPUT_PATH = "data/staging/customer/customer_structured.csv"

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
    sep="\t",        # tabulation comme séparateur
    header=None,     # pas d'en-têtes dans le fichier brut
    names=columns,   # ajouter les bons noms
    engine="python"
)

# Vérification rapide
print("Colonnes détectées :", df.columns.tolist())
print(df.head())

# Sauvegarde directement en CSV structuré
df.to_csv(OUTPUT_PATH, index=False)

print(f"Fichier {OUTPUT_PATH} créé avec succès !")