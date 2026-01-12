import pandas as pd

# Chemins
RAW_PATH = "data/raw/SalesOrderDetail.csv"
OUTPUT_PATH = "data/staging/orders/SalesOrderDetail_structured.csv"

# Définir les positions des colonnes (colspecs) pour SalesOrderDetail.txt
# À adapter selon ton fichier
colspecs = [
    (0, 11),     # SalesOrderID
    (11, 21),    # SalesOrderDetailID
    (21, 31),    # CarrierTrackingNumber
    (31, 41),    # OrderQty
    (41, 56),    # ProductID
    (56, 71),    # UnitPrice
    (71, 86),    # UnitPriceDiscount
    (86, 101),   # LineTotal
    (101, 137),  # RowGuid
    (137, 157)   # ModifiedDate
]

# Définir les noms des colonnes
columns = [
    "SalesOrderID",
    "SalesOrderDetailID",
    "CarrierTrackingNumber",
    "OrderQty",
    "ProductID",
    "UnitPrice",
    "UnitPriceDiscount",
    "LineTotal",
    "RowGuid",
    "ModifiedDate"
]

# Lecture du fichier texte avec colonnes fixes
df = pd.read_fwf(
    RAW_PATH,
    colspecs=colspecs,
    names=columns
)

# Affichage pour vérification
print("Colonnes reconstruites :", df.columns.tolist())
print(df.head())

# Nettoyage rapide : supprimer espaces avant/après
df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)


# Sauvegarde en CSV
df.to_csv(OUTPUT_PATH, index=False)

print("Fichier SalesOrderDetail structuré créé avec succès")