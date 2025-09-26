import pandas as pd

# Charger les données RH
df_rh = pd.read_excel("data/Donnees_RH.xlsx")

# Renommer les colonnes (suppression des espaces)
df_rh.columns = df_rh.columns.str.strip().str.replace(' ', '_')

# Conversion des dates
df_rh['Date_de_naissance'] = pd.to_datetime(df_rh['Date_de_naissance'], errors='coerce')


# Nettoyage des chaînes de caractères
str_cols = ['Nom', 'Prénom', 'BU', 'Type_de_contrat', 'Adresse_du_domicile', 'Moyen_de_déplacement']
for col in str_cols:
    df_rh[col] = df_rh[col].astype(str).str.strip().str.title()

# Nettoyage des valeurs numériques
df_rh['Salaire_brut'] = pd.to_numeric(df_rh['Salaire_brut'], errors='coerce')
df_rh['Nombre_de_jours_de_CP'] = pd.to_numeric(df_rh['Nombre_de_jours_de_CP'], errors='coerce')

# Supprimer les doublons si l'ID est unique
df_rh = df_rh.drop_duplicates(subset='ID_salarié')

# Affichage final
print(df_rh.info())

# Sauvegarde du DataFrame nettoyé
df_rh.to_csv("data/Donnees_RH.csv", index=False)