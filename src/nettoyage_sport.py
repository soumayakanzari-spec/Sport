
import pandas as pd


# Charger les données sportives
df_sport = pd.read_excel("data/Donnees_Sportive.xlsx")

# Nettoyage des noms de colonnes
df_sport.columns = df_sport.columns.str.strip().str.replace(' ', '_')




# Supprimer les doublons sur l'ID salarié
df_sport = df_sport.drop_duplicates(subset='ID_salarié')

# Affichage final
print(df_sport.info())

# Sauvegarde du DataFrame nettoyé
df_sport.to_csv("data/Donnees_Sportive.csv", index=False)
