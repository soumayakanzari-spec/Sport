import random
import pandas as pd
from datetime import datetime, timedelta



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



# Affichage final
print(df_rh.info())


activities = ["Course à pied", "Vélo", "Marche", "Randonnée"]

def generate_sport_data(df_rh, months=6):
    data = []
    today = datetime.today()
    for _, row in df_rh.iterrows():
        id_salarie = row["ID_salarié"]
        for _ in range(random.randint(5, 15)):
            start_date = today - timedelta(days=random.randint(0, 30 * months))
            activity_type = random.choice(activities)
            distance = round(random.uniform(1000, 15000), 2)
            duration_sec = random.randint(600, 5400)
            data.append({
                "ID salarié": id_salarie,
                "Date": start_date,
                "Type": activity_type,
                "Distance (m)": distance,
                "Durée (s)": duration_sec
            })
    return pd.DataFrame(data)

df_sport = generate_sport_data(df_rh)
print("Sport simulé :")
print(df_sport.head())
