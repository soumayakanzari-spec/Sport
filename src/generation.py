import pandas as pd
import random
from datetime import datetime, timedelta

# --- 1. Charger les fichiers Excel ---
df_sportives = pd.read_excel("data/Donnees_Sportive.xlsx")

print("Données Sportives :")
print(df_sportives.head())

# Vérifie si une colonne 'ID salarié' ou similaire existe
id_col = [col for col in df_sportives.columns if "ID" in col.upper()][0]

# --- 2. Définir les types d'activités sportives 
activities = [
    "Course à pied", "Randonnée", "Vélo", "Marche", "Trottinette"
]

# --- 3. Générer des données sportives simulées ---
def generate_sport_data(df_rh, months=12):
    data = []
    today = datetime.today()
    for _, row in df_sportives.iterrows():
        id_salarie = row[id_col]
        num_activities = random.randint(10, 40)  # entre 10 et 40 activités
        for _ in range(num_activities):
            start_date = today - timedelta(days=random.randint(0, 30 * months))
            duration_sec = random.randint(600, 7200)  # entre 10 min et 2h
            activity_type = random.choice(activities)
            if activity_type in ["Course à pied", "Vélo", "Marche", "Trottinette", "Randonnée"]:
                distance = round(random.uniform(1000, 25000), 2)
            else:
                distance = None
            comment = random.choice([
                "", "Super séance !", "Randonnée magnifique", "Reprise du sport :)"
            ])
            data.append({
                "ID salarié": id_salarie,
                "Date de début": start_date.strftime("%Y-%m-%d %H:%M"),
                "Type": activity_type,
                "Distance (m)": distance,
                "Temps écoulé (s)": duration_sec,
                "Commentaire": comment,
            })
    return pd.DataFrame(data)

# --- 4. Générer et sauvegarder les données simulées ---
df_simulated = generate_sport_data(df_sportives)
print("\nExtrait des données sportives simulées :")
print(df_simulated.head())

# Sauvegarde en Excel ou CSV
df_simulated.to_excel("activites_sportives_simulees.xlsx", index=False)
# ou
# df_simulated.to_csv("activites_sportives_simulees.csv", index=False)
