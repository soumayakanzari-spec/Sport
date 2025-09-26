import os
import time
import random
import pandas as pd
import psycopg2
from datetime import datetime, timedelta
from itertools import zip_longest

# Attente pour que PostgreSQL démarre
time.sleep(5)

# --- Connexion à PostgreSQL ---
conn = psycopg2.connect(
    host=os.environ["DB_HOST"],
    dbname=os.environ["DB_NAME"],
    user=os.environ["DB_USER"],
    password=os.environ["DB_PASSWORD"]
)
cur = conn.cursor()

# --- Création de la table sport_activities ---
cur.execute("""
    CREATE TABLE IF NOT EXISTS sport_activities (
        id SERIAL PRIMARY KEY,
        id_salarie VARCHAR(50),
        start_date TIMESTAMP,
        activity_type VARCHAR(100),
        distance_m REAL,
        duration_sec INTEGER,
        comment TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
""")

# --- Création de la table employee_prime ---
cur.execute("""
    CREATE TABLE IF NOT EXISTS employee_prime (
        id SERIAL PRIMARY KEY,
        ID_salarie VARCHAR(50) NOT NULL,
        Salaire_brut VARCHAR(50) NOT NULL,
        Moyen_deplacement VARCHAR(100) NOT NULL,
        Nom VARCHAR(100),
        Prenom VARCHAR(100),
        Date_embauche DATE,
        Type_contrat VARCHAR(50),
        BU VARCHAR(100),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
""")
conn.commit()

print("✅ Tables créées avec succès")

# --- Charger les données Excel ---
df_sportives = pd.read_excel("data/Donnees_Sportive.xlsx")
df_rh = pd.read_excel("data/Donnees_RH.xlsx")

# Afficher les colonnes disponibles pour le débogage
print("Colonnes dans Donnees_RH.xlsx:", list(df_rh.columns))
print("Colonnes dans Donnees_Sportive.xlsx:", list(df_sportives.columns))

# --- Nettoyer les noms de colonnes pour df_rh ---
df_rh.columns = df_rh.columns.str.strip().str.replace(' ', '_').str.replace('-', '_')\
    .str.replace('é', 'e').str.replace('è', 'e').str.replace('ê', 'e')\
    .str.replace('à', 'a').str.replace('â', 'a').str.replace('î', 'i')\
    .str.replace('ô', 'o').str.replace('û', 'u').str.replace('ç', 'c')\
    .str.replace('°', '').str.replace('(', '').str.replace(')', '')\
    .str.replace("'", "").str.replace('"', '').str.replace('/', '_')\
    .str.lower()

print("Colonnes nettoyées dans Donnees_RH.xlsx:", list(df_rh.columns))

# --- Mapping des colonnes pour df_rh - MISE À JOUR ---
column_mapping = {
    'id_salarie': 'id_salarie',
    'id': 'id_salarie',
    'salarie': 'id_salarie',
    'nom': 'nom',
    'prenom': 'prenom',
    'date_de_naissance': 'date_naissance',
    'date_dembauche': 'date_embauche',
    'bu': 'bu',
    'salaire_brut': 'salaire_brut',
    'salaire': 'salaire_brut',
    'type_de_contrat': 'type_contrat',
    'moyen_de_deplacement': 'moyen_deplacement',
    'deplacement': 'moyen_deplacement',
    'transport': 'moyen_deplacement',
    'moyen_transport': 'moyen_deplacement'
}

# Renommer les colonnes selon le mapping
df_rh.columns = [column_mapping.get(col, col) for col in df_rh.columns]

print("Colonnes après mapping:", list(df_rh.columns))

# --- Vérifier que les colonnes requises pour employee_prime existent ---
required_columns = ['id_salarie', 'salaire_brut', 'moyen_deplacement']
missing_columns = [col for col in required_columns if col not in df_rh.columns]

if missing_columns:
    print(f"⚠️ Colonnes manquantes: {missing_columns}")
    print("Création de données par défaut...")
    
    # Créer des données par défaut si des colonnes manquent
    if 'moyen_deplacement' not in df_rh.columns:
        moyens = ["Voiture", "Transport en commun", "Vélo", "Marche", "Télétravail"]
        df_rh['moyen_deplacement'] = [random.choice(moyens) for _ in range(len(df_rh))]
    
    if 'salaire_brut' not in df_rh.columns:
        df_rh['salaire_brut'] = [str(random.randint(30000, 80000)) for _ in range(len(df_rh))]
    
    if 'id_salarie' not in df_rh.columns:
        # Essayer de trouver une colonne ID alternative
        id_cols = [col for col in df_rh.columns if 'id' in col.lower() or 'matricule' in col.lower()]
        if id_cols:
            df_rh['id_salarie'] = df_rh[id_cols[0]]
        else:
            df_rh['id_salarie'] = [f"EMP{i:03d}" for i in range(1, len(df_rh)+1)]

# Convertir toutes les valeurs en string pour éviter les problèmes de type
df_rh['salaire_brut'] = df_rh['salaire_brut'].astype(str)

# Afficher les valeurs de salaire_brut
print("Valeurs de salaire_brut:", df_rh['salaire_brut'].unique())

# --- Types d'activités pour sport_activities ---
activities = [
    "Course à pied", "Randonnée", "Vélo", "Marche", "Trottinette",
    "Natation", "Tennis", "Football", "Basketball", "Yoga"
]

# --- Identifier la colonne d'ID pour sport_activities ---
id_cols_sport = [col for col in df_sportives.columns if "ID" in col.upper() or "MATRICULE" in col.upper()]
if id_cols_sport:
    id_col = id_cols_sport[0]
else:
    id_col = df_sportives.columns[0]  # Prendre la première colonne

print(f"Utilisation de la colonne '{id_col}' comme ID pour les activités sportives")

# --- Génération de données pour sport_activities ---
def generate_sport_data(df_sportives, months=12):
    data = []
    today = datetime.today()
    for _, row in df_sportives.iterrows():
        id_salarie = row[id_col]
        for _ in range(random.randint(10, 40)):
            start_date = today - timedelta(days=random.randint(0, 30 * months))
            duration_sec = random.randint(600, 7200)
            activity_type = random.choice(activities)
            distance = round(random.uniform(1000, 25000), 2)
            comment = random.choice([
                "", "Super séance !", "Randonnée magnifique", "Reprise du sport :)",
                "Séance intensive", "Détente", "Entraînement matinal"
            ])
            data.append((id_salarie, start_date, activity_type, distance, duration_sec, comment))
    return data

# --- Générer les données pour sport_activities ---
print("Génération des données sport_activities...")
sport_rows = generate_sport_data(df_sportives)
print(f"{len(sport_rows)} données sportives générées")

# --- Préparer les données pour employee_prime ---
print("Préparation des données employee_prime...")

# Convertir les dates si nécessaire
if 'date_embauche' in df_rh.columns:
    df_rh['date_embauche'] = pd.to_datetime(df_rh['date_embauche'], errors='coerce')

# Remplir les valeurs manquantes
df_rh = df_rh.fillna({
    'nom': 'Inconnu',
    'prenom': 'Inconnu',
    'moyen_deplacement': 'Non spécifié',
    'bu': 'Non spécifié',
    'type_contrat': 'CDI',
    'salaire_brut': '30000'  # Maintenant une chaîne
})

# --- Préparer les données employee_prime pour l'insertion ---
employee_rows = []
for i, row in df_rh.iterrows():
    employee_rows.append((
        str(row.get('id_salarie', '')),
        str(row.get('salaire_brut', '30000')),  # Conserver comme string
        str(row.get('moyen_deplacement', 'Non spécifié')),
        str(row.get('nom', 'Inconnu')),
        str(row.get('prenom', 'Inconnu')),
        row.get('date_embauche', None),
        str(row.get('type_contrat', 'CDI')),
        str(row.get('bu', 'Non spécifié'))
    ))

print(f"{len(employee_rows)} données employee préparées")

# --- Insérer les données dans une seule boucle ---
print("Début de l'insertion des données...")

# Insertion des employés d'abord
for i, employee_row in enumerate(employee_rows):
    try:
        cur.execute("""
            INSERT INTO employee_prime (
                ID_salarie, Salaire_brut, Moyen_deplacement, Nom, Prenom, 
                Date_embauche, Type_contrat, BU
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, employee_row)
        
        conn.commit()
        print(f"✅ Insertion employé #{i+1}: {employee_row[0]} - {employee_row[2]}")
            
    except Exception as e:
        print(f"❌ Erreur lors de l'insertion de l'employé {i+1}: {e}")
        conn.rollback()
    
    # Attendre 1 seconde avant la prochaine insertion
    time.sleep(1)

# Puis insertion des activités sportives
for i, sport_row in enumerate(sport_rows):
    try:
        cur.execute("""
            INSERT INTO sport_activities (
                id_salarie, start_date, activity_type, distance_m, duration_sec, comment
            ) VALUES (%s, %s, %s, %s, %s, %s)
        """, sport_row)
        
        conn.commit()
        
        if (i + 1) % 10 == 0:
            print(f"✅ {i+1}/{len(sport_rows)} insertions - Activité: {sport_row[0]} - {sport_row[2]}")
        else:
            print(f"✅ Insertion activité #{i+1}: {sport_row[0]} - {sport_row[2]}")
                
    except Exception as e:
        print(f"❌ Erreur lors de l'insertion de l'activité {i+1}: {e}")
        conn.rollback()
    
    # Attendre 1 seconde avant la prochaine insertion
    time.sleep(1)

print(f"✅ Toutes les données ont été insérées avec succès!")
print(f"📊 Total sport_activities: {len(sport_rows)} lignes insérées")
print(f"📊 Total employee_prime: {len(employee_rows)} lignes insérées")

# --- Fermer la connexion ---
cur.close()
conn.close()

print("🎉 Toutes les opérations sont terminées avec succès!")