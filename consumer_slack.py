import os
from kafka import KafkaConsumer
import json
import requests
import time

# Configuration Slack
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "https://hooks.slack.com/services/your-webhook-here")

# Configuration Redpanda
REDPANDA_BROKERS = os.getenv("REDPANDA_BROKERS", "redpanda:9092")
TOPIC_NAME = os.getenv("TOPIC_NAME", "postgres_server.public.sport_activities")

def send_to_slack(message):
    """Envoie un message à Slack"""
    try:
        slack_data = {'text': message}
        response = requests.post(
            SLACK_WEBHOOK_URL, 
            data=json.dumps(slack_data),
            headers={'Content-Type': 'application/json'}
        )
        if response.status_code == 200:
            print(f"✅ Message envoyé à Slack: {message}")
        else:
            print(f"❌ Erreur Slack: {response.status_code}")
    except Exception as e:
        print(f"❌ Erreur lors de l'envoi à Slack: {e}")

def process_message(message_value):
    """Traite le message JSON et le formate pour Slack"""
    try:
        data = json.loads(message_value)
        
        # Extraction des données (structure Debezium)
        if 'payload' in data and 'after' in data['payload']:
            after_data = data['payload']['after']
            
            employee_id = after_data.get('id_salarie', 'Inconnu')
            activity_type = after_data.get('activity_type', 'Activité inconnue')
            distance = after_data.get('distance_m', 0)
            duration = after_data.get('duration_sec', 0)
            comment = after_data.get('comment', '')
            
            # Création du message Slack
            message = f"🎯 Nouvelle activité sportive!\n" \
                     f"👤 Employé: {employee_id}\n" \
                     f"🏃 Type: {activity_type}\n" \
                     f"📏 Distance: {distance}m\n" \
                     f"⏱️ Durée: {duration}s"
            
            if comment:
                message += f"\n💬 Commentaire: {comment}"
            
            return message
        
    except Exception as e:
        print(f"❌ Erreur traitement message: {e}")
    
    return None

def main():
    print("🚀 Démarrage du consumer Redpanda -> Slack")
    print(f"📡 Connexion à Redpanda: {REDPANDA_BROKERS}")
    print(f"📋 Topic: {TOPIC_NAME}")
    
    # Tentative de connexion à Redpanda avec réessais
    max_retries = 10
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            consumer = KafkaConsumer(
                TOPIC_NAME,
                bootstrap_servers=REDPANDA_BROKERS,
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id='slack-consumer-group',
                value_deserializer=lambda x: x.decode('utf-8')
            )
            print("✅ Connecté à Redpanda!")
            break
            
        except Exception as e:
            retry_count += 1
            print(f"❌ Erreur connexion Redpanda (tentative {retry_count}/{max_retries}): {e}")
            time.sleep(5)
    
    if retry_count == max_retries:
        print("❌ Impossible de se connecter à Redpanda")
        return
    
    # Lecture des messages
    print("👂 En écoute des messages...")
    try:
        for message in consumer:
            print(f"📨 Message reçu: {message.value[:100]}...")
            
            # Traitement du message
            slack_message = process_message(message.value)
            
            if slack_message:
                # Envoi à Slack
                send_to_slack(slack_message)
            else:
                print("⚠️ Message non traité")
                
    except KeyboardInterrupt:
        print("🛑 Arrêt demandé")
    except Exception as e:
        print(f"❌ Erreur: {e}")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()