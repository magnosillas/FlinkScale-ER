import csv
from kafka import KafkaProducer
import json
import time
from er_models import Attribute, EntityProfile # Importa as classes que criamos

def create_producer(kafka_server='localhost:9093'):
    """Cria um produtor Kafka com retry para dar tempo ao container de iniciar."""
    retries = 15
    while retries > 0:
        try:
            producer = KafkaProducer(
                bootstrap_servers=kafka_server,
                value_serializer=lambda v: json.dumps(v, default=lambda o: o.__dict__).encode('utf-8')
            )
            print("✅ Conectado ao Kafka com sucesso!")
            return producer
        except Exception as e:
            print(f"⏳ Conectando ao Kafka... Tentativas restantes: {retries-1}. Erro: {e}")
            retries -= 1
            time.sleep(2) # Espera 2 segundos antes de tentar novamente
    raise Exception("❌ Não foi possível conectar ao Kafka após várias tentativas.")

def stream_dataset(producer, topic, file_path, source_name):
    """Lê um arquivo CSV e envia cada linha como uma EntityProfile para o Kafka."""
    print(f"🚀 Começando a enviar dados de '{source_name}' do arquivo '{file_path}' para o tópico '{topic}'...")
    with open(file_path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        header = next(reader) # Pula o cabeçalho
        
        for row in reader:
            entity_id = row[0]
            attributes = set()
            # Cria um atributo para cada coluna (exceto a primeira, que é o ID)
            for i, value in enumerate(row[1:]):
                attributes.add(Attribute(name=header[i+1], value=value))
            
            profile = EntityProfile(entity_id=entity_id, attributes=attributes, source_name=source_name)
            
            # Envia para o Kafka
            producer.send(topic, profile)
            print(f"  -> Enviado: {profile.entity_id}")
            time.sleep(0.1) # Simula um fluxo de dados
            
    producer.flush()
    print(f"✅ Dados de '{source_name}' enviados com sucesso!")

if __name__ == "__main__":
    KAFKA_TOPIC = 'entity_stream'
    
    # Caminhos para os datasets (ajuste conforme necessário)
    # Caminhos corrigidos, relativos à raiz do projeto
    DATASET_1_PATH = 'ER_Streaming/PRIME-F/datasets/Datasets/DblpAcm/DBLP2.csv'
    DATASET_2_PATH = 'ER_Streaming/PRIME-F/datasets/Datasets/DblpAcm/ACM.csv'

    kafka_producer = create_producer()

    # Altere o "source_name" para refletir os novos datasets
    stream_dataset(kafka_producer, KAFKA_TOPIC, DATASET_1_PATH, "DBLP")
    stream_dataset(kafka_producer, KAFKA_TOPIC, DATASET_2_PATH, "ACM")