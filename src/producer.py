# producer.py (versão final com argumentos de linha de comando)

import json
import time
import sys  # Importa o módulo para ler argumentos da linha de comando
from kafka import KafkaProducer
from data_structures import EntityProfile, Attribute

# --- CONFIGURAÇÕES ---
KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'entity_stream'
# ---------------------

def create_kafka_producer(broker_url):
    """Cria e retorna um produtor Kafka conectado ao broker."""
    print(f"Conectando ao Kafka em {broker_url}...")
    try:
        producer = KafkaProducer(
            bootstrap_servers=[broker_url],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print("Produtor Kafka conectado com sucesso!")
        return producer
    except Exception as e:
        print(f"Erro ao conectar ao Kafka: {e}")
        exit()

def read_and_send_data(producer, topic, file_path, id_prefix, is_source_dataset):
    """Lê perfis de entidade de um arquivo JSON e envia para o Kafka."""
    print(f"Lendo dados de: {file_path}")
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        total_entities = len(data)
        print(f"{total_entities} entidades encontradas. Iniciando o envio...")

        for i, entity_dict in enumerate(data):
            original_id = entity_dict.get("entityUrl", f"unknown_{i}")
            attributes = [Attribute(attr['name'], attr['value']) for attr in entity_dict.get('attributes', [])]

            profile = EntityProfile(
                entity_id=f"{id_prefix}_{original_id}", # Adiciona o prefixo ao ID
                attributes=attributes,
                is_source=is_source_dataset, # Marca se é da primeira ou segunda fonte
                increment_id=entity_dict.get("incrementID", 0)
            )

            producer.send(topic, value=profile.to_dict())

        producer.flush()
        print(f"Envio de {total_entities} entidades de '{file_path}' concluído!")

    except FileNotFoundError:
        print(f"ERRO: O arquivo de entrada não foi encontrado em '{file_path}'")
        exit()
    except Exception as e:
        print(f"Ocorreu um erro durante a leitura ou envio: {e}")
        exit()


if __name__ == "__main__":
    # Verifica se os dois caminhos de arquivo foram passados como argumento
    if len(sys.argv) != 3:
        print("\nERRO: Uso incorreto.")
        print("Por favor, forneça os caminhos para os dois arquivos JSON que deseja comparar.")
        print(r"Exemplo: python src/producer.py data/converted/abt.json data/converted/buy.json")
        exit()

    # Pega os caminhos dos argumentos da linha de comando
    file_path_1 = sys.argv[1]
    file_path_2 = sys.argv[2]
    
    # Extrai prefixos dos nomes dos arquivos (ex: 'abt', 'buy')
    prefix_1 = file_path_1.split('/')[-1].split('.')[0]
    prefix_2 = file_path_2.split('/')[-1].split('.')[0]

    kafka_producer = create_kafka_producer(KAFKA_BROKER)

    # Envia o primeiro dataset
    read_and_send_data(kafka_producer, KAFKA_TOPIC, file_path_1, id_prefix=prefix_1, is_source_dataset=True)
    
    # Envia o segundo dataset
    read_and_send_data(kafka_producer, KAFKA_TOPIC, file_path_2, id_prefix=prefix_2, is_source_dataset=False)
    
    print("\nTodos os dados foram enviados com sucesso!")