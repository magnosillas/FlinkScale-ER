# flink_processor.py (com remoção de stopwords)
import json
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.window import TumblingProcessingTimeWindows
from pyflink.common.time import Time
from pyflink.datastream.functions import ProcessWindowFunction
from pyflink.datastream.connectors.file_system import FileSink, OutputFileConfig, RollingPolicy
from pyflink.common.serialization import SimpleStringEncoder

from itertools import combinations
# --- NOVOS IMPORTS ---
import nltk
from nltk.corpus import stopwords
import re

# --- CONFIGURAÇÕES ---
KAFKA_BROKER = 'kafka:29092'
KAFKA_TOPIC = 'entity_stream'
# ---------------------

# Carrega a lista de stopwords em português uma única vez
nltk.data.path.append('/app/nltk_data') # Garante que o NLTK encontre os dados no container
STOP_WORDS = set(stopwords.words('portuguese'))

def jaccard_similarity(set1, set2):
    intersection = len(set1.intersection(set2))
    union = len(set1.union(set2))
    return intersection / union if union != 0 else 0

class FindDuplicates(ProcessWindowFunction):
    def process(self, key, context, elements):
        unique_entities = {item[1] for item in elements}
        entities = [json.loads(s) for s in unique_entities]

        if len(entities) < 2:
            return

        for entity1, entity2 in combinations(entities, 2):
            tokens1 = set(entity1.get('tokens', []))
            tokens2 = set(entity2.get('tokens', []))
            sim = jaccard_similarity(tokens1, tokens2)

            if sim > 0.7:
                id1 = entity1.get('entity_id')
                id2 = entity2.get('entity_id')
                yield f"POTENCIAL DUPLICATA ENCONTRADA: ID {id1} <-> ID {id2} (Similaridade: {sim:.2f})"

def process_er_streaming():
    env = StreamExecutionEnvironment.get_execution_environment()
    print("Ambiente Flink configurado.")

    kafka_props = {
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': 'er_flink_consumer_group'
    }
    kafka_consumer = FlinkKafkaConsumer(
        topics=KAFKA_TOPIC,
        deserialization_schema=SimpleStringSchema(),
        properties=kafka_props
    )
    kafka_consumer.set_start_from_earliest()
    print(f"Consumidor Kafka configurado para o tópico '{KAFKA_TOPIC}'.")

    data_stream = env.add_source(kafka_consumer)

    # --- LÓGICA DE TOKENIZAÇÃO ATUALIZADA ---
    def tokenize(json_string):
        try:
            entity = json.loads(json_string)
            full_text = " ".join([attr['value'] for attr in entity.get('attributes', [])])
            
            # 1. Deixa tudo minúsculo e remove pontuação
            clean_text = re.sub(r'[^\w\s]', '', full_text.lower())
            
            # 2. Quebra o texto em palavras
            words = clean_text.split()
            
            # 3. Remove as stopwords
            tokens = {word for word in words if word not in STOP_WORDS and len(word) > 1}
            
            entity['tokens'] = list(tokens)

            for token in tokens:
                yield (token, json.dumps(entity))
        except json.JSONDecodeError:
            pass

    tokenized_stream = data_stream.flat_map(tokenize)
    keyed_stream = tokenized_stream.key_by(lambda x: x[0])
    windowed_stream = keyed_stream.window(TumblingProcessingTimeWindows.of(Time.seconds(15)))
    result_stream = windowed_stream.process(FindDuplicates())
    
        # --- MUDANÇA AQUI ---
    # 4. Salvar o resultado em arquivos de texto na pasta /app/output
    output_path = "/app/output"
    file_sink = FileSink.for_row_format(
        base_path=output_path,
        encoder=SimpleStringEncoder()
    ).with_output_file_config(
        OutputFileConfig.builder()
        .with_part_prefix("matches")
        .with_part_suffix(".txt")
        .build()
    ).build()

    result_stream.sink_to(file_sink)

    print("Iniciando o job PyFlink... Os resultados serão salvos na pasta /app/output do container taskmanager.")

if __name__ == '__main__':
    process_er_streaming()