from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.datastream.connectors import FlinkKafkaConsumer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream.window import TumblingProcessingTimeWindows
from pyflink.common.time import Time
import json
import re
from itertools import product

# Importa as classes de dados que definimos
from er_models import EntityProfile, Attribute

# ======================================
# Lógica de Blocagem inspirada no artigo
# ======================================

def entity_resolution_pipeline(env):
    """Define o pipeline de ER com PyFlink."""
    
    kafka_props = {
        'bootstrap.servers': 'kafka:9092',
        'group.id': 'er_flink_group',
        'auto.offset.reset': 'earliest' # Garante que o Flink leia desde o início do tópico
    }
    kafka_topic = 'entity_stream'
    
    kafka_source = FlinkKafkaConsumer(
        topics=kafka_topic,
        deserialization_schema=SimpleStringSchema(),
        properties=kafka_props
    )

    # 1. Fonte de Dados: Lê do Kafka e converte a string JSON em objetos EntityProfile
    data_stream = env.add_source(kafka_source) \
        .map(deserialize_entity, output_type=Types.PICKLED_BYTE_ARRAY())

    # 2. Tokenização (Token Extraction)
    # Para cada entidade, emite múltiplos pares (token, entidade)
    token_stream = data_stream.flat_map(token_extraction, output_type=Types.TUPLE([Types.STRING(), Types.PICKLED_BYTE_ARRAY()]))

    # 3. Geração de Blocos e Comparação (Block Generation & Pruning)
    # Agrupa por token (chave do bloco) e aplica uma janela de tempo
    results_stream = token_stream \
        .key_by(lambda x: x[0]) \
        .window(TumblingProcessingTimeWindows.of(Time.seconds(30))) \
        .apply(block_processing, result_type=Types.STRING()) # A saída é uma string formatada com os resultados

    # 4. Saída (Sink): Imprime os pares encontrados no log do Flink
    results_stream.print()

# ======================================
# Funções que definem a lógica do pipeline
# ======================================

def deserialize_entity(json_str: str) -> EntityProfile:
    """Converte uma string JSON de volta para um objeto EntityProfile."""
    data = json.loads(json_str)
    attributes = {Attribute(attr['name'], attr['value']) for attr in data['attributes']}
    return EntityProfile(
        entity_id=data['entity_id'],
        attributes=attributes,
        source_name=data['source_name']
    )

def token_extraction(profile: EntityProfile):
    """Recebe uma entidade e emite um par (token, profile) para cada token."""
    tokens = profile.get_tokens()
    for token in tokens:
        if len(token) > 2: # Ignora tokens muito pequenos que geram ruído
            yield token, profile

def block_processing(key, window, elements):
    """
    Função principal. Executada para cada bloco (definido por uma chave/token) dentro de uma janela de tempo.
    Aqui acontece a comparação e a filtragem (pruning).
    """
    blocking_key = key
    entities = [element[1] for element in elements] # Extrai os objetos EntityProfile

    source_1_entities = []
    source_2_entities = []

    # Separa as entidades por sua fonte de origem
    for entity in entities:
        if entity.source_name == "DBPedia_1":
            source_1_entities.append(entity)
        else:
            source_2_entities.append(entity)
            
    # Evita processamento desnecessário se um dos lados do bloco estiver vazio
    if not source_1_entities or not source_2_entities:
        return

    # Compara cada entidade da fonte 1 com cada entidade da fonte 2
    for entity1 in source_1_entities:
        for entity2 in source_2_entities:
            # Pega o conjunto de tokens de cada entidade para calcular a similaridade
            tokens1 = entity1.get_tokens()
            tokens2 = entity2.get_tokens()
            
            # Calcula a similaridade Jaccard
            sim = jaccard_similarity(tokens1, tokens2)
            
            # Pruning (Filtragem): Só considera pares com similaridade alta
            if sim > 0.5:
                yield (f"Match Found (in block '{blocking_key}'): "
                       f"{entity1.entity_id} ({entity1.source_name}) <-> {entity2.entity_id} ({entity2.source_name}) "
                       f"| Similarity: {sim:.2f}")


def jaccard_similarity(set1: Set[str], set2: Set[str]) -> float:
    """Calcula a similaridade Jaccard entre dois conjuntos de tokens."""
    intersection = len(set1.intersection(set2))
    union = len(set1.union(set2))
    return intersection / union if union > 0 else 0.0

if __name__ == '__main__':
    env = StreamExecutionEnvironment.get_execution_environment()
    # Define que estamos usando o tempo de processamento da máquina
    env.set_stream_time_characteristic(TimeCharacteristic.ProcessingTime)
    
    print("🚀 Iniciando o Job de Resolução de Entidades em PyFlink...")
    entity_resolution_pipeline(env)
    
    # Inicia a execução do job Flink
    env.execute("Python_ER_Job")