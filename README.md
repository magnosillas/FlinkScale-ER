# Resolução de Entidades em Streaming com Kafka e PyFlink

Este projeto é uma migração para Python de um sistema de Resolução de Entidades (ER) originalmente desenvolvido em Java. O objetivo é identificar, em tempo real, registros de diferentes fontes de dados que se referem à mesma entidade do mundo real (duplicatas).

A arquitetura utiliza um pipeline de processamento de dados em streaming com tecnologias de Big Data, incluindo Apache Kafka como sistema de mensageria e Apache Flink para o processamento distribuído.

## Como o Algoritmo Funciona

O sistema opera em um fluxo contínuo, projetado para processar dados à medida que eles chegam. A arquitetura pode ser dividida em três grandes etapas:

### 1. Produtor de Dados (`producer.py`)

- **Responsabilidade**: Ler os dados de fontes externas (arquivos JSON) e enviá-los para o sistema.
- **Funcionamento**: O script `producer.py` é responsável por ler os arquivos de dados, criar um "perfil de entidade" para cada registro e enviar esses perfis como mensagens para um tópico no Apache Kafka. Ele aceita dois arquivos de dados como entrada para que possamos comparar duas fontes distintas (ex: produtos da loja A vs. produtos da loja B).

### 2. Fila de Mensagens (Apache Kafka)

- **Responsabilidade**: Atuar como um intermediário robusto e escalável entre o produtor e o processador.
- **Funcionamento**: Kafka recebe as mensagens do produtor e as armazena em um tópico chamado `entity_stream`. Ele garante que os dados não se percam e os disponibiliza para serem consumidos pelo Flink, permitindo o processamento de um fluxo contínuo.

### 3. Processador em Streaming (`flink_processor.py`)

Este é o cérebro do sistema. Ele consome os dados do Kafka e executa um pipeline de Resolução de Entidades usando a API do PyFlink. O processo é dividido em várias etapas:

- **A. Tokenização (Blocking)**: Para evitar a comparação de todos os registros entre si, aplicamos uma técnica de bloqueio. A função `tokenize` extrai palavras-chave (tokens) dos atributos de cada entidade. Atualmente, ela também remove "stopwords" (palavras comuns como "o", "a", "de") usando a biblioteca NLTK para focar apenas nos termos mais significativos.

- **B. Agrupamento (`key_by`)**: O Flink agrupa todas as entidades que compartilham o mesmo token. Isso cria "blocos" de entidades que são candidatas a serem duplicatas, reduzindo drasticamente o número de comparações necessárias.

- **C. Janelamento (`window`)**: Para processar os dados em lotes lógicos, o Flink agrupa os blocos que chegam em "janelas de tempo" de 15 segundos. Ao final de cada janela, o processamento daquele lote é disparado.

- **D. Comparação e Classificação**: Dentro de cada bloco, a classe `FindDuplicates` compara cada par de entidades. Ela calcula a **Similaridade Jaccard** entre os conjuntos de tokens de cada entidade. Se a similaridade for maior que um limiar pré-definido (atualmente `0.7`), o par é classificado como uma duplicata em potencial e o resultado é salvo em um arquivo de texto.

## Estrutura do Projeto
```bash
/dynamic-entity-blocking
|
├── data/
│   └── converted/              # Onde os arquivos de dados .json devem ser colocados
│       └── AbtBuy/
│           ├── dataset1_abt.json
│           └── dataset2_buy.json
├── jars/
│   └── ...kafka-connector.jar  # Biblioteca para o Flink se comunicar com o Kafka
│
├── src/
│   ├── data_structures.py      # Define as classes de dados (EntityProfile, Attribute)
│   ├── producer.py             # Script para enviar dados para o Kafka
│   ├── flink_processor.py      # Script com a lógica de processamento do Flink
│   └── evaluate.py             # Script para avaliar a precisão dos resultados
│
├── Dockerfile.flink            # Define como construir a imagem Docker do Flink
├── docker-compose.yml          # Orquestra todos os serviços (Kafka, Zookeeper, Flink)
└── requirements.txt            # Lista de dependências Python do projeto
```
## Guia de Execução Completo

Siga estes passos para configurar e rodar o projeto do zero.

### Pré-requisitos

- Docker e Docker Compose instalados.
- Python 3.8+ e `pip` instalados.
- Um clone deste repositório.

### 1. Configuração do Ambiente

Primeiro, prepare o ambiente Python.


# Crie e ative um ambiente virtual
python -m venv venv
source venv/bin/activate  # No Linux/macOS
.\venv\Scripts\Activate.ps1 # No Windows PowerShell

# Instale as dependências
pip install -r requirements.txt

### 2. Iniciar a Infraestrutura Docker
Este comando irá construir as imagens customizadas do Flink (instalando Python e as dependências) e iniciar todos os contêineres necessários em segundo plano.
```bash
docker-compose up -d --build
```
### 3. Enviar Dados para o Kafka
Antes de processar, precisamos popular o Kafka com os dados.

a. (Opcional) Limpar o Tópico Kafka:
Se você já rodou o produtor antes, pode limpar os dados antigos para começar do zero.

```bash
docker exec -it kafka /bin/kafka-topics --bootstrap-server kafka:9092 --delete --topic entity_stream
```
b. Executar o Produtor:
Passe os caminhos dos dois arquivos JSON que você deseja comparar como argumentos.

```bash
# Exemplo para comparar os datasets Abt e Buy
python src/producer.py data/converted/AbtBuy/dataset1_abt.json data/converted/AbtBuy/dataset2_buy.json
```
Espere até a mensagem "Todos os dados foram enviados com sucesso!" aparecer.

### 4. Iniciar o Processamento com Flink
Agora, submeta o script de processamento para o cluster Flink que está rodando no Docker.

```bash
docker exec -it flink_jobmanager /opt/flink/bin/flink run -py /app/src/flink_processor.py
```
O job será submetido e começará a processar os dados.

### 5. Coletar e Avaliar os Resultados
O job do Flink irá salvar os pares de duplicatas encontrados em arquivos de texto dentro do contêiner do taskmanager.

a. Acompanhar os Logs (opcional):
Para ver os resultados sendo gerados em tempo real, você pode monitorar os logs do taskmanager.
```bash
docker logs -f flink_taskmanager
```

b. Copiar os Resultados para sua Máquina:
Após o término do job (ou quando você o cancelar), copie os arquivos de resultado do contêiner.

```bash
# Crie uma pasta local para os resultados
mkdir flink-output

# Copie o conteúdo da pasta de output do container para a sua pasta local
docker cp flink_taskmanager:/app/output ./flink-output/
```

c. Executar o Script de Avaliação:
Use o script evaluate.py para comparar os resultados do Flink com um arquivo de "gabarito" (ground truth) e calcular as métricas de precisão

```bash
# Exemplo para o par Abt-Buy
python src/evaluate.py ./flink-output/output data/groundtruth/abt_buy_groundtruth.csv
```
O script irá imprimir a Precisão, o Recall e o F1-Score do seu algoritmo.








