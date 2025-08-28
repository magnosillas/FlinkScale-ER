# src/evaluate.py
import sys
import csv
import os

def load_ground_truth(file_path):
    """Carrega o gabarito. Espera um CSV com id1,id2."""
    matches = set()
    print(f"Carregando gabarito de {file_path}...")
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            next(reader)  # Pula o cabeçalho
            for row in reader:
                # Normaliza a ordem para (menor, maior) para evitar duplicatas como (a,b) e (b,a)
                id1, id2 = sorted((f"dataset1_abt_{row[0]}", f"dataset2_buy_{row[1]}"))
                matches.add((id1, id2))
        print(f"Gabarito carregado: {len(matches)} pares verdadeiros.")
        return matches
    except FileNotFoundError:
        print(f"ERRO: Arquivo de gabarito não encontrado em {file_path}")
        exit()


def load_flink_results(folder_path):
    """Carrega os resultados do Flink de uma pasta. Extrai os pares de IDs."""
    matches = set()
    print(f"Carregando resultados do Flink da pasta: {folder_path}...")
    try:
        for filename in os.listdir(folder_path):
            if filename.endswith(".txt"):
                with open(os.path.join(folder_path, filename), 'r', encoding='utf-8') as f:
                    for line in f:
                        if "POTENCIAL DUPLICATA ENCONTRADA" in line:
                            parts = line.split("ID ")
                            id1 = parts[1].split(" <->")[0]
                            id2 = parts[2].split(" (Similaridade")[0]
                            # Normaliza a ordem
                            id1, id2 = sorted((id1, id2))
                            matches.add((id1, id2))
        print(f"Resultados do Flink carregados: {len(matches)} pares encontrados.")
        return matches
    except FileNotFoundError:
        print(f"ERRO: Pasta de resultados do Flink não encontrada em '{folder_path}'")
        exit()

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("\nUso: python src/evaluate.py <caminho_para_pasta_de_resultados> <caminho_para_groundtruth.csv>")
        exit()

    flink_results_folder = sys.argv[1]
    ground_truth_path = sys.argv[2]

    ground_truth_pairs = load_ground_truth(ground_truth_path)
    flink_pairs = load_flink_results(flink_results_folder)

    # --- CÁLCULO DAS MÉTRICAS ---
    true_positives = len(flink_pairs.intersection(ground_truth_pairs))
    false_positives = len(flink_pairs - ground_truth_pairs)
    false_negatives = len(ground_truth_pairs - flink_pairs)

    # Precisão: Dos pares que encontramos, quantos estavam corretos?
    precision = true_positives / (true_positives + false_positives) if (true_positives + false_positives) > 0 else 0

    # Recall: Dos pares corretos que existiam, quantos nós encontramos?
    recall = true_positives / (true_positives + false_negatives) if (true_positives + false_negatives) > 0 else 0

    # F1-Score: A média harmônica entre os dois.
    f1_score = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0

    print("\n--- RESULTADOS DA AVALIAÇÃO ---")
    print(f"Pares Corretos Encontrados (True Positives):  {true_positives}")
    print(f"Pares Errados Encontrados (False Positives): {false_positives}")
    print(f"Pares Corretos Perdidos (False Negatives): {false_negatives}")
    print("---------------------------------")
    print(f"Precisão (Precision): {precision:.2%}")
    print(f"Recall:               {recall:.2%}")
    print(f"F1-Score:             {f1_score:.2%}")
    print("---------------------------------")