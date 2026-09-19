import argparse
import csv
import math
import random
import statistics
import time
from collections import defaultdict

import mmh3
import pandas as pd
from scipy import stats


def str_to_MinHash(texto, q, seed=0):
    return min(mmh3.hash(texto[i:i + q], seed) for i in range(len(texto) - q + 1))


def preparar_registros(dataframe, coluna_id, colunas_atributos):
    colunas = [coluna_id, *colunas_atributos]
    return [
        (registro[0], (" " + " ".join(map(str, registro[1:]))).lower())
        for registro in dataframe[colunas].itertuples(index=False, name=None)
    ]


def load_dataset(dataset_type="dblp"):
    if dataset_type.lower() == "dblp":
        df1 = pd.read_csv("./00-datasets/DBLP.csv", encoding="utf-8", keep_default_na=False)
        df2 = pd.read_csv("./00-datasets/Scholar.csv", encoding="utf-8", keep_default_na=False)
        truth = pd.read_csv("./00-datasets/truth.csv", encoding="utf-8", keep_default_na=False)
        id_a = "idDBLP"
        id_b = "idScholar"
        tp_total = 5347
        columns = ["id", "id", "title", "authors"]
        print("Dataset carregado: DBLP + Scholar")
    elif dataset_type.lower() == "ncvoter":
        df1 = pd.read_csv("./00-datasets/ncvoter42.csv", encoding="utf-8", keep_default_na=False)
        df2 = pd.read_csv("./00-datasets/perturbed_recordsNC_voter.csv", encoding="utf-8", keep_default_na=False)
        truth = pd.read_csv("./00-datasets/ground_truthNC_voter.csv", encoding="utf-8", keep_default_na=False)
        id_a = "antigoId"
        id_b = "novoId"
        tp_total = 40893
        columns = ["ncid", "id", "first_name", "last_name", "registr_dt", "age_at_year_end"]
        print("Dataset carregado: NCVoter1 + NCVoter2")
    elif dataset_type.lower() == "musicbrainz":
        df1 = pd.read_csv("./00-datasets/musicbrainz-200-A01.csv.dapo", encoding="utf-8", keep_default_na=False)
        df2 = pd.read_csv("./00-datasets/music_brainz-simple-mutated.csv", encoding="utf-8", keep_default_na=False)
        truth = pd.read_csv("./00-datasets/ground_truth_music_brainz.csv", encoding="utf-8", keep_default_na=False)
        id_a = "antigoidmusic2"
        id_b = "novoidmusic1"
        tp_total = len(truth)
        columns = ["TID", "TID2", "title", "length", "artist", "album"]
        print("Dataset carregado: Musicbrainz + Musicbrainz")
    else:
        raise ValueError(f"Dataset desconhecido: {dataset_type}. Use 'dblp', 'ncvoter' ou 'musicbrainz'")

    return df1, df2, truth, id_a, id_b, tp_total, columns


def preparar_experimento(dataset_type):
    df1, df2, truth, id_a, id_b, tp_total, columns = load_dataset(dataset_type)
    atributos = columns[2:]
    registros_bloqueio = preparar_registros(df1, columns[0], atributos)
    registros_matching = preparar_registros(df2, columns[1], atributos)
    truth_por_id = defaultdict(set)

    for id_origem, id_destino in truth[[id_a, id_b]].itertuples(index=False, name=None):
        truth_por_id[id_origem].add(id_destino)

    return registros_bloqueio, registros_matching, truth_por_id, tp_total


def matching(registros_matching, inicio, tamanho_lote, blocos, truth_por_id, L, L1, q, limite):
    tp = fp = pairs_no = 0
    fim = min(inicio + tamanho_lote, len(registros_matching))
    randrange = random.randrange

    for indice_registro in range(inicio, fim):
        id_destino, texto = registros_matching[indice_registro]
        contagens = {}
        candidatos = set()

        for _ in range(L1):
            indice_hash = randrange(L)
            chave = str_to_MinHash(texto, q, indice_hash)
            for id_origem in blocos[indice_hash].get(chave, ()):
                contagem = contagens.get(id_origem, 0) + 1
                contagens[id_origem] = contagem
                if contagem == limite:
                    candidatos.add(id_origem)

        for id_origem in candidatos:
            pairs_no += 1
            if id_destino in truth_por_id.get(id_origem, ()):
                tp += 1
            else:
                fp += 1

    return tp, fp, pairs_no, fim < inicio + tamanho_lote


def executar_uma_replica(dataset_type, seed_valor, experimento=None):
    """Executa uma replica sem descartar entidades dos blocos."""
    random.seed(seed_valor)
    if experimento is None:
        experimento = preparar_experimento(dataset_type)
    registros_bloqueio, registros_matching, truth_por_id, tp_total = experimento

    t = 0.5
    eps = 0.1
    delta = 0.1
    L = math.ceil(math.log(1 / delta) / (2 * eps**2))
    L1 = int(1 / (2 * 0.01))
    q = 2
    limite = math.ceil(t * L1)
    blocos = [{} for _ in range(L)]
    tp = fp = pairs_no = 0
    nbS = naS = 1
    offset_a = offset_b = 50
    blocking_time = matching_time = 0.0

    while True:
        inicio = time.perf_counter()
        fim_bloqueio = min(naS + offset_a, len(registros_bloqueio))
        for indice_registro in range(naS, fim_bloqueio):
            id_origem, texto = registros_bloqueio[indice_registro]
            for indice_hash in range(L):
                chave = str_to_MinHash(texto, q, indice_hash)
                blocos[indice_hash].setdefault(chave, []).append(id_origem)
        blocking_time += time.perf_counter() - inicio

        inicio = time.perf_counter()
        novos_tp, novos_fp, novos_pares, terminou = matching(
            registros_matching, nbS, offset_b, blocos, truth_por_id, L, L1, q, limite
        )
        matching_time += time.perf_counter() - inicio
        tp += novos_tp
        fp += novos_fp
        pairs_no += novos_pares
        if terminou:
            break

        nbS += offset_b
        naS += offset_a

    return {
        "blocking_s": blocking_time,
        "matching_s": matching_time,
        "tp": tp,
        "fp": fp,
        "pairs_no": pairs_no,
        "recall": tp / tp_total if tp_total > 0 else 0.0,
        "precision": tp / (tp + fp) if tp + fp > 0 else 0.0,
    }


def executar_repeticoes(dataset_type, num_repeticoes, seed_inicial):
    experimento = preparar_experimento(dataset_type)
    resultados = []
    print(f"\n{'=' * 70}")
    print(f"Executando {num_repeticoes} replicas sem descarte para {dataset_type}...")
    print(f"{'=' * 70}")

    for replica in range(num_repeticoes):
        seed = seed_inicial + replica
        print(f"Replica {replica + 1}/{num_repeticoes} (seed={seed})...", end="", flush=True)
        resultado = executar_uma_replica(dataset_type, seed, experimento)
        resultado["replica"] = replica + 1
        resultado["seed"] = seed
        resultados.append(resultado)
        print(" OK")

    metricas = ["blocking_s", "matching_s", "tp", "fp", "pairs_no", "recall", "precision"]
    print(f"\n{'=' * 70}")
    print(f"Resumo Estatistico: {dataset_type.upper()} (n={num_repeticoes}, IC=95%)")
    print(f"{'=' * 70}")
    print(f"{'Metrica':<15} {'Media':<13} {'Desvio Pad':<13} {'IC Inferior':<13} {'IC Superior':<13}")
    print("-" * 75)
    for metrica in metricas:
        valores = [resultado[metrica] for resultado in resultados]
        media = statistics.mean(valores)
        desvio = statistics.stdev(valores) if num_repeticoes > 1 else 0.0
        erro = stats.t.ppf(0.975, num_repeticoes - 1) * desvio / math.sqrt(num_repeticoes) if num_repeticoes > 1 else 0.0
        print(f"{metrica:<15} {media:<13.6f} {desvio:<13.6f} {media - erro:<13.6f} {media + erro:<13.6f}")
    print(f"{'=' * 70}\n")
    return resultados


def salvar_csv(resultados, caminho):
    if not resultados:
        return
    with open(caminho, "w", newline="", encoding="utf-8") as arquivo:
        escritor = csv.DictWriter(arquivo, fieldnames=resultados[0].keys())
        escritor.writeheader()
        escritor.writerows(resultados)
    print(f"Resultados salvos em: {caminho}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Executa o experimento de bloqueio aproximado sem descarte.")
    parser.add_argument("-d", "--dataset-type", choices=["dblp", "ncvoter", "musicbrainz"], default="dblp")
    parser.add_argument("-r", "--repeticoes", type=int, default=10)
    parser.add_argument("-s", "--seed", type=int, default=20260907)
    parser.add_argument("-o", "--saida-csv")
    args = parser.parse_args()

    if args.repeticoes > 1:
        resultados = executar_repeticoes(args.dataset_type, args.repeticoes, args.seed)
        if args.saida_csv:
            salvar_csv(resultados, args.saida_csv)
    else:
        resultado = executar_uma_replica(args.dataset_type, args.seed)
        print(f"\nblocking time (in secs) {resultado['blocking_s']:.4f}")
        print(f"matching time (in secs) {resultado['matching_s']:.4f}")
        print(f"TP= {resultado['tp']} Recall= {resultado['recall']:.4f} Precision= {resultado['precision']:.4f} pairsNo= {resultado['pairs_no']}")