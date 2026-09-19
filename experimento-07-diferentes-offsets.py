import argparse
import csv
import math
import random
import statistics
import time
from collections import defaultdict, deque

import mmh3
import pandas as pd
from scipy import stats


OFFSETS_PADRAO = (1000, 3000, 9000, 27000)


def str_to_minhash(texto, q, seed):
    return min(mmh3.hash(texto[indice:indice + q], seed) for indice in range(len(texto) - q + 1))


def preparar_registros(dataframe, coluna_id, colunas_atributos):
    colunas = [coluna_id, *colunas_atributos]
    return [
        (registro[0], (" " + " ".join(map(str, registro[1:]))).lower())
        for registro in dataframe[colunas].itertuples(index=False, name=None)
    ]


def load_dataset(dataset_type):
    if dataset_type == "dblp":
        df1 = pd.read_csv("./00-datasets/DBLP.csv", encoding="utf-8", keep_default_na=False)
        df2 = pd.read_csv("./00-datasets/Scholar.csv", encoding="utf-8", keep_default_na=False)
        truth = pd.read_csv("./00-datasets/truth.csv", encoding="utf-8", keep_default_na=False)
        id_a, id_b, tp_total = "idDBLP", "idScholar", 5347
        columns = ["id", "id", "title", "authors"]
    elif dataset_type == "ncvoter":
        df1 = pd.read_csv("./00-datasets/ncvoter42.csv", encoding="utf-8", keep_default_na=False)
        df2 = pd.read_csv("./00-datasets/perturbed_recordsNC_voter.csv", encoding="utf-8", keep_default_na=False)
        truth = pd.read_csv("./00-datasets/ground_truthNC_voter.csv", encoding="utf-8", keep_default_na=False)
        id_a, id_b, tp_total = "antigoId", "novoId", 40893
        columns = ["ncid", "id", "first_name", "last_name", "registr_dt", "age_at_year_end"]
    elif dataset_type == "musicbrainz":
        df1 = pd.read_csv("./00-datasets/musicbrainz-200-A01.csv.dapo", encoding="utf-8", keep_default_na=False)
        df2 = pd.read_csv("./00-datasets/music_brainz-simple-mutated.csv", encoding="utf-8", keep_default_na=False)
        truth = pd.read_csv("./00-datasets/ground_truth_music_brainz.csv", encoding="utf-8", keep_default_na=False)
        id_a, id_b, tp_total = "antigoidmusic2", "novoidmusic1", len(truth)
        columns = ["TID", "TID2", "title", "length", "artist", "album"]
    else:
        raise ValueError("Dataset desconhecido. Use 'dblp', 'ncvoter' ou 'musicbrainz'.")

    print(f"Dataset carregado: {dataset_type}")
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


def matching(registros_matching, inicio, tamanho_lote, blocos, truth_por_id, quantidade_hashes, amostras_hash, q, limite):
    tp = fp = pairs_no = 0
    fim = min(inicio + tamanho_lote, len(registros_matching))
    randrange = random.randrange

    for indice_registro in range(inicio, fim):
        id_destino, texto = registros_matching[indice_registro]
        contagens = {}
        candidatos = set()
        for _ in range(amostras_hash):
            indice_hash = randrange(quantidade_hashes)
            chave = str_to_minhash(texto, q, indice_hash)
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


def executar_uma_replica(experimento, offset_a, offset_b, seed, tamanho_bloco=1000):
    """Executa uma réplica com descarte integral quando um bloco alcança a capacidade."""
    random.seed(seed)
    registros_bloqueio, registros_matching, truth_por_id, tp_total = experimento
    limiar = 0.5
    epsilon = 0.1
    delta = 0.1
    quantidade_hashes = math.ceil(math.log(1 / delta) / (2 * epsilon**2))
    amostras_hash = int(1 / (2 * 0.01))
    q = 2
    limite = math.ceil(limiar * amostras_hash)
    blocos = [{} for _ in range(quantidade_hashes)]
    tp = fp = pairs_no = 0
    inicio_bloqueio = inicio_matching = 1
    blocking_time = matching_time = 0.0

    while True:
        inicio_tempo = time.perf_counter()
        fim_bloqueio = min(inicio_bloqueio + offset_a, len(registros_bloqueio))
        for indice_registro in range(inicio_bloqueio, fim_bloqueio):
            id_origem, texto = registros_bloqueio[indice_registro]
            for indice_hash in range(quantidade_hashes):
                chave = str_to_minhash(texto, q, indice_hash)
                bloco = blocos[indice_hash].get(chave)
                if bloco is None:
                    bloco = blocos[indice_hash][chave] = deque()
                elif len(bloco) >= tamanho_bloco:
                    bloco.clear()
                bloco.append(id_origem)
        blocking_time += time.perf_counter() - inicio_tempo

        inicio_tempo = time.perf_counter()
        novos_tp, novos_fp, novos_pares, terminou = matching(
            registros_matching,
            inicio_matching,
            offset_b,
            blocos,
            truth_por_id,
            quantidade_hashes,
            amostras_hash,
            q,
            limite,
        )
        matching_time += time.perf_counter() - inicio_tempo
        tp += novos_tp
        fp += novos_fp
        pairs_no += novos_pares
        if terminou:
            break

        inicio_bloqueio += offset_a
        inicio_matching += offset_b

    return {
        "offset_a": offset_a,
        "offset_b": offset_b,
        "blocking_s": blocking_time,
        "matching_s": matching_time,
        "tp": tp,
        "fp": fp,
        "pairs_no": pairs_no,
        "recall": tp / tp_total if tp_total else 0.0,
        "precision": tp / (tp + fp) if tp + fp else 0.0,
    }


def parse_offsets(texto):
    valores = tuple(int(valor.strip()) for valor in texto.split(",") if valor.strip())
    if not valores or any(valor <= 0 for valor in valores):
        raise argparse.ArgumentTypeError("Offsets devem ser inteiros positivos separados por vírgula.")
    return valores


def pares_de_offsets(offsets_a, offsets_b):
    if len(offsets_a) == len(offsets_b):
        return tuple(zip(offsets_a, offsets_b))
    if len(offsets_a) == 1:
        return tuple((offsets_a[0], offset_b) for offset_b in offsets_b)
    if len(offsets_b) == 1:
        return tuple((offset_a, offsets_b[0]) for offset_a in offsets_a)
    raise ValueError("Forneça listas de mesmo tamanho ou uma lista com um único offset para comparar contra a outra.")


def imprimir_resumo(resultados):
    metricas = ("blocking_s", "matching_s", "tp", "fp", "pairs_no", "recall", "precision")
    for offset_a, offset_b in sorted({(r["offset_a"], r["offset_b"]) for r in resultados}):
        grupo = [r for r in resultados if r["offset_a"] == offset_a and r["offset_b"] == offset_b]
        print(f"\nOffset A={offset_a}, Offset B={offset_b}, n={len(grupo)}")
        for metrica in metricas:
            valores = [r[metrica] for r in grupo]
            media = statistics.mean(valores)
            desvio = statistics.stdev(valores) if len(valores) > 1 else 0.0
            erro = stats.t.ppf(0.975, len(valores) - 1) * desvio / math.sqrt(len(valores)) if len(valores) > 1 else 0.0
            print(f"{metrica:<12} media={media:.6f} IC95%=[{media - erro:.6f}, {media + erro:.6f}]")


def salvar_csv(resultados, caminho):
    with open(caminho, "w", newline="", encoding="utf-8") as arquivo:
        escritor = csv.DictWriter(arquivo, fieldnames=resultados[0].keys(), lineterminator="\n")
        escritor.writeheader()
        escritor.writerows(resultados)
    print(f"Resultados salvos em: {caminho}")


def main():
    parser = argparse.ArgumentParser(description="Mede o impacto de diferentes offsets de bloqueio e matching.")
    parser.add_argument("-d", "--dataset-type", choices=("dblp", "ncvoter", "musicbrainz"), default="ncvoter")
    parser.add_argument("-r", "--repeticoes", type=int, default=10)
    parser.add_argument("-s", "--seed", type=int, default=20260907)
    parser.add_argument("--offset-a", type=parse_offsets, default=OFFSETS_PADRAO, help="Lista de offsets do bloqueio. Padrão: 1000,3000,9000,27000.")
    parser.add_argument("--offset-b", type=parse_offsets, default=OFFSETS_PADRAO, help="Lista de offsets do matching. Padrão: 1000,3000,9000,27000.")
    parser.add_argument("-w", "--tamanho-bloco", type=int, default=1000)
    parser.add_argument("-o", "--saida-csv", default="resultados-exp-07-diferentes-offsets.csv")
    args = parser.parse_args()

    if args.repeticoes < 1 or args.tamanho_bloco < 1:
        parser.error("Repetições e tamanho do bloco devem ser positivos.")

    try:
        configuracoes = pares_de_offsets(args.offset_a, args.offset_b)
    except ValueError as erro:
        parser.error(str(erro))

    experimento = preparar_experimento(args.dataset_type)
    resultados = []
    for offset_a, offset_b in configuracoes:
        print(f"\nOffset A={offset_a}, Offset B={offset_b}")
        for replica in range(args.repeticoes):
            seed = args.seed + replica
            resultado = executar_uma_replica(experimento, offset_a, offset_b, seed, args.tamanho_bloco)
            resultado["replica"] = replica + 1
            resultado["seed"] = seed
            resultados.append(resultado)
            print(f"Replica {replica + 1}/{args.repeticoes} concluida")

    salvar_csv(resultados, args.saida_csv)
    imprimir_resumo(resultados)


if __name__ == "__main__":
    main()
