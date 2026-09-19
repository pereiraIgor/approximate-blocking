import pandas as pd
import time
import random
import math
import mmh3
import argparse
import csv
import statistics
from collections import defaultdict, deque
from scipy import stats

def str_to_MinHash(texto, q, seed=0):
    return min(mmh3.hash(texto[i:i + q], seed) for i in range(len(texto) - q + 1))

def preparar_registros(dataframe, coluna_id, colunas_atributos):
    colunas = [coluna_id, *colunas_atributos]
    return [
        (registro[0], (" " + " ".join(map(str, registro[1:]))).lower())
        for registro in dataframe[colunas].itertuples(index=False, name=None)
    ]

def preparar_experimento(dataset_type):
    df1, df2, truth, idA, idB, tp_total, columns = load_dataset(dataset_type)
    atributos = columns[2:]
    registros_bloqueio = preparar_registros(df1, columns[0], atributos)
    registros_matching = preparar_registros(df2, columns[1], atributos)
    truthD = defaultdict(set)

    for id_a, id_b in truth[[idA, idB]].itertuples(index=False, name=None):
        truthD[id_a].add(id_b)

    return registros_bloqueio, registros_matching, truthD, tp_total


def matching(registros_matching, inicio, tamanho_lote, dictB, truthD, L, L1, q, limite):
    tp = fp = pairs_no = 0
    fim = min(inicio + tamanho_lote, len(registros_matching))
    randrange = random.randrange

    for index2 in range(inicio, fim):
        id_scholar, srec = registros_matching[index2]
        contagens = {}
        matching_pairs = set()
        for _ in range(L1):
            indice_hash = randrange(L)
            chave = str_to_MinHash(srec, q, indice_hash)
            for id_dblp, _ in dictB[indice_hash].get(chave, ()):
                contagem = contagens.get(id_dblp, 0) + 1
                contagens[id_dblp] = contagem
                if contagem == limite:
                    matching_pairs.add(id_dblp)

        for id_dblp in matching_pairs:
            pairs_no += 1
            if id_scholar in truthD.get(id_dblp, ()):
                tp += 1
            else:
                fp += 1

    return tp, fp, pairs_no, fim < inicio + tamanho_lote

def eliminar_expirados(bloco, tempo_minimo):
    while bloco and bloco[0][1] < tempo_minimo:
        bloco.popleft()

def load_dataset(dataset_type="dblp"):
    if dataset_type.lower() == "dblp":
        df1 = pd.read_csv("./00-datasets/DBLP.csv", sep=",", encoding="utf-8", keep_default_na=False)
        df2 = pd.read_csv("./00-datasets/Scholar.csv", sep=",", encoding="utf-8", keep_default_na=False)
        truth = pd.read_csv("./00-datasets/truth.csv", sep=",", encoding="utf-8", keep_default_na=False)
        idA = "idDBLP"
        idB = "idScholar"
        tp = 5347
        columns = ["id","id", "title", "authors"]
        print("Dataset carregado: DBLP + Scholar")

    elif dataset_type.lower() == "ncvoter":
        df1 = pd.read_csv("./00-datasets/ncvoter42.csv", sep=",", encoding="utf-8", keep_default_na=False)
        df2 = pd.read_csv("./00-datasets/perturbed_recordsNC_voter.csv", sep=",", encoding="utf-8", keep_default_na=False)
        truth = pd.read_csv("./00-datasets/ground_truthNC_voter.csv", sep=",", encoding="utf-8", keep_default_na=False)
        idA = "antigoId"
        idB = "novoId"
        tp = 40893
        columns = ["ncid", "id", "first_name", "last_name", "registr_dt", "age_at_year_end"]
        print("Dataset carregado: NCVoter1 + NCVoter2")
       
    elif dataset_type.lower() == "musicbrainz":
        df1 = pd.read_csv("./00-datasets/musicbrainz-200-A01.csv.dapo", sep=",", encoding="utf-8", keep_default_na=False)
        df2 = pd.read_csv("./00-datasets/music_brainz-simple-mutated.csv", sep=",", encoding="utf-8", keep_default_na=False)
        truth = pd.read_csv("./00-datasets/ground_truth_music_brainz.csv", sep=",", encoding="utf-8", keep_default_na=False)
        idA = "antigoidmusic2"
        idB = "novoidmusic1"
        tp = len(truth) #193471

        columns = ["TID", "TID2","title", "length", "artist", "album"]
        print("Dataset carregado: Musicbrainz + Musicbrainz")
    
    else:
        raise ValueError(f"Dataset desconhecido: {dataset_type}. Use 'dblp', 'ncvoter' ou 'musicbrainz'")
    
    return df1, df2, truth, idA, idB, tp, columns


def executar_uma_replica(dataset_type, seed_valor, experimento=None):
    """Executa uma única réplica do experimento e retorna os resultados."""
    random.seed(seed_valor)
    if experimento is None:
        experimento = preparar_experimento(dataset_type)
    registros_bloqueio, registros_matching, truthD, tp_total = experimento

    t = 0.5
    eps = 0.1
    w = 1000
    delta = 0.1
    L = math.ceil(math.log(1 / delta) / (2 * (eps ** 2)))
    eps = 0.01
    L1 = int(1 / (2 * eps))
    q = 2
    limite = math.ceil(t * L1)
    dictB = [{} for _ in range(L)]
    tp = 0
    fp = 0
    pairs_no = 0
    nbS = 1
    naS = 1
    offsetA = 50
    offsetB = 50
    blockingTime = 0
    matchingTime = 0
    tempo_insercao = 0
    elementos_para_descarte = 50
    proximo_descarte_por_bloco = {}
    
    while True:
        st = time.perf_counter()
        fim_bloqueio = min(naS + offsetA, len(registros_bloqueio))
        for index1 in range(naS, fim_bloqueio):
            id_value, srec = registros_bloqueio[index1]

            for l in range(L):
                key = str_to_MinHash(srec, q, l)
                d = dictB[l]
                bloco = d.get(key)
                if bloco is None:
                    bloco = d[key] = deque()
                    proximo_descarte_por_bloco[(key, l)] = elementos_para_descarte
                elif len(bloco) >= w:
                    tempo_minimo = proximo_descarte_por_bloco[(key, l)]
                    eliminar_expirados(bloco, tempo_minimo)
                    proximo_descarte_por_bloco[(key, l)] = tempo_minimo + elementos_para_descarte
                bloco.append((id_value, tempo_insercao))

            tempo_insercao += 1

        end = time.perf_counter()
        blockingTime += (end - st)
        st = time.perf_counter()
        novos_tp, novos_fp, novos_pares, termination = matching(
            registros_matching, nbS, offsetB, dictB, truthD, L, L1, q, limite
        )
        end = time.perf_counter()
        matchingTime += (end - st)
        tp += novos_tp
        fp += novos_fp
        pairs_no += novos_pares
        if termination:
            break

        nbS += offsetB
        naS += offsetA
    
    return {
        "blocking_s": blockingTime,
        "matching_s": matchingTime,
        "tp": tp,
        "fp": fp,
        "pairs_no": pairs_no,
        "recall": tp / tp_total if tp_total > 0 else 0.0,
        "precision": tp / (tp + fp) if (tp + fp) > 0 else 0.0,
    }


def executar_repeticoes(dataset_type, num_repeticoes, seed_inicial):
    """Executa múltiplas réplicas e calcula estatísticas."""
    resultados = []
    experimento = preparar_experimento(dataset_type)
    print(f"\n{'='*70}")
    print(f"Executando {num_repeticoes} réplicas para {dataset_type}...")
    print(f"{'='*70}")
    
    for replica in range(num_repeticoes):
        seed = seed_inicial + replica
        print(f"Réplica {replica + 1}/{num_repeticoes} (seed={seed})...", end="", flush=True)
        resultado = executar_uma_replica(dataset_type, seed, experimento)
        resultado["replica"] = replica + 1
        resultado["seed"] = seed
        resultados.append(resultado)
        print(" OK")
    
    metricas = ["blocking_s", "matching_s", "tp", "fp", "pairs_no", "recall", "precision"]
    
    print(f"\n{'='*70}")
    print(f"Resumo Estatístico: {dataset_type.upper()} (n={num_repeticoes}, IC=95%)")
    print(f"{'='*70}")
    print(f"{'Métrica':<15} {'Média':<13} {'Desvio Pad':<13} {'IC Inferior':<13} {'IC Superior':<13}")
    print("-" * 75)
    
    for metrica in metricas:
        valores = [r[metrica] for r in resultados]
        media = statistics.mean(valores)
        desvio = statistics.stdev(valores) if num_repeticoes > 1 else 0.0
        
        if num_repeticoes > 1:
            erro = stats.t.ppf(0.975, num_repeticoes - 1) * desvio / (num_repeticoes ** 0.5)
        else:
            erro = 0.0
        
        ic_inf = media - erro
        ic_sup = media + erro
        
        print(f"{metrica:<15} {media:<13.6f} {desvio:<13.6f} {ic_inf:<13.6f} {ic_sup:<13.6f}")
    
    print(f"{'='*70}\n")
    
    return resultados


def carregar_baseline_csv(caminho):
    """Carrega dados do baseline de um arquivo CSV."""
    baseline = []
    try:
        with open(caminho, 'r', encoding='utf-8') as f:
            leitor = csv.DictReader(f)
            for linha in leitor:
                baseline.append({
                    'blocking_s': float(linha['blocking_s']),
                    'matching_s': float(linha['matching_s']),
                    'tp': float(linha['tp']),
                    'fp': float(linha['fp']),
                    'pairs_no': float(linha['pairs_no']),
                    'recall': float(linha['recall']),
                    'precision': float(linha['precision']),
                })
        print(f"✓ Baseline carregado: {len(baseline)} réplicas de {caminho}")
        return baseline
    except FileNotFoundError:
        print(f"✗ Arquivo de baseline não encontrado: {caminho}")
        return None


def comparar_com_baseline(resultados, baseline, dataset_type):
    """Compara resultados com baseline usando teste t independente."""
    if not baseline or len(baseline) == 0:
        print(f"✗ Baseline não disponível")
        return
    
    metricas = ["blocking_s", "matching_s", "tp", "fp", "pairs_no", "recall", "precision"]
    
    print(f"\n{'='*90}")
    print(f"Comparação com Baseline: {dataset_type.upper()}")
    print(f"{'='*90}")
    print(f"{'Métrica':<15} {'Baseline':<13} {'Atual':<13} {'Diferença':<13} {'t-Student':<13} {'p-valor':<13}")
    print("-" * 110)
    
    for metrica in metricas:
        baseline_vals = [b[metrica] for b in baseline]
        atual_vals = [r[metrica] for r in resultados]
        
        media_baseline = statistics.mean(baseline_vals)
        media_atual = statistics.mean(atual_vals)
        diferenca = media_atual - media_baseline
        
        # Teste t independente (welch's t-test)
        teste = stats.ttest_ind(atual_vals, baseline_vals, equal_var=False)
        t_stat = teste.statistic
        p_val = teste.pvalue
        
        sig = "✓" if p_val < 0.05 else "✗"
        print(f"{metrica:<15} {media_baseline:<13.6f} {media_atual:<13.6f} {diferenca:<13.6f} {t_stat:<13.6f} {p_val:<13.6g} {sig}")
    
    print(f"{'='*90}\n")


def salvar_csv(resultados, caminho):
    """Salva os resultados em arquivo CSV."""
    if not resultados:
        return
    
    with open(caminho, "w", newline="", encoding="utf-8") as arquivo:
        campos = list(resultados[0].keys())
        escritor = csv.DictWriter(arquivo, fieldnames=campos, lineterminator="\n")
        escritor.writeheader()
        escritor.writerows(resultados)
    
    print(f"Resultados salvos em: {caminho}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Executa o experimento de bloqueio aproximado com o dataset informado."
    )
    parser.add_argument(
        "-d",
        "--dataset-type",
        choices=["dblp", "ncvoter", "musicbrainz"],
        default="dblp",
        help="Tipo do dataset: dblp, ncvoter ou musicbrainz (padrão: dblp)",
    )
    parser.add_argument(
        "-r",
        "--repeticoes",
        type=int,
        default=10,
        help="Número de réplicas para análise estatística (padrão: 10)",
    )
    parser.add_argument(
        "-s",
        "--seed",
        type=int,
        default=20260907,
        help="Semente inicial; cada réplica usa seed consecutiva (padrão: 20260907)",
    )
    parser.add_argument(
        "-o",
        "--saida-csv",
        help="Arquivo CSV para salvar resultados das réplicas (opcional)",
    )
    parser.add_argument(
        "-b",
        "--baseline-csv",
        help="Arquivo CSV do baseline para comparação (opcional)",
    )
    args = parser.parse_args()
    DATASET_TYPE = args.dataset_type
    NUM_REPETICOES = args.repeticoes
    SEED_INICIAL = args.seed
    SAIDA_CSV = args.saida_csv
    BASELINE_CSV = args.baseline_csv

    if NUM_REPETICOES > 1:
        resultados = executar_repeticoes(DATASET_TYPE, NUM_REPETICOES, SEED_INICIAL)
        if SAIDA_CSV:
            salvar_csv(resultados, SAIDA_CSV)
        # Comparar com baseline se fornecido
        if BASELINE_CSV:
            baseline = carregar_baseline_csv(BASELINE_CSV)
            if baseline:
                comparar_com_baseline(resultados, baseline, DATASET_TYPE)
    else:
        resultado = executar_uma_replica(DATASET_TYPE, SEED_INICIAL)
        if SAIDA_CSV:
            salvar_csv([resultado], SAIDA_CSV)
        print(f"\nblocking time (in secs) {resultado['blocking_s']:.4f}")
        print(f"matching time (in secs) {resultado['matching_s']:.4f}")
        print(f"TP= {resultado['tp']} Recall= {resultado['recall']:.4f} Precision= {resultado['precision']:.4f} pairsNo= {resultado['pairs_no']}")
    