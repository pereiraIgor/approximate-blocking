import pandas as pd
import time
import random
import math
import mmh3
import argparse
import csv
import re
import statistics
import sys
from scipy import stats

def str_to_MinHash(str1, q, seed=0):
    return min([mmh3.hash(str1[i:i + q], seed) for i in range(len(str1) - q + 1)])

def frequent2(temp, L, t):
    return {k: v for (k, v) in temp.items() if v/L >= t}

def matching(columns):
    global tp, fp, pairsNo, L1, q
    for index2 in range(nbS, nbS + offsetB):  # DBLP
        if index2 > len(df2) - 1:
            return True

        rr = df2.iloc[index2]
        idScholar = rr[columns[1]]
        srec = ""
        for col in columns[2:]:
            srec += " " + str(rr[col])
        key = ""
        temp = dict()
        indices = [random.randrange(0, L) for i in range(L1)]
        matchingPairs = {}
        for l in indices:
            key = str(str_to_MinHash(srec.lower(), q, l))
            d = dictB[l]
            if key in d:
                ids = d[key]
                for id in ids:
                    if id in temp:
                        temp[id] += 1
                        if temp[id] / L1 >= t:
                            matchingPairs[id] = 1
                    else:
                        temp[id] = 1
        for id in matchingPairs.keys():
            idDBLP = id
            pairsNo += 1
            if idDBLP in truthD:
                ids = truthD[idDBLP]
                for id in ids:
                    if id == idScholar:
                        tp += 1
                        break
            else:
                fp += 1
    return False

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


def executar_uma_replica(dataset_type, seed_valor):
    """Executa uma única réplica do experimento e retorna os resultados."""
    global df1, df2, truthD, dictB, tp, fp, pairsNo, nbS, naS, TP, blockingTime, matchingTime, offsetB, L1, L,q, t
    
    random.seed(seed_valor)
    
    df1, df2, truth, idA, idB, tp_total, columns = load_dataset(dataset_type)
    truthD = dict()
    
    for i, r in truth.iterrows():
        idA_value = r[idA]
        idB_value = r[idB]
        if idA_value in truthD:
            ids = truthD[idA_value]
            ids.append(idB_value)
        else:
            truthD[idA_value] = [idB_value]
    
    t = 0.5
    p1 = (t) ** 8
    L = math.ceil(math.log(0.1) / math.log(1 - p1))
    TP = tp_total
    eps = 0.1
    w = 1000
    delta = 0.1
    L = math.ceil(math.log(1 / delta) / (2 * (eps ** 2)))
    eps = 0.01
    L1 = int(1 / (2 * eps))
    q = 2
    dictB = [dict() for l in range(L)]
    tp = 0
    fp = 0
    pairsNo = 0
    nbS = 1
    naS = 1
    offsetA = 50
    offsetB = 50
    blockingTime = 0
    matchingTime = 0
    
    while True:
        st = time.time()
        for index1 in range(naS, naS + offsetA):
            if index1 >= len(df1):
                break
            
            rr = df1.iloc[index1]
            id_value = rr[columns[0]]
            srec = ""
            for col in columns[2:]:
                srec += " " + str(rr[col])
            key = ""
            
            for l in range(L):
                key = str(str_to_MinHash(srec.lower(), 2, l))
                d = dictB[l]
                if key in d:
                    ids = d[key]
                    if len(ids) < w:
                        ids.append(id_value)
                    else:
                        ids.pop(0)
                        ids.append(id_value)
                else:
                    d[key] = [id_value]
        end = time.time()
        blockingTime += (end - st)
        st = time.time()
        termination = matching(columns)
        end = time.time()
        matchingTime += (end - st)
        if termination:
            break
        
        nbS += offsetB
        naS += offsetA
    
    return {
        "blocking_s": blockingTime,
        "matching_s": matchingTime,
        "tp": tp,
        "fp": fp,
        "pairs_no": pairsNo,
        "recall": tp / TP if TP > 0 else 0.0,
        "precision": tp / (tp + fp) if (tp + fp) > 0 else 0.0,
    }


def executar_repeticoes(dataset_type, num_repeticoes, seed_inicial, valor_nulo):
    """Executa múltiplas réplicas e calcula estatísticas."""
    resultados = []
    
    print(f"\n{'='*70}")
    print(f"Executando {num_repeticoes} réplicas para {dataset_type}...")
    print(f"{'='*70}")
    
    for replica in range(num_repeticoes):
        seed = seed_inicial + replica
        print(f"Réplica {replica + 1}/{num_repeticoes} (seed={seed})...", end="", flush=True)
        resultado = executar_uma_replica(dataset_type, seed)
        resultado["replica"] = replica + 1
        resultado["seed"] = seed
        resultados.append(resultado)
        print(" OK")
    
    # Calcular estatísticas
    metricas = ["blocking_s", "matching_s", "tp", "fp", "pairs_no", "recall", "precision"]
    
    print(f"\n{'='*70}")
    print(f"Resumo Estatístico: {dataset_type.upper()} (n={num_repeticoes}, IC=95%)")
    print(f"{'='*70}")
    print(f"{'Métrica':<15} {'Média':<12} {'Desvio Pad':<12} {'IC Inferior':<12} {'IC Superior':<12} {'t-Student':<12} {'p-valor':<12}")
    print("-" * 95)
    
    for metrica in metricas:
        valores = [r[metrica] for r in resultados]
        media = statistics.mean(valores)
        desvio = statistics.stdev(valores) if num_repeticoes > 1 else 0.0
        
        if num_repeticoes > 1:
            erro = stats.t.ppf(0.975, num_repeticoes - 1) * desvio / (num_repeticoes ** 0.5)
            teste = stats.ttest_1samp(valores, valor_nulo)
            t_stat = teste.statistic
            p_val = teste.pvalue
        else:
            erro = 0.0
            t_stat = float("nan")
            p_val = float("nan")
        
        ic_inf = media - erro
        ic_sup = media + erro
        
        print(f"{metrica:<15} {media:<12.6f} {desvio:<12.6f} {ic_inf:<12.6f} {ic_sup:<12.6f} {t_stat:<12.6f} {p_val:<12.6g}")
    
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
        escritor = csv.DictWriter(arquivo, fieldnames=campos)
        escritor.writeheader()
        escritor.writerows(resultados)
    
    print(f"Resultados salvos em: {caminho}")

if __name__ == '__main__':
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
    VALOR_NULO = 0.0
    SAIDA_CSV = args.saida_csv
    BASELINE_CSV = args.baseline_csv

    # Executar réplicas
    if NUM_REPETICOES > 1:
        resultados = executar_repeticoes(DATASET_TYPE, NUM_REPETICOES, SEED_INICIAL, VALOR_NULO)
        if SAIDA_CSV:
            salvar_csv(resultados, SAIDA_CSV)
        # Comparar com baseline se fornecido
        if BASELINE_CSV:
            baseline = carregar_baseline_csv(BASELINE_CSV)
            if baseline:
                comparar_com_baseline(resultados, baseline, DATASET_TYPE)
    else:
        # Execução única (modo compatível com o original)
        resultado = executar_uma_replica(DATASET_TYPE, SEED_INICIAL)
        print(f"\nblocking time (in secs) {resultado['blocking_s']:.4f}")
        print(f"matching time (in secs) {resultado['matching_s']:.4f}")
        print(f"TP= {resultado['tp']} Recall= {resultado['recall']:.4f} Precision= {resultado['precision']:.4f} pairsNo= {resultado['pairs_no']}")

