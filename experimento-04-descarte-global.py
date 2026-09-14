import pandas as pd
import time
import random
import math
import mmh3
import argparse
from collections import deque
import csv
import statistics
import sys
from scipy import stats

def str_to_MinHash(str1, q, seed=0):
    return min([mmh3.hash(str1[i:i + q], seed) for i in range(len(str1) - q + 1)])

def frequent2(temp, L, t):
    return {k: v for (k, v) in temp.items() if v/L >= t}

def matching(valores_para_matching, columns):
    global tp, fp, pairsNo, L1, q
    
    if len(valores_para_matching) > 0:
        for correspondencias in valores_para_matching:
            for index2 in correspondencias: 
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
    else:
        for index2 in range(nbS, nbS + offsetB): 
            if index2 > len(df2) - 1:
                return True

            rr = df2.iloc[index2]
            idScholar = rr[columns[1]]
            srec = ""
            for col in columns[2:]:
                srec += " " + str(rr[col])
            key = ""

            possui_correspondencia = rr["poss_correspondencia"]

            if possui_correspondencia == False:
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

def remoção_global_heap(dictGlobalUnico, dictB, dictB_igual, tempo_minimo):
    while dictGlobalUnico and dictGlobalUnico[0][1] < tempo_minimo:
        id, tempo_atual, lista = dictGlobalUnico.popleft()
        for id_2234, posicao_array in lista:
            if id_2234 in dictB[posicao_array] and dictB[posicao_array][id_2234][0] == id:
                dictB[posicao_array][id_2234].popleft()
                dictB_igual[posicao_array][id_2234].popleft()
    return dictGlobalUnico

def elimina_elementos_dentro_dictB(array_para_descarte, array_para_descarte_igual):
    array_para_descarte.clear()
    array_para_descarte_igual.clear()

def pega_correspondencias(df1, truthD, indices_df2, iloc, columns):
    rr = df1.iloc[iloc]
    idDBLP = rr[columns[0]]
    correspondencias = []

    if idDBLP in truthD:
        ids_scholar = truthD[idDBLP]
        for id_scholar in ids_scholar:
            index2 = indices_df2.get(id_scholar)
            if index2 is not None:
                correspondencias.append(index2)

    return correspondencias

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
    global df1, df2, truthD, dictB, dictB_igual, tp, fp, pairsNo, nbS, naS, TP, blockingTime, matchingTime, offsetB, offsetA, L1, L, q, t, w
    
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
    
    df2['poss_correspondencia'] = df2[columns[1]].apply(lambda x: True if x in truth[idB].unique() else False)
    
    indices_df2 = {}
    for index, id_value in enumerate(df2[columns[1]]):
        indices_df2.setdefault(id_value, index)
    
    t = 0.5
    TP = tp_total
    eps = 0.1
    w = 1000
    delta = 0.1
    L = math.ceil(math.log(1 / delta) / (2 * (eps ** 2)))
    eps = 0.01
    L1 = int(1 / (2 * eps))
    q = 2
    dictB = [dict() for l in range(L)]
    dictB_igual = [dict() for l in range(L)]
    tp = 0
    fp = 0
    pairsNo = 0
    nbS = 1
    naS = 1
    offsetA = 50
    offsetB = 50
    blockingTime = 0
    matchingTime = 0
    tempoQueFoiInseridoNaEstrutura = 0
    tamanhoDosBlocos = {}
    dictGlobalUnico = deque()
    valores_para_matching = []
    
    while True:
        st = time.time()
        for index1 in range(naS, naS + offsetA):
            if index1 >= len(df1):
                break
            correspondencias = pega_correspondencias(df1, truthD, indices_df2, index1, columns)
            valores_para_matching.append(correspondencias)
            
            rr = df1.iloc[index1]
            id_value = rr[columns[0]]
            srec = ""
            for col in columns[2:]:
                srec += " " + str(rr[col])
            key = ""
            
            conjunto_chaves = []
            for l in range(L):
                key = str(str_to_MinHash(srec.lower(), 2, l))
                conjunto_chaves.append((key, l))
                d = dictB[l]
                d_igual = dictB_igual[l]
                if key in d:
                    ids = d[key]
                    ids_igual = d_igual[key]
                    tamanho_atual = tamanhoDosBlocos[(key, l)]
                    if len(ids) < tamanho_atual:
                        ids.append(id_value)
                        ids_igual.append(tempoQueFoiInseridoNaEstrutura)
                    else:
                        elimina_elementos_dentro_dictB(ids, ids_igual)
                        ids.append(id_value)
                        ids_igual.append(tempoQueFoiInseridoNaEstrutura)
                else:
                    d[key] = deque([id_value])
                    d_igual[key] = deque([tempoQueFoiInseridoNaEstrutura])
                    tamanhoDosBlocos[(key, l)] = w
            
            dictGlobalUnico.append((id_value, tempoQueFoiInseridoNaEstrutura, conjunto_chaves))
            
            entidades_eliminadas = tempoQueFoiInseridoNaEstrutura - 2500
            if entidades_eliminadas > 0:
                dictGlobalUnico = remoção_global_heap(
                    dictGlobalUnico, dictB, dictB_igual, entidades_eliminadas
                )
            
            tempoQueFoiInseridoNaEstrutura += 1
        
        end = time.time()
        blockingTime += (end - st)
        st = time.time()
        termination = matching(valores_para_matching, columns)
        end = time.time()
        matchingTime += (end - st)
        if termination:
            break
        if len(valores_para_matching) == 0:
            nbS += offsetB
        naS += offsetA
        valores_para_matching = []
    
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
        escritor = csv.DictWriter(arquivo, fieldnames=campos)
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
    VALOR_NULO = 0.0
    SAIDA_CSV = args.saida_csv
    BASELINE_CSV = args.baseline_csv

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
        resultado = executar_uma_replica(DATASET_TYPE, SEED_INICIAL)
        print(f"\nblocking time (in secs) {resultado['blocking_s']:.4f}")
        print(f"matching time (in secs) {resultado['matching_s']:.4f}")
        print(f"TP= {resultado['tp']} Recall= {resultado['recall']:.4f} Precision= {resultado['precision']:.4f} pairsNo= {resultado['pairs_no']}")
