
import pandas as pd
import time
import random
import math
import mmh3

def str_to_MinHash(str1, q, seed=0):
    return min([mmh3.hash(str1[i:i + q], seed) for i in range(len(str1) - q + 1)])

def frequent2(temp, L, t):
    return {k: v for (k, v) in temp.items() if v/L >= t}

def matching():
    global tp, fp, pairsNo, L1, q
    for index2 in range(nbS, nbS + offsetB): 
        if index2 > len(df2) - 1:
            return True

        rr = df2.iloc[index2, 0:5]
        idScholar = rr["id"]
        title = rr["title"]
        authors = rr["authors"]
        srec = title + " " + authors

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



def elimina_elementos_dentro_dictB(array_para_descarte, array_para_descarte_igual):
    array_para_descarte.clear()
    array_para_descarte_igual.clear()

if __name__ == '__main__':
    df1 = pd.read_csv("../00-datasets/DBLP.csv", sep=",", encoding="utf-8", keep_default_na=False)
    df2 = pd.read_csv("../00-datasets/Scholar.csv", sep=",", encoding="utf-8", keep_default_na=False)
    truth = pd.read_csv("../00-datasets/truth.csv", sep=",", encoding="utf-8", keep_default_na=False)
    truthD = dict()
    for i, r in truth.iterrows():
        idDBLP = r["idDBLP"]
        idScholar = r["idScholar"]
        if idDBLP in truthD:
            ids = truthD[idDBLP]
            ids.append(idScholar)
        else:
            truthD[idDBLP] = [idScholar]

    t = 0.5
    TP = 5347
    eps = 0.1
    w = 1000
    delta = 0.1
    L = math.ceil(math.log(1 / delta) / (2 * (eps ** 2)))
    eps = 0.01
    L1 = int(1 / (2 * eps))
    print("L=", L, "L1=", L1)
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

    tempoQueFoiInseridoNaEstrutura = 0 # esse é o valor que sera colocado junto com o id, seria a posiçaõ do elemento no array

    tamanhoDosBlocos = {}

    total_tempermanencia = 0
    total_entidades = 0


    while True:
        st = time.time()
        for index1 in range(naS, naS + offsetA):
            if index1 >= len(df1):
                break
            rr = df1.iloc[index1, 0:5]
            idDBLP = rr["id"]
            title = rr["title"]
            authors = rr["authors"]

            srec = title + " " + authors
            key = ""
            
            for l in range(L):
                key = str(str_to_MinHash(srec.lower(), 2, l))
                d = dictB[l]
                d_igual = dictB_igual[l]
                if key in d:
                    ids = d[key]
                    ids_igual = d_igual[key]
                    tamanho_atual = tamanhoDosBlocos[(key, l)]
                    if len(ids) < tamanho_atual:
                        ids.append(idDBLP)
                        ids_igual.append(tempoQueFoiInseridoNaEstrutura)
                    else:
                        elimina_elementos_dentro_dictB(ids, ids_igual)

                        ids.append(idDBLP)
                        ids_igual.append(tempoQueFoiInseridoNaEstrutura)
                else:
                    d[key] = [idDBLP]
                    d_igual[key] = [tempoQueFoiInseridoNaEstrutura]

                    tamanhoDosBlocos[(key, l)] = w
                
            tempoQueFoiInseridoNaEstrutura += 1

            for idx, id_entidade in enumerate(ids):
                tempo_permanencia = tempoQueFoiInseridoNaEstrutura - ids_igual[idx]  # Subtraindo o tempo de inserção do tempo atual
                total_tempermanencia += tempo_permanencia
                total_entidades += 1
        
        end = time.time()

        blockingTime += (end - st)
        st = time.time()
        termination = matching()  
        end = time.time()
        matchingTime += (end - st)
        if termination:
                break

        nbS += offsetB
        naS += offsetA

    print("blocking time (in mins)", blockingTime / 60)
    print("matching time (in mins)", matchingTime / 60)
    if tp + fp > 0:
        print("TP=", tp, "Recall=", tp / TP, "Precision=", tp / (tp + fp), "pairsNo=", pairsNo)