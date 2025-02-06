import pandas as pd
import time
import random
import math
import mmh3
import statistics

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
                for id, _ in ids:
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


def elimina_elementos_dentro_dictB(array_para_descarte, elementos_para_descarte):
    # print(f"indice {elementos_para_descarte} para descarte e o array é {array_para_descarte}")
    indice = 0
    for (_, tempo_inserido) in array_para_descarte:
        if tempo_inserido < elementos_para_descarte:
            indice += 1
        else:
            del array_para_descarte[:indice]
            break


if __name__ == '__main__':
    df1 = pd.read_csv("./DBLP.csv", sep=",", encoding="utf-8", keep_default_na=False)
    df2 = pd.read_csv("./Scholar.csv", sep=",", encoding="utf-8", keep_default_na=False)
    truth = pd.read_csv("truth.csv", sep=",", encoding="utf-8", keep_default_na=False)
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
    elementos_para_descarte = 50 # esse é a quantidade de elementos que serão descartados
    nova_estrutura = {}

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
                if key in d:
                    ids = d[key]
                    if len(ids) < w:
                        ids.append((idDBLP, tempoQueFoiInseridoNaEstrutura))
                    else:
                        qtd_descarte, elementoAtualParaDescarte = nova_estrutura[(key, l)]
                        elimina_elementos_dentro_dictB(ids, elementoAtualParaDescarte)
                        nova_estrutura[(key, l)] = [qtd_descarte + 1, elementoAtualParaDescarte + 50]
                        ids.append((idDBLP, tempoQueFoiInseridoNaEstrutura))
                else:
                    d[key] = [(idDBLP, tempoQueFoiInseridoNaEstrutura)]
                    nova_estrutura[(key, l)] = [0, 50]
            tempoQueFoiInseridoNaEstrutura += 1
        
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