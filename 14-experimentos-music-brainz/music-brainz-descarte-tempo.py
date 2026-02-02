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
    for index2 in range(nbS, nbS + offsetB):  # DBLP
        if index2 > len(df2) - 1:
            return True

        rr = df2.iloc[index2]
        ncid = rr["TID2"]
        title = rr["title"]
        length = rr["length"]
        artist = rr["artist"]
        album = rr["album"]
        srec = title + " " + length + " " + str(artist) + " " + str(album)
    
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
                    if id == ncid:
                        tp += 1
                        break
            else:
                fp += 1

    return False

def elimina_elementos_dentro_dictB(array_para_descarte, array_para_descarte_igual, elementos_para_descarte):
    # print("qtd_descarte")
    indice = 0
    for (tempo_inserido) in array_para_descarte_igual:
        if tempo_inserido < elementos_para_descarte:
            indice += 1
        else:
            del array_para_descarte[:indice]
            del array_para_descarte_igual[:indice]
            break

if __name__ == "__main__":
    df1 = pd.read_csv("musicbrainz-200-A01.csv.dapo", sep=",", encoding="utf-8", keep_default_na=False)
    df2 = pd.read_csv("music_brainz-simple-mutated.csv", sep=",", encoding="utf-8", keep_default_na=False)

    truth = pd.read_csv("ground_truth_music_brainz.csv", sep=",", encoding="utf-8", keep_default_na=False)
    truthD = dict()
    for i, r in truth.iterrows():
        novoId = r["novoidmusic1"]
        antigoId = r["antigoidmusic2"]
        if antigoId in truthD:
            ids = truthD[antigoId]
            ids.append(novoId)
        else:
            truthD[antigoId] = [novoId]

    t = 0.5
    TP = 193471
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
    indices = [random.randint(0, L) for i in range(L1)]
    blockingTime = 0
    matchingTime = 0

    tempoQueFoiInseridoNaEstrutura = 0 # esse é o valor que sera colocado junto com o id, seria a posiçaõ do elemento no array
    elementos_para_descarte = 50 # esse é a quantidade de elementos que serão descartados
    acompanhamentoIndicePorBloco = {} #essa estrutura é um indice que vai ser usada para saber qual a posição do elemento que será descartado


    while True:
        st = time.time()
        for index1 in range(naS, naS + offsetA):  # Scholar
            if index1 >= len(df1):
                break
            rr = df1.iloc[index1]
            ncid = rr["TID"]
            title = rr["title"]
            length = rr["length"]
            artist = rr["artist"]
            album = rr["album"]
            srec = title + " " + length + " " + str(artist) + " " + str(album)
            key = ""
            
            for l in range(L):
                key = str(str_to_MinHash(srec.lower(), 2, l))
                d = dictB[l]
                d_igual = dictB_igual[l]
                if key in d:
                    ids = d[key]
                    ids_igual = d_igual[key]
                    if len(ids) < w:
                        ids.append(ncid)
                        ids_igual.append(tempoQueFoiInseridoNaEstrutura)
                    else:
                        elementoAtualParaDescarte = acompanhamentoIndicePorBloco[(key, l)]
                        elimina_elementos_dentro_dictB(ids, ids_igual, elementoAtualParaDescarte)
                        acompanhamentoIndicePorBloco[(key, l)] = elementoAtualParaDescarte + elementos_para_descarte
                        
                        ids.append(ncid)
                        ids_igual.append(tempoQueFoiInseridoNaEstrutura)
                else:
                    d[key] = [ncid]
                    d_igual[key] = [tempoQueFoiInseridoNaEstrutura]
                    acompanhamentoIndicePorBloco[(key, l)] = elementos_para_descarte

            tempoQueFoiInseridoNaEstrutura += 1

        end = time.time()
        blockingTime += (end - st)
        st = time.time()
        #You can use either method matching() or topK()  
        
        termination = matching()  
        #termination = topK()
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
    