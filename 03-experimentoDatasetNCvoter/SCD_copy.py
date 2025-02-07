import pandas as pd
import time
import random
import math
import mmh3
import statistics

def retorna_contagem_blocos(dictB):
    resultado_de_tudo = []
    for e in dictB:
        for valor in e.values():
            resultado_de_tudo.append(len(valor))
    return resultado_de_tudo

def str_to_MinHash(str1, q, seed=0):
    return min([mmh3.hash(str1[i:i + q], seed) for i in range(len(str1) - q + 1)])

def frequent2(temp, L, t):
    return {k: v for (k, v) in temp.items() if v/L >= t}

def matching():
    global tp, fp, pairsNo, L1, q
    for index2 in range(nbS, nbS + offsetB):  # DBLP
        if index2 > len(df2) - 1:
            return True

        # rr = df2.iloc[index2, 0:5]
        # idScholar = rr["id"]
        # title = rr["title"]
        # authors = rr["authors"]
        # srec = title + " " + authors

        rr = df2.iloc[index1, 0:40]
        ncid = rr["ncid"]
        first_name = rr["first_name"]
        last_name = rr["last_name"]
        registr_dt = rr["registr_dt"]
        age_at_year_end = rr["age_at_year_end"]
        srec = first_name + " " + last_name + " " + str(registr_dt) + " " + str(age_at_year_end)
        

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
        # for id in matchingPairs.keys():
        #     idDBLP = id
        #     pairsNo += 1
        #     if idDBLP in truthD:
        #         ids = truthD[idDBLP]
        #         for id in ids:
        #             if id == ncid:
        #                 tp += 1
        #                 break
        #     else:
        #         fp += 1

    return False

def topK():
       global tp, fp, pairsNo, dictB, L, q
       for index2 in range(nbS, nbS+offsetB):
            if index2 >= len(df2):
               return True
            rr = df2.iloc[index2, 0:3]
            idScholar = rr["id"]
            title = rr["title"]
            authors = rr["authors"]
            srec = title + " " + authors
            temp = dict()
            indices = [random.randrange(0, L) for i in range(L1)]
            K = 5
            topK = [0] * K
            topKIds = [None] * K
            for l in indices:
                key = str(str_to_MinHash(srec.lower(), 2, l))
                d = dictB[l]
                if key in d:
                    ids = d[key]
                    for id in ids:
                        if id in temp:
                           temp[id] += 1
                        else:
                           temp[id] = 1

            for id in temp.keys():
               if temp[id] / L1 >= t:
                   for i in range(K):
                       if topK[i] <= temp[id]:
                         topK.insert(i, temp[id])
                         topK.pop()
                         topKIds.insert(i, id)
                         topKIds.pop()
                         break

            for id_1 in topKIds:
                if id_1 in truthD:
                    ids = truthD[id_1]
                    if idScholar in ids:
                          tp += 1
                    else:
                          fp += 1

       return False
# esse codigo é para rodar com o ncvoter42
if __name__ == '__main__':
    df1 = pd.read_csv("../00-datasets/ncvoter42.csv", sep=",", encoding="utf-8", keep_default_na=False)
    df2 = pd.read_csv("../00-datasets/ncvoter42_perturbed.csv", sep=",", encoding="utf-8", keep_default_na=False)
    
    t = 0.5
    p1 = (t) ** 8
    L = math.ceil(math.log(0.1) / math.log(1 - p1))
    # print("Minhash LSH=", L)
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
    indices = [random.randint(0, L) for i in range(L1)]
    blockingTime = 0
    matchingTime = 0
    print("df1", df1.shape, "df2", df2.shape)
    while True:
        st = time.time()
        for index1 in range(naS, naS + offsetA):  # Scholar
            if index1 >= len(df1):
                break
            rr = df1.iloc[index1, 0:40]
            ncid = rr["ncid"]
            first_name = rr["first_name"]
            last_name = rr["last_name"]
            registr_dt = rr["registr_dt"]
            age_at_year_end = rr["age_at_year_end"]
            srec = first_name + " " + last_name + " " + str(registr_dt) + " " + str(age_at_year_end)
            key = ""
            
            for l in range(L):
                key = str(str_to_MinHash(srec.lower(), 2, l))
                d = dictB[l]
                if key in d:
                    ids = d[key]
                    if len(ids) < w:
                        ids.append(ncid)
                    else:
                        ids.pop(0)
                        ids.append(ncid)
                else:
                    d[key] = [ncid]
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
    
    print(retorna_contagem_blocos(dictB))

    