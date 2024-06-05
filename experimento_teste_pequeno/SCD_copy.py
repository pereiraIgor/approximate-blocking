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
    print("ETAPA DE MATCHING")
    for index2 in range(nbS, nbS + offsetB):  # Scholar_copy
        if index2 > len(df2) - 1:
            return True

        rr = df2.iloc[index2, 0:3]
        idScholar = rr["id"]
        title = rr["title"]
        srec = title + " " + rr["authors"]
        print("\n\nsrec do matching é esse: ", srec)
        temp = dict()
        indices = [random.randrange(0, L) for i in range(L1)] # gera valores random , porem pode ser igual, o que pode acontecer de fazer varias vezes a mesma comparação, se melhorar isso pode melhorar o precision
        print("os indices gerados: \n", indices)
        matchingPairs = {}
        for l in indices:
            key = str(str_to_MinHash(srec.lower(), q, l))
            print("--- a KEY gerada é: ", key)
            d = dictB[l]
            print("--- o d é: ", d)
            print("dict b é ", dictB)
            if key in d: #ACHO QUE AQUI PODE SER UMA MELHORIA, TALVEZ UM MATCHING REVERSO, ELE PARA AQUI
                print("------ chave esta em d")
                ids = d[key]
                print("------ o array de ids é ", ids)
                print("------ temporario antes", temp)
                for id in ids:
                    print("------ o id é",id)
                    if id in temp:
                        temp[id] += 1
                        print("matemagica é ", temp[id] / L1)
                        if temp[id] / L1 >= t: 
                            print("é um match")
                            matchingPairs[id] = 1
                    else:
                        temp[id] = 1
                print("temporario depois", temp, "\n")

        print("Matching pairs", matchingPairs)
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

if __name__ == '__main__':
    df1 = pd.read_csv("DBLP_copy.csv", sep=",", encoding="utf-8", keep_default_na=False) #2617
    df2 = pd.read_csv("Scholar_copy.csv", sep=",", encoding="utf-8", keep_default_na=False) #64000
    truth = pd.read_csv("truth_copy.csv", sep=",", encoding="utf-8", keep_default_na=False)
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
    p1 = (t) ** 8
    L = math.ceil(math.log(0.1) / math.log(1 - p1))
    TP = 5347
    eps = 0.1
    w = 1000
    delta = 0.1
    L = math.ceil(math.log(1 / delta) / (2 * (eps ** 2)))
    eps = 0.01
    L1 = int(1 / (2 * eps))
    L=5
    L1=2
    print("L=", L, "L1=", L1)
    q = 2
    dictB = [dict() for l in range(L)]
    tp = 0
    fp = 0
    pairsNo = 0
    nbS = 0 #MUDEI AQUI
    naS = 0 #MUDEI AQUI
    offsetA = 1
    offsetB = 1
    indices = [random.randint(0, L) for i in range(L1)]
    blockingTime = 0
    matchingTime = 0

    while True:
      st = time.time()
      for index1 in range(naS, naS + offsetA): #DBLP 
          if index1 >= len(df1):
              break
          rr = df1.iloc[index1, 0:3]
          idDBLP = rr["id"]
          title = rr["title"]
          srec = title + " " + rr["authors"]
          key = ""
          print("\n\n\n correspondente df1", idDBLP)
          for l in range(L):
              key = str(str_to_MinHash(srec.lower(), 2, l))# roda usando o str com varias seeds diferentes, no caso o l
              #print("--- Chave gerada é:", key," ---")
              #print(dictB)
              d = dictB[l]
              #print(d)
              #print("--- O DictB está assim:", dictB)
              if key in d:
                  #print("--- chave esta no d")
                  ids = d[key]
                  #print("antes",ids)
                  if len(ids) < w:
                      ids.append(df1.iloc[index1, 0])
                  else:
                      ids.pop(0)
                      ids.append(df1.iloc[index1, 0])
                  #print("depois", ids)
                  #print("--- O DictB AGORAAAAA está assim:", dictB)

              else:
                  #print("--- chave não esta no d")
                  d[key] = [df1.iloc[index1, 0]]
                  #print("--- O DictB AGORAAAAA está assim:", dictB)
          #print(ids)
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



