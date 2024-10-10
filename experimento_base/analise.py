import mmh3

def str_to_MinHash(str1, q, seed=0):
    return ([mmh3.hash(str1[i:i + q], seed) for i in range(len(str1) - q + 1)])

palavra = "dffasdfasd asfdçlakhfasdoht pasdo fhpasodhgopáwe hpoasdh poasdn oapwhtnoptopngpoas hh topajwh poi asdpohasdop hasdp ohoph "

print(str_to_MinHash(palavra, 2, 2))
