from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import StreamingFileSink
from pyflink.datastream.functions import ProcessFunction
from pyflink.table import (DataTypes, Schema, TableDescriptor)
from pyflink.common.serialization import Encoder
import mmh3
import random
import math
import time
import csv
from pyflink.datastream.connectors.file_system import (FileSource, StreamFormat, FileSink,
                                                       OutputFileConfig, RollingPolicy)
class DataProcessor:
    def __init__(self, env):
        self.env = env

    def process_data(self, input_path1, output_path):
        self.env.set_parallelism(1)# Configuração do ambiente de execução

        data_stream1 = self.env.read_text_file(input_path1)# Leitura dos dados de entrada
        processed_stream = data_stream1.process(BlockingFunction())
        #processed_stream.add_sink(StreamingFileSink.for_row_format(output_path,Encoder.simple_string_encoder()).build()) 
        
        processed_stream.sink_to(
            sink=FileSink.for_row_format(base_path=output_path, encoder=Encoder.simple_string_encoder())
            .with_output_file_config(OutputFileConfig.builder().with_part_prefix("prefix").with_part_suffix(".ext").build())
            .with_rolling_policy(RollingPolicy.default_rolling_policy()).build()
        )
        print("terminou")
        self.env.execute("Data Processing Job")

class BlockingFunction(ProcessFunction):
    def __init__(self):
        super(BlockingFunction, self).__init__()
        self.t = 0.5
        self.p1 = (self.t) ** 8
        self.L = math.ceil(math.log(0.1) / math.log(1 - self.p1))
        self.TP = 5347
        self.eps = 0.1
        self.w = 1000
        self.delta = 0.1
        self.L = math.ceil(math.log(1 / self.delta) / (2 * (self.eps ** 2)))
        self.eps = 0.01
        self.L1 = int(1 / (2 * self.eps))
        #self.L=5
        #self.L1=2
        self.q = 2
        self.dictB = [dict() for l in range(self.L)]
        self.tp = 0
        self.fp = 0
        self.pairsNo = 0
        self.nbS = 1
        self.naS = 1
        self.offsetA = 1
        self.offsetB = 1
        self.indices = [random.randint(0, self.L) for i in range(self.L1)]
        self.blockingTime = 0
        self.matchingTime = 0

    def str_to_MinHash(self, str1, q, seed=0):
        return min([mmh3.hash(str1[i:i + q], seed) for i in range(len(str1) - q + 1)])

    def frequent2(self, temp, L, t):
        return {k: v for (k, v) in temp.items() if v/L >= t}
    
    def matching(self, value):
        #global tp, fp, pairsNo, L1, q
        st = time.time()
        row = next(csv.reader([value])) 
        if(row[5] == "scholar"):
            
            #print("linha do matching", row)      
            idScholar = row[0]
            title = row[1]
            srec = title + " " + row[2]
            
            temp = dict()
            indices = [random.randrange(0, self.L) for i in range(self.L1)]
            matchingPairs = {}
            for l in indices:
                key = str(self.str_to_MinHash(srec.lower(), self.q, l))
                d = self.dictB[l]
                if key in d:
                    ids = d[key]
                    for id in ids:
                        if id in temp:
                            temp[id] += 1
                            if temp[id] / self.L1 >= self.t:
                                matchingPairs[id] = 1
                        else:
                            temp[id] = 1
            print(idScholar, matchingPairs)
            end = time.time()      
            self.matchingTime += (end - st)
        
    def process_element(self, value, ctx: 'ProcessFunction.Context'):
        st = time.time()
        row = next(csv.reader([value]))
        if(row[5] == "dblp"):
            #print("linha do blocking", row)
            idDBLP = row[0]
            title = row[1]
            srec = title + " " + row[2]
            key = ""

            for l in range(self.L):
                key = str(self.str_to_MinHash(srec.lower(), 2, l)) #len(key) = 11
                d = self.dictB[l]
                if key in d:
                    ids = d[key]
                    if len(ids) < self.w:
                        ids.append(idDBLP)
                    else:
                        ids.pop(0)
                        ids.append(idDBLP)
                else:
                    d[key] = [idDBLP]
        
        end = time.time()

        self.blockingTime += (end - st)
        yield self.dictB
        self.matching(value)
 
# Exemplo de uso da classe DataProcessor
if __name__ == '__main__':
    env = StreamExecutionEnvironment.get_execution_environment()
    processor = DataProcessor(env)
    processor.process_data("df__full_merged.csv", "output.txt")
    #processor.process_data("copia/DBLP_copy.csv", "copia/Scholar_copy.csv", "output.txt")