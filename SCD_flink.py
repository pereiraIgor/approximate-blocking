# from pyflink.common.typeinfo import Types
# from pyflink.datastream import StreamExecutionEnvironment
# from pyflink.datastream.connectors import StreamingFileSink
# from pyflink.datastream.functions import ProcessFunction
# from pyflink.table import (DataTypes, Schema, TableDescriptor)
# from pyflink.common.serialization import Encoder
# import mmh3
# import random
# import math
# import time
# import csv

# class DataProcessor:
#     def __init__(self, env):
#         self.env = env

#     def process_data(self, input_path1, input_path2, output_path):
#         self.env.set_parallelism(1)# Configuração do ambiente de execução

#         data_stream1 = self.env.read_text_file(input_path1)# Leitura dos dados de entrada
#         processed_stream = data_stream1.process(BlockingFunction())         
#         print(processed_stream.map(lambda x : print(x)))
        
#         data_stream2 = self.env.read_text_file(input_path2)
#         #processed_stream2 = data_stream2.process(MatchingFunction(), processed_stream)
        
#         processed_stream.add_sink(StreamingFileSink.for_row_format(output_path,Encoder.simple_string_encoder()).build()) 
#         self.env.execute("Data Processing Job")

# class BlockingFunction(ProcessFunction):
#     def __init__(self):
#         super(BlockingFunction, self).__init__()
#         self.t = 0.5
#         self.p1 = (self.t) ** 8
#         self.L = math.ceil(math.log(0.1) / math.log(1 - self.p1))
#         self.TP = 5347
#         self.eps = 0.1
#         self.w = 1000
#         self.delta = 0.1
#         self.L = math.ceil(math.log(1 / self.delta) / (2 * (self.eps ** 2)))
#         self.eps = 0.01
#         self.L1 = int(1 / (2 * self.eps))
#         self.L=5
#         self.L1=2
#         self.q = 2
#         self.dictB = [dict() for l in range(self.L)]
#         self.tp = 0
#         self.fp = 0
#         self.pairsNo = 0
#         self.nbS = 1
#         self.naS = 1
#         self.offsetA = 1
#         self.offsetB = 1
#         self.indices = [random.randint(0, self.L) for i in range(self.L1)]
#         self.blockingTime = 0
#         self.matchingTime = 0

#     def str_to_MinHash(self, str1, q, seed=0):
#         return min([mmh3.hash(str1[i:i + q], seed) for i in range(len(str1) - q + 1)])

#     def frequent2(self, temp, L, t):
#         return {k: v for (k, v) in temp.items() if v/L >= t}
    
#     def process_element(self, value, ctx: 'ProcessFunction.Context'):
#         #"id","title","authors","venue","year"
#         st = time.time()
#         #print("o value do process elemento eh esse", value, "\n")
#         row = next(csv.reader([value]))
        
#         idDBLP = row[0]
#         title = row[1]
#         srec = title + " " + row[2]
        
#         key = ""
#         #print("printando srec", srec)
#         #print(idDBLP)
#         for l in range(self.L):
#             key = str(self.str_to_MinHash(srec.lower(), 2, l)) #len(key) = 11
#             d = self.dictB[l]
#             if key in d:
#                 ids = d[key]
#                 if len(ids) < self.w:
#                     ids.append(idDBLP)
#                 else:
#                     ids.pop(0)
#                     ids.append(idDBLP)
#             else:
#                 d[key] = [idDBLP]

        
#         end = time.time()

#         self.blockingTime += (end - st)
#         yield self.dictB
#         #print("Terminou o BlockingFunction e o dictB é ", self.dictB, "\n\n\n")
        

# class MatchingFunction(ProcessFunction):
#     def str_to_MinHash(self, str1, q, seed=0):
#         return min([mmh3.hash(str1[i:i + q], seed) for i in range(len(str1) - q + 1)])

#     def frequent2(self, temp, L, t):
#         return {k: v for (k, v) in temp.items() if v/L >= t}
       
#     def process_element(self, value, ctx: 'ProcessFunction.Context'):
        
#         print("esse printo é do matching function e é do dictB")
#         function_result = blocking_result.function_result
#         dictB = blocking_result.dictB
#         #print(dictB)
#         #print(self.dictB)
#         global tp, fp, pairsNo, L1, q
#         st = time.time()
#         #print("O print do matching eh esse ",value, "\n")
#         row = next(csv.reader([value]))
        
#         idScholar = row[0]
#         title = row[1]
#         srec = title + " " + row[2]
        
#         temp = dict()
#         indices = [random.randrange(0, self.L) for i in range(self.L1)]
#         matchingPairs = {}
#         for l in indices:
#             key = str(self.str_to_MinHash(srec.lower(), self.q, l))
#             d = self.dictB[l]
#             if key in d:
#                 ids = d[key]
#                 for id in ids:
#                     if id in temp:
#                         temp[id] += 1
#                         if temp[id] / self.L1 >= self.t:
#                             matchingPairs[id] = 1
#                     else:
#                         temp[id] = 1
#         print(matchingPairs)
#         end = time.time()   
#         print("terminou o Matching Function")     
#         self.matchingTime += (end - st)
#         pass


# # Exemplo de uso da classe DataProcessor
# if __name__ == '__main__':
#     env = StreamExecutionEnvironment.get_execution_environment()
#     processor = DataProcessor(env)
#     processor.process_data("copia/DBLP_copy.csv", "copia/Scholar_copy.csv", "output.txt")



from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import ProcessFunction, CoProcessFunction
from pyflink.common.serialization import Encoder
from pyflink.datastream.connectors.file_system import FileSink, OutputFileConfig, RollingPolicy
import csv
import random
import math
import mmh3
import time

class FirstProcessFunction(ProcessFunction):
    def __init__(self):
        super(FirstProcessFunction, self).__init__()
        # Inicialização específica da primeira função de processamento

    def process_element(self, value, ctx: 'ProcessFunction.Context'):
        # Lógica de processamento para o primeiro dataset
        yield value

class SecondProcessFunction(CoProcessFunction):
    def __init__(self):
        super(SecondProcessFunction, self).__init__()
        # Inicialização específica da segunda função de processamento

    def process_element1(self, value, ctx: 'CoProcessFunction.Context'):
        # Processamento do primeiro fluxo de dados
        self.first_stream_state = value  # Armazenando o estado do primeiro fluxo
        # Pode incluir lógica para combinar ou utilizar os dados do segundo fluxo
        yield value

    def process_element2(self, value, ctx: 'CoProcessFunction.Context'):
        # Processamento do segundo fluxo de dados
        if hasattr(self, 'first_stream_state'):
            combined_result = f"{self.first_stream_state} - {value}"
            yield combined_result
        else:
            yield value

class DataProcessor:

    def __init__(self):
        self.env = StreamExecutionEnvironment.get_execution_environment()

    def process_data(self, input_path1, input_path2, output_path):
        self.env.set_parallelism(7)  # Configuração do ambiente de execução

        # Primeiro fluxo de dados
        data_stream1 = self.env.read_text_file(input_path1)
        processed_stream1 = data_stream1.process(FirstProcessFunction())
        
        # Segundo fluxo de dados
        data_stream2 = self.env.read_text_file(input_path2)
        
        # Conectar os dois fluxos de dados
        connected_stream = processed_stream1.connect(data_stream2)
        
        # Aplicar a CoProcessFunction
        processed_stream = connected_stream.process(SecondProcessFunction())

        # Configuração do sink
        file_sink = FileSink.for_row_format(
            base_path=output_path,
            encoder=Encoder.simple_string_encoder()
        ).with_output_file_config(
            OutputFileConfig.builder()
            .with_part_prefix("prefix")
            .with_part_suffix(".ext")
            .build()
        ).with_rolling_policy(
            RollingPolicy.default_rolling_policy()
        ).build()
        processed_stream.sink_to(sink=file_sink)

        self.env.execute("Data Processing Job")

if __name__ == '__main__':
    processor = DataProcessor()
    processor.process_data("input1.csv", "input2.csv", "output.txt")