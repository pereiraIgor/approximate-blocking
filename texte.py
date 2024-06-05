import os
# Função para ler os arquivos da sink
def read_sink_files(directory):
    results = []
    for filename in os.listdir(directory):
        if filename.startswith('.part-'):
            with open(os.path.join(directory, filename), 'r') as f:
                results.extend(f.readlines())
    return results

# Função para exibir os dados formatados
def display_data(data):
    formatted_data = [line.strip() for line in data]
    print("\n".join(formatted_data))

# Ler e exibir os dados
data = read_sink_files("output.txt/2024-05-23--11")
display_data(data)