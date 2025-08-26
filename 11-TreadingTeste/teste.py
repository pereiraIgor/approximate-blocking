import threading
import time

# Função que será executada por cada thread
def tarefa(nome, tempo):
    print(f"Thread {nome} iniciada.")
    time.sleep(tempo)  # simula trabalho demorado
    print(f"Thread {nome} finalizada após {tempo} segundos.")

# Lista de threads
threads = []

# Criando e iniciando 5 threads
for i in range(5):
    t = threading.Thread(target=tarefa, args=(f"T{i+1}", i+1))
    threads.append(t)
    t.start()

# Aguardando todas terminarem
for t in threads:
    t.join()

print("Todas as threads finalizaram!")