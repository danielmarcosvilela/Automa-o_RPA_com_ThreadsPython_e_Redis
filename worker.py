import redis
import redis.exceptions
import threading
import time
import requests
import pandas as pd
from geopy.geocoders import Nominatim
import mysql.connector
from mysql.connector import Error



# Cria um evento que será utilizado para sinalizar quando a inserção de CEPs estiver completa.
"""O Event é usado para sincronizar as threads. No caso, a thread_inserir sinaliza que terminou seu trabalho chamando 
insercao_completa.set(), o que permite que a thread_blpop saiba que a inserção de CEPs foi completada. 
No entanto, a thread_blpop continua processando itens enquanto houver elementos na fila do Redis.
"""
insercao_completa = threading.Event()

# Função que estabelece uma conexão com o Redis.
def conectar_redis():
    conn = redis.Redis(host='localhost', port=6379, db=0)
    return conn

# Função que desconecta do Redis.
def desconectar_redis(conn):
    conn.connection_pool.disconnect()
    
# Função que estabelece uma conexão com o MySQL.    
def conectar_mysql():
    try:
        conn = mysql.connector.connect(
            host='localhost',
            database='Estudos',
            user='daniel',
            password='B@nana23'
        )
        if conn.is_connected():
            print("Conectado ao MySQL")
            return conn
    except Error as e:
        print(f"Erro ao conectar ao MySQL: {e}")
        return None

# Função que desconecta do MySQL.    
def desconectar_mysql(conn):
    if conn.is_connected():
        conn.close()
        print("Conexão ao MySQL fechada")
    
# Função que lê um arquivo de texto (CEPs.txt), insere os CEPs em uma fila Redis e sinaliza quando a inserção está completa.    
def inserir():
    try:
        conn = conectar_redis()
        
        with open('CEPs.txt', 'r') as file:
            ceps = file.readlines()
            
        for cep in ceps:
            fila = cep.strip()
            
            try:
                res = conn.rpush('fila', fila)
                if res:
                    print(f'O cep {fila} foi inserido com sucesso.')
                    ceps.remove(cep)
                    with open('CEPs.txt', 'w') as file:
                        file.writelines(ceps)
                else:
                    print('Não foi possivel inserir o cep.')
            except redis.exceptions.ConnectionError as e:
                print(f'Não foi possivel inserir o cep na fila: {e}')
                
                
        insercao_completa.set()
        desconectar_redis(conn)
    except FileNotFoundError:
        print('Arquivo CEPs.txt não encontrado.')
    except Exception as e:
        print(f'Ocorreu um erro: {e}')
        
# Função que remove CEPs da fila no Redis, busca informações de localização usando uma API, e insere esses dados no MySQL.        
def blpop_lista(lista, timeout=0):
    redis_conn = conectar_redis()
    mysql_conn = conectar_mysql()
    cursor = mysql_conn.cursor()
    
    try:
        while not insercao_completa.is_set() or redis_conn.llen(lista) > 0:
            result = redis_conn.blpop(lista, timeout)
            time.sleep(2)
            if result:
                nome_lista, valor = result
                cep = valor.decode("utf-8")
                print(f'Elemento removido da lista {nome_lista.decode("utf-8")}: {valor.decode("utf-8")}')
                
                
                try:
                    if len(cep) == 8:
                        link = f'https://viacep.com.br/ws/{cep}/json/'
                        requisicao = requests.get(link)
                        dic_requisicao = requisicao.json()
                        uf = dic_requisicao.get('uf', '')
                        cidade = dic_requisicao.get('localidade', '')
                        bairro = dic_requisicao.get('bairro', '')
                        logradouro = dic_requisicao.get('logradouro', '')
                        
                        geolocalizador = Nominatim(user_agent="worker")
                        localizacao = geolocalizador.geocode(logradouro)
                        
                        if localizacao:
                            inserir_no_mysql(cursor, mysql_conn, cep, localizacao.latitude, localizacao.longitude, logradouro, bairro, cidade, uf)
                        else:
                            print(f'Não foi possível encontrar localização para o CEP {cep}')
                    else:
                        print(f'CEP {cep} inválido')
                except Exception as e:
                    print(f'Erro ao buscar dados do CEP {cep}: {e}')
            else:
                time.sleep(1)
                
    except redis.exceptions.ConnectionError as e:
        print(f'Erro de conexão ao executar BLPOP: {e}')
    finally:
        desconectar_redis(redis_conn)
        desconectar_mysql(mysql_conn)
        
# Função que insere os dados de endereço no banco de dados MySQL.        
def inserir_no_mysql(cursor, conn, cep, latitude, longitude, logradouro, bairro, cidade, estado):
    try:
        sql = """
        INSERT INTO endereco (CEP, LATITUDE, LONGITUDE, LAGRADOURO, BAIRRO, CIDADE, ESTADO)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        val = (cep, latitude, longitude, logradouro, bairro, cidade, estado)
        cursor.execute(sql, val)
        conn.commit()
        print(f'Endereço com CEP {cep} inserido no banco de dados.')
    except Error as e:
        print(f'Erro ao inserir no MySQL: {e}')
        

"""
Estas linhas criam duas threads: uma para inserir CEPs na fila Redis e outra para processar essa fila, 
obtendo dados de localização e inserindo no MySQL. As threads são iniciadas e o programa espera que ambas terminem.
"""        
if __name__ == "__main__":
    
    # Aqui, são criadas duas threads:
    thread_inserir = threading.Thread(target=inserir)
    thread_blpop = threading.Thread(target=blpop_lista, args=('fila', 0))
    
    
    """
    As threads são iniciadas com o método start(). A partir desse ponto, ambas as threads começam a ser executadas quase ao mesmo tempo. 
    Elas são independentes uma da outra em termos de execução. Ou seja, a execução de uma não depende do término da outra.
    Após serem iniciadas, as duas threads são executadas em paralelo. O sistema operacional gerencia a execução das threads, 
    permitindo que ambas possam progredir simultaneamente. 
    No entanto, como elas podem estar operando em recursos compartilhados (como o arquivo de CEPs e as conexões de banco de dados), 
    há necessidade de uma coordenação cuidadosa.
    """
    thread_inserir.start()
    thread_blpop.start()
    
    """O método join() é usado para bloquear a thread principal até que a thread específica (thread_inserir e thread_blpop) 
    termine sua execução. Isso garante que o programa principal não termine antes que ambas as threads completem seu trabalho.
    """
    thread_inserir.join()
    thread_blpop.join()