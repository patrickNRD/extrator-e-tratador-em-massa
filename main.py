import os
import re
import zipfile
import paramiko
from tqdm import tqdm
import subprocess

# definindo funções
def menu():
    while True:
        x = input("""
        Bem vindo ao controlador e extrator em massa de dados da NRD Drogarias!
        Digite a sua opção e aperte enter:
        
        1) Inserir o período de registro dos dados a serem retirados; 
        2) Extração em massa;
        3) Exportação dos dados extraídos;
        4) Sair;
        
        """);
        
        if x == '1':
            data_dados();
        elif x == '2':
            extracao_dados();
        elif x == '3':
            exportacao_dados();
        elif x == '4':
            quit();

# Função que atualiza a data de registro nos arquivos de vendas
def data_dados():
    keyword = input("Digite o período de registro dos dados no formato AAAA-MM-DD, com os traços, por favor: ")

    # Percorrer todos os arquivos com estrutura "vendas_(algumTextoVariável).py"
    arquivos = [f for f in os.listdir() if re.match(r'vendas_.*\.py', f)]
    
    sucesso = True
    for arquivo in tqdm(arquivos, desc="Atualizando data_registrov nos arquivos de vendas"):
        try:
            with open(arquivo, 'r') as f:
                conteudo = f.read()
            
            # Modificar a variável 'data_registrov' no arquivo
            novo_conteudo = re.sub(r'data_registrov\s*=\s*".*"', f'data_registrov = "{keyword}"', conteudo)

            with open(arquivo, 'w') as f:
                f.write(novo_conteudo)
        except Exception as e:
            print(f"Erro ao modificar o arquivo {arquivo}: {e}")
            sucesso = False
    
    if sucesso:
        print("Todos os arquivos foram atualizados com sucesso.")
    else:
        print("Ocorreu um erro durante a atualização dos arquivos.")

# Função de extração de dados e execução em paralelo
def extracao_dados():
    # Criar vetor bidimensional vazio
    arquivos = [["", ""] for _ in range(len(os.listdir()))]

    # Preencher vetor X com arquivos de vendas e Y com arquivos de estoque
    x_pos = 0
    y_pos = 0
    for arquivo in os.listdir():
        if re.match(r'vendas_.*\.py', arquivo):
            arquivos[x_pos][0] = arquivo
            x_pos += 1
        elif re.match(r'estoque_.*\.py', arquivo):
            arquivos[y_pos][1] = arquivo
            y_pos += 1

    # Executar cinco processos de vendas por vez
    for i in tqdm(range(0, len(arquivos), 5), desc="Executando arquivos de vendas"):
        processos = []
        for j in range(i, min(i + 5, len(arquivos))):
            if arquivos[j][0]:
                processo = subprocess.Popen(['python3', arquivos[j][0]])
                processos.append(processo)
        
        # Esperar a conclusão dos processos
        for processo in processos:
            processo.wait()

    # Executar cinco processos de estoque por vez
    for i in tqdm(range(0, len(arquivos), 5), desc="Executando arquivos de estoque"):
        processos = []
        for j in range(i, min(i + 5, len(arquivos))):
            if arquivos[j][1]:
                processo = subprocess.Popen(['python3', arquivos[j][1]])
                processos.append(processo)
        
        # Esperar a conclusão dos processos
        for processo in processos:
            processo.wait()

    print("Processos de extração concluídos.")

# Função de exportação de arquivos .csv e envio por SFTP
def exportacao_dados():
    # Localizar todos os arquivos .csv no diretório atual
    arquivos_csv = [f for f in os.listdir() if f.endswith('.csv')]

    # Empacotar arquivos em um arquivo zip
    with zipfile.ZipFile('dados_extraidos.zip', 'w') as zipf:
        for arquivo in arquivos_csv:
            zipf.write(arquivo)

    # Enviar o arquivo zip via SFTP
    try:
        host = "189.126.106.117"
        port = 22
        username = "root"
        password = "Teste23@"

        # Conectar ao servidor SFTP
        transport = paramiko.Transport((host, port))
        transport.connect(username=username, password=password)

        sftp = paramiko.SFTPClient.from_transport(transport)
        
        # Enviar o arquivo
        sftp.put('dados_extraidos.zip', '/path/remoto/dados_extraidos.zip')
        
        sftp.close()
        transport.close()

        print("Arquivo enviado com sucesso ao servidor SFTP.")
    except Exception as e:
        print(f"Erro ao enviar o arquivo: {e}")

menu();
            