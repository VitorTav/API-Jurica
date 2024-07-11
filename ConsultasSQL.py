import pyodbc
import json
import requests
import csv

# Função para consultar a API e retornar o JSON
def consultar_api(numero_processo, link_api):
    if link_api is None:
        return None  # Retorna None se a URL da API for nula
    url = link_api.strip()
    api_key = "APIKey cDZHYzlZa0JadVREZDJCendQbXY6SkJlTzNjLV9TRENyQk1RdnFKZGRQdw=="  # Chave pública
    payload = json.dumps({
        "query": {
            "match": {
                "numeroProcesso": numero_processo
            }
        }
    })
    headers = {
        'Authorization': api_key,
        'Content-Type': 'application/json'
    }
    response = requests.request("POST", url, headers=headers, data=payload)
    dados_dict = response.json()
    return json.dumps(dados_dict)  # Convertendo o dicionário em string JSON

# Conexão ao SQL Server
conn = pyodbc.connect(
    'DRIVER={SQL Server};'
    'SERVER=172.16.0.19;'
    'DATABASE=Meridio_Db;'
    'UID=horizonteti;'
    'PWD=horizonte@2017'
)

# Consulta SQL
query = """
WITH ProcesosCorrigidos AS (

SELECT

DISTINCT REPLACE(REPLACE(NumeroProcesso, '-', ''), '.', '') AS NumeroProcesso

FROM dbo.Processo AS Processo

LEFT JOIN EntidadesProcesso AS Entidades ON Processo.CodigoProcesso = Entidades.CodigoProcesso
LEFT JOIN Pessoa AS Pessoa ON Entidades.CodigoPessoaParte = Pessoa.CodigoPessoa
LEFT JOIN TipoPessoa ON TipoPessoa.CodigoPessoa = Pessoa.CodigoPessoa
LEFT JOIN NaturezaAcao AS Natureza ON Processo.CodigoNaturezaAcao = Natureza.CodigoNaturezaAcao

WHERE TipoPessoa.CodigoTipoPessoa = 'C' 
AND Processo.DataDistribuicao >= '2015-01-01'
AND (Natureza.CodigoNaturezaAcao = 1 OR Natureza.CodigoNaturezaAcao = 42)
AND Processo.NumeroProcesso <> '' 

)

SELECT

NumeroProcesso,

CASE 
        WHEN SUBSTRING(NumeroProcesso, 14, 1) = '1' AND SUBSTRING(NumeroProcesso, 15, 1) = '0' AND SUBSTRING(NumeroProcesso, 16, 1) = '0' THEN ''
        WHEN SUBSTRING(NumeroProcesso, 14, 1) = '2' AND SUBSTRING(NumeroProcesso, 15, 1) = '0' AND SUBSTRING(NumeroProcesso, 16, 1) = '0' THEN ''
        WHEN SUBSTRING(NumeroProcesso, 14, 1) = '3' AND SUBSTRING(NumeroProcesso, 15, 1) = '0' AND SUBSTRING(NumeroProcesso, 16, 1) = '0' THEN 'https://api-publica.datajud.cnj.jus.br/api_publica_stj/_search'
        WHEN SUBSTRING(NumeroProcesso, 14, 1) = '4' THEN 
            CASE SUBSTRING(NumeroProcesso, 15, 2)
                WHEN '01' THEN 'https://api-publica.datajud.cnj.jus.br/api_publica_trf1/_search'
                WHEN '02' THEN 'https://api-publica.datajud.cnj.jus.br/api_publica_trf2/_search'
                WHEN '03' THEN 'https://api-publica.datajud.cnj.jus.br/api_publica_trf3/_search'
                WHEN '04' THEN 'https://api-publica.datajud.cnj.jus.br/api_publica_trf4/_search'
                WHEN '05' THEN 'https://api-publica.datajud.cnj.jus.br/api_publica_trf5/_search'
                WHEN '06' THEN 'https://api-publica.datajud.cnj.jus.br/api_publica_trf6/_search'
                ELSE ''
            END
        WHEN SUBSTRING(NumeroProcesso, 14, 1) = '5' THEN 
            CASE SUBSTRING(NumeroProcesso, 15, 2)
                WHEN '00' THEN 'https://api-publica.datajud.cnj.jus.br/api_publica_tst/_search'
                WHEN '01' THEN 'https://api-publica.datajud.cnj.jus.br/api_publica_trt1/_search'
                WHEN '02' THEN 'https://api-publica.datajud.cnj.jus.br/api_publica_trt2/_search'
                WHEN '03' THEN 'https://api-publica.datajud.cnj.jus.br/api_publica_trt3/_search'
                WHEN '04' THEN 'https://api-publica.datajud.cnj.jus.br/api_publica_trt4/_search'
                WHEN '05' THEN 'https://api-publica.datajud.cnj.jus.br/api_publica_trt5/_search'
                WHEN '06' THEN 'https://api-publica.datajud.cnj.jus.br/api_publica_trt6/_search'
                WHEN '07' THEN 'https://api-publica.datajud.cnj.jus.br/api_publica_trt7/_search'
                WHEN '08' THEN 'https://api-publica.datajud.cnj.jus.br/api_publica_trt8/_search'
                WHEN '09' THEN 'https://api-publica.datajud.cnj.jus.br/api_publica_trt9/_search'
                WHEN '10' THEN 'https://api-publica.datajud.cnj.jus.br/api_publica_trt10/_search'
                WHEN '11' THEN 'https://api-publica.datajud.cnj.jus.br/api_publica_trt11/_search'
                WHEN '12' THEN 'https://api-publica.datajud.cnj.jus.br/api_publica_trt12/_search'
                WHEN '13' THEN 'https://api-publica.datajud.cnj.jus.br/api_publica_trt13/_search'
                WHEN '14' THEN 'https://api-publica.datajud.cnj.jus.br/api_publica_trt14/_search'
                WHEN '15' THEN 'https://api-publica.datajud.cnj.jus.br/api_publica_trt15/_search'
                WHEN '16' THEN 'https://api-publica.datajud.cnj.jus.br/api_publica_trt16/_search'
                WHEN '17' THEN 'https://api-publica.datajud.cnj.jus.br/api_publica_trt17/_search'
                WHEN '18' THEN 'https://api-publica.datajud.cnj.jus.br/api_publica_trt18/_search'
                WHEN '19' THEN 'https://api-publica.datajud.cnj.jus.br/api_publica_trt19/_search'
                WHEN '20' THEN 'https://api-publica.datajud.cnj.jus.br/api_publica_trt20/_search'
                WHEN '21' THEN 'https://api-publica.datajud.cnj.jus.br/api_publica_trt21/_search'
                WHEN '22' THEN 'https://api-publica.datajud.cnj.jus.br/api_publica_trt22/_search'
                WHEN '23' THEN 'https://api-publica.datajud.cnj.jus.br/api_publica_trt23/_search'
                WHEN '24' THEN 'https://api-publica.datajud.cnj.jus.br/api_publica_trt24/_search'
                ELSE ''
            END
		WHEN SUBSTRING(NumeroProcesso, 14, 1) = '6' THEN 
			CASE SUBSTRING(NumeroProcesso, 15, 2)
				WHEN '00' THEN 'https://api-publica.datajud.cnj.jus.br/api_publica_tse/_search'
				WHEN '01' THEN 'https://api-publica.datajud.cnj.jus.br/api_publica_tre-ac/_search'
				WHEN '02' THEN 'https://api-publica.datajud.cnj.jus.br/api_publica_tre-al/_search'
				WHEN '03' THEN 'https://api-publica.datajud.cnj.jus.br/api_publica_tre-ap/_search'
				WHEN '04' THEN 'https://api-publica.datajud.cnj.jus.br/api_publica_tre-am/_search'
				WHEN '05' THEN 'https://api-publica.datajud.cnj.jus.br/api_publica_tre-ba/_search'
				WHEN '06' THEN 'https://api-publica.datajud.cnj.jus.br/api_publica_tre-ce/_search'
				WHEN '07' THEN 'https://api-publica.datajud.cnj.jus.br/api_publica_tre-dft/_search'
				WHEN '08' THEN 'https://api-publica.datajud.cnj.jus.br/api_publica_tre-es/_search'
				WHEN '09' THEN 'https://api-publica.datajud.cnj.jus.br/api_publica_tre-go/_search'
				WHEN '10' THEN 'https://api-publica.datajud.cnj.jus.br/api_publica_tre-ma/_search'
				WHEN '11' THEN 'https://api-publica.datajud.cnj.jus.br/api_publica_tre-mt/_search'
				WHEN '12' THEN 'https://api-publica.datajud.cnj.jus.br/api_publica_tre-ms/_search'
				WHEN '13' THEN 'https://api-publica.datajud.cnj.jus.br/api_publica_tre-mg/_search'
				WHEN '14' THEN 'https://api-publica.datajud.cnj.jus.br/api_publica_tre-pa/_search'
				WHEN '15' THEN 'https://api-publica.datajud.cnj.jus.br/api_publica_tre-pb/_search'
				WHEN '16' THEN 'https://api-publica.datajud.cnj.jus.br/api_publica_tre-pr/_search'
				WHEN '17' THEN 'https://api-publica.datajud.cnj.jus.br/api_publica_tre-pe/_search'
				WHEN '18' THEN 'https://api-publica.datajud.cnj.jus.br/api_publica_tre-pi/_search'
				WHEN '19' THEN 'https://api-publica.datajud.cnj.jus.br/api_publica_tre-rj/_search'
				WHEN '20' THEN 'https://api-publica.datajud.cnj.jus.br/api_publica_tre-rn/_search'
				WHEN '21' THEN 'https://api-publica.datajud.cnj.jus.br/api_publica_tre-rs/_search'
				WHEN '22' THEN 'https://api-publica.datajud.cnj.jus.br/api_publica_tre-ro/_search'
				WHEN '23' THEN 'https://api-publica.datajud.cnj.jus.br/api_publica_tre-rr/_search'
				WHEN '24' THEN 'https://api-publica.datajud.cnj.jus.br/api_publica_tre-sc/_search'
				WHEN '25' THEN 'https://api-publica.datajud.cnj.jus.br/api_publica_tre-se/_search'
				WHEN '26' THEN 'https://api-publica.datajud.cnj.jus.br/api_publica_tre-sp/_search'
				WHEN '27' THEN 'https://api-publica.datajud.cnj.jus.br/api_publica_tre-to/_search'
				ELSE ''
			END
		WHEN SUBSTRING(NumeroProcesso, 14, 1) = '7' THEN 
			CASE SUBSTRING(NumeroProcesso, 15, 2)
				WHEN '00' THEN 'https://api-publica.datajud.cnj.jus.br/api_publica_stm/_search'
				WHEN '01' THEN 'https://api-publica.datajud.cnj.jus.br/api_publica_1a_cjm/_search'
				WHEN '02' THEN 'https://api-publica.datajud.cnj.jus.br/api_publica_2a_cjm/_search'
				WHEN '03' THEN 'https://api-publica.datajud.cnj.jus.br/api_publica_3a_cjm/_search'
				WHEN '04' THEN 'https://api-publica.datajud.cnj.jus.br/api_publica_4a_cjm/_search'
				WHEN '05' THEN 'https://api-publica.datajud.cnj.jus.br/api_publica_5a_cjm/_search'
				WHEN '06' THEN 'https://api-publica.datajud.cnj.jus.br/api_publica_6a_cjm/_search'
				WHEN '07' THEN 'https://api-publica.datajud.cnj.jus.br/api_publica_7a_cjm/_search'
				WHEN '08' THEN 'https://api-publica.datajud.cnj.jus.br/api_publica_8a_cjm/_search'
				WHEN '09' THEN 'https://api-publica.datajud.cnj.jus.br/api_publica_9a_cjm/_search'
				WHEN '10' THEN 'https://api-publica.datajud.cnj.jus.br/api_publica_10a_cjm/_search'
				WHEN '11' THEN 'https://api-publica.datajud.cnj.jus.br/api_publica_11a_cjm/_search'
				WHEN '12' THEN 'https://api-publica.datajud.cnj.jus.br/api_publica_12a_cjm/_search'
				ELSE ''
			END
		WHEN SUBSTRING(NumeroProcesso, 14, 1) = '8' THEN 
			CASE SUBSTRING(NumeroProcesso, 15, 2)
				WHEN '01' THEN 'https://api-publica.datajud.cnj.jus.br/api_publica_tjac/_search'
				WHEN '02' THEN 'https://api-publica.datajud.cnj.jus.br/api_publica_tjal/_search'
				WHEN '03' THEN 'https://api-publica.datajud.cnj.jus.br/api_publica_tjap/_search'
				WHEN '04' THEN 'https://api-publica.datajud.cnj.jus.br/api_publica_tjam/_search'
				WHEN '05' THEN 'https://api-publica.datajud.cnj.jus.br/api_publica_tjba/_search'
				WHEN '06' THEN 'https://api-publica.datajud.cnj.jus.br/api_publica_tjce/_search'
				WHEN '07' THEN 'https://api-publica.datajud.cnj.jus.br/api_publica_tjdft/_search'
				WHEN '08' THEN 'https://api-publica.datajud.cnj.jus.br/api_publica_tjes/_search'
				WHEN '09' THEN 'https://api-publica.datajud.cnj.jus.br/api_publica_tjgo/_search'
				WHEN '10' THEN 'https://api-publica.datajud.cnj.jus.br/api_publica_tjma/_search'
				WHEN '11' THEN 'https://api-publica.datajud.cnj.jus.br/api_publica_tjmt/_search'
				WHEN '12' THEN 'https://api-publica.datajud.cnj.jus.br/api_publica_tjms/_search'
				WHEN '13' THEN 'https://api-publica.datajud.cnj.jus.br/api_publica_tjmg/_search'
				WHEN '14' THEN 'https://api-publica.datajud.cnj.jus.br/api_publica_tjpa/_search'
				WHEN '15' THEN 'https://api-publica.datajud.cnj.jus.br/api_publica_tjpb/_search'
				WHEN '16' THEN 'https://api-publica.datajud.cnj.jus.br/api_publica_tjpr/_search'
				WHEN '17' THEN 'https://api-publica.datajud.cnj.jus.br/api_publica_tjpe/_search'
				WHEN '18' THEN 'https://api-publica.datajud.cnj.jus.br/api_publica_tjpi/_search'
				WHEN '19' THEN 'https://api-publica.datajud.cnj.jus.br/api_publica_tjrj/_search'
				WHEN '20' THEN 'https://api-publica.datajud.cnj.jus.br/api_publica_tjrn/_search'
				WHEN '21' THEN 'https://api-publica.datajud.cnj.jus.br/api_publica_tjrs/_search'
				WHEN '22' THEN 'https://api-publica.datajud.cnj.jus.br/api_publica_tjro/_search'
				WHEN '23' THEN 'https://api-publica.datajud.cnj.jus.br/api_publica_tjrr/_search'
				WHEN '24' THEN 'https://api-publica.datajud.cnj.jus.br/api_publica_tjsc/_search'
				WHEN '25' THEN 'https://api-publica.datajud.cnj.jus.br/api_publica_tjse/_search'
				WHEN '26' THEN 'https://api-publica.datajud.cnj.jus.br/api_publica_tjsp/_search'
				WHEN '27' THEN 'https://api-publica.datajud.cnj.jus.br/api_publica_tjto/_search'
				ELSE ''
			END
		WHEN SUBSTRING(NumeroProcesso, 14, 1) = '9' THEN 
			CASE SUBSTRING(NumeroProcesso, 15, 2)
				WHEN '13' THEN 'https://api-publica.datajud.cnj.jus.br/api_publica_tjmmg/_search'
				WHEN '21' THEN 'https://api-publica.datajud.cnj.jus.br/api_publica_tjmrs/_search'
				WHEN '26' THEN 'https://api-publica.datajud.cnj.jus.br/api_publica_tjmsp/_search'
				ELSE ''
			END
        ELSE ''
    END AS LinkAPI

FROM ProcesosCorrigidos
WHERE NumeroProcesso IS NOT NULL AND LEN(NumeroProcesso) = 20
"""

# Executando a query
cursor = conn.cursor()
cursor.execute(query)

# Lista para armazenar os dados atualizados
dados_atualizados = []

# Processando os resultados da query
for row in cursor.fetchall():
    numero_processo = row.NumeroProcesso.strip() if row.NumeroProcesso is not None else None
    link_api_value = row.LinkAPI.strip() if row.LinkAPI is not None else None
    link_api = link_api_value.rstrip(';') if link_api_value is not None else None

    # Verificar se o número do processo e a URL da API são válidos
    if not numero_processo or not numero_processo.isdigit() or not link_api:
        print(f"Ignorando linha com dados inválidos: {row}")
        continue

    dados_json = consultar_api(numero_processo, link_api)
    dados_atualizados.append({'NumeroProcesso': numero_processo, 'LinkAPI': link_api, 'DadosJSON': dados_json})
    print(f"Consulta para o processo {numero_processo} concluída.")

# Fechar a conexão
conn.close()

# Especificando o caminho para o arquivo CSV de saída
output_csv_file = r'C:\Users\igor.silva\Grupo Horizonte\SP-Inteligência - Documentos\01_Controladoria\23_Projeto_Jurídico\Processos_com_JSON.csv'

# Escrevendo os dados atualizados no arquivo CSV de saída
with open(output_csv_file, 'w', newline='', encoding='utf-8') as csvfile:
    fieldnames = ['NumeroProcesso', 'LinkAPI', 'DadosJSON']  # Especifica as colunas de interesse

    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    for row in dados_atualizados:
        writer.writerow(row)

print("Dados atualizados foram escritos para", output_csv_file)
