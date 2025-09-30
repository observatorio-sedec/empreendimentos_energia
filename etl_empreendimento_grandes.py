import pandas as pd
import requests as rq
from openpyxl import load_workbook
import re


dict_tarifado = {
    'A1': 'Tensão de fornecimento igual ou superior a 230 kV',
    'A2': 'Tensão de fornecimento de 88 kV a 138 kV',
    'A3': 'Tensão de fornecimento de 69 kV',
    'A3a': 'Tensão de fornecimento de 30 kV a 44 kV',
    'A4': 'Tensão de fornecimento de 2,3 kV a 25 kV',
    'AS': 'Subterrâneo',
    'B1': 'Residencial',
    'B2': 'Rural',
    'B3': 'Demais classes',
    'B4': 'Iluminação pública'
}

url_download = f'https://dadosabertos.aneel.gov.br/dataset/6d90b77c-c5f5-4d81-bdec-7bc619494bb9/resource/11ec447d-698d-4ab8-977f-b424d5deee6a/download/siga-empreendimentos-geracao.csv'
arquivo_destino = "empreendimentos_grandes.xlsx"
response = rq.get(url_download)
if response.status_code == 200:
    with open(arquivo_destino, "wb") as file:
        file.write(response.content)
else:
    print(f"Erro ao baixar o arquivo. Código: {response.status_code}")
    
df_empreendimentos_grandes = pd.read_csv(arquivo_destino, sep=';', encoding='latin1')
df_empreendimentos_grandes['DscSubBacia'] = df_empreendimentos_grandes['DscSubBacia'].str.replace(r'^\s*\d+\s*-\s*', '', regex=True)
df_empreendimentos_grandes['IdcGeracaoQualificada'] = df_empreendimentos_grandes['IdcGeracaoQualificada'].fillna("Sem Informação")
df_empreendimentos_grandes['DscSubBacia'] = df_empreendimentos_grandes['DscSubBacia'].fillna("Sem Informação")
df_empreendimentos_grandes  = df_empreendimentos_grandes.rename(columns={'DscMuninicpios': 'DscMunicipios'})
df_empreendimentos_grandes["DscMunicipios"] = df_empreendimentos_grandes["DscMunicipios"].str.split(" - ").str[0]
# print(df_empreendimentos_grandes.info())
# df_empreendimentos_grandes.to_excel("empreendimentos_grandes.xlsx", index=False)



url_download_pequeno = f"https://dadosabertos.aneel.gov.br/dataset/5e0fafd2-21b9-4d5b-b622-40438d40aba2/resource/b1bd71e7-d0ad-4214-9053-cbd58e9564a7/download/empreendimento-geracao-distribuida.csv"
arquivo_destino_pequeno = "empreendimentos_pequenos.xlsx"
response = rq.get(url_download_pequeno)
if response.status_code == 200:
    with open(arquivo_destino_pequeno, "wb") as file:
        file.write(response.content)
else:
    print(f"Erro ao baixar o arquivo. Código: {response.status_code}")
    
df_empreendimentos_pequenos = pd.read_csv(arquivo_destino_pequeno, sep=';', encoding='latin1')
df_empreendimentos_pequenos = df_empreendimentos_pequenos.drop(columns=['AnmPeriodoReferencia', 'CodClasseConsumo','CodSubGrupoTarifario', 'CodUFibge', 'CodRegiao'
                                                      'SigModalidadeEmpreendimento'], errors='ignore')
df_empreendimentos_pequenos['NumCNPJDistribuidora'] = df_empreendimentos_pequenos['NumCNPJDistribuidora'].astype(str)
df_empreendimentos_pequenos['NomSubEstacao'] = df_empreendimentos_pequenos['NomSubEstacao'].fillna('Sem Subestação')
df_empreendimentos_pequenos['NomMunicipio'] = df_empreendimentos_pequenos['NomMunicipio'].fillna('Sem informação')
df_empreendimentos_pequenos['DscSubGrupoTarifario'] = df_empreendimentos_pequenos['DscSubGrupoTarifario'].replace(dict_tarifado)
# print(df_empreendimentos_pequenos.info())
  
  
if __name__ == "__main__":
    from sql import executar_sql
    executar_sql()