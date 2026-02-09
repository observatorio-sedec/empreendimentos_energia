import requests as rq
from openpyxl import load_workbook
import re
import polars as pl


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
print(f"Baixando arquivo grande de: {url_download}...")
response = rq.get(url_download)
if response.status_code == 200:
    print("Download concluído. Salvando arquivo...")
    with open(arquivo_destino, "wb") as file:
        file.write(response.content)
else:
    print(f"Erro ao baixar o arquivo: {arquivo_destino}. Código: {response.status_code}")
    
print("Lendo CSV com Polars...", flush=True)
# Lendo como CSV (mesmo com extensão .xlsx o arquivo original é CSV)
df_empreendimentos_grandes = pl.read_csv(arquivo_destino, separator=';', encoding='latin1', infer_schema_length=1000)
print("Transformando dados...", flush=True)
df_empreendimentos_grandes = df_empreendimentos_grandes.with_columns([
    pl.col("DscSubBacia").str.replace(r'^\s*\d+\s*-\s*', '').fill_null("Sem Informação"),
    pl.col("IdcGeracaoQualificada").fill_null("Sem Informação"),
    # Limpeza numérica: garante cast para string antes de substituir vírgula
    pl.col("MdaPotenciaOutorgadaKw").cast(pl.Utf8).str.replace(",", ".").cast(pl.Float64),
    pl.col("MdaPotenciaFiscalizadaKw").cast(pl.Utf8).str.replace(",", ".").cast(pl.Float64),
    pl.col("MdaGarantiaFisicaKw").cast(pl.Utf8).str.replace(",", ".").cast(pl.Float64)
])
df_empreendimentos_grandes  = df_empreendimentos_grandes.rename({'DscMuninicpios': 'DscMunicipios'})
df_empreendimentos_grandes = df_empreendimentos_grandes.with_columns([
    pl.col("DscMunicipios").str.replace(r'\s*-\s*.*$', '')
])
print("Primeiras linhas transformadas (Grandes):", flush=True)
print(df_empreendimentos_grandes.head(), flush=True)
# print(df_empreendimentos_grandes.glimpse())
# df_empreendimentos_grandes.write_excel("empreendimentos_grandes.xlsx")



url_download_pequeno = f"https://dadosabertos.aneel.gov.br/dataset/5e0fafd2-21b9-4d5b-b622-40438d40aba2/resource/b1bd71e7-d0ad-4214-9053-cbd58e9564a7/download/empreendimento-geracao-distribuida.csv"
arquivo_destino_pequeno = "empreendimentos_pequenos.xlsx"
print(f"Baixando arquivo pequeno de: {url_download_pequeno}...")
response = rq.get(url_download_pequeno)
if response.status_code == 200:
    print("Download concluído. Salvando arquivo...")
    with open(arquivo_destino_pequeno, "wb") as file:
        file.write(response.content)
else:
    print(f"Erro ao baixar o arquivo: {arquivo_destino_pequeno}. Código: {response.status_code}")
    
print("Lendo CSV pequeno com Polars...", flush=True)
df_empreendimentos_pequenos = pl.read_csv(arquivo_destino_pequeno, separator=';', encoding='latin1', infer_schema_length=1000)
print("Transformando dados...", flush=True)
df_empreendimentos_pequenos = df_empreendimentos_pequenos.drop('AnmPeriodoReferencia', 'CodClasseConsumo','CodSubGrupoTarifario', 'CodUFibge', 'CodRegiao', 'SigModalidadeEmpreendimento')
df_empreendimentos_pequenos = df_empreendimentos_pequenos.with_columns([
    pl.col("NumCNPJDistribuidora").cast(pl.Utf8),
    pl.col("NomSubEstacao").fill_null('Sem Subestação'),
    pl.col("NomMunicipio").fill_null('Sem informação'),
    pl.col("DscSubGrupoTarifario").replace(dict_tarifado),
    pl.col("MdaPotenciaInstaladaKW").cast(pl.Utf8).str.replace(",", ".").cast(pl.Float64)
])
print("Resumo (Pequenos):", flush=True)
print(df_empreendimentos_pequenos.glimpse(), flush=True)
# df_empreendimentos_pequenos.write_excel("empreendimentos_pequenos.xlsx")
  
  
if __name__ == "__main__":
    from sql import executar_sql
    executar_sql()