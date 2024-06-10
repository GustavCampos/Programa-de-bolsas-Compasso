import json
import pandas as pd
from os.path import join, dirname, realpath


def process_date(date: str) -> str | None:
    if not date:return None
    
    day, month, year = date.split("/")   
     
    return f"{year}-{month}-{day}"

def process_cnpj(cnpj: str) -> str | None:
    if not cnpj:return None
    return cnpj.replace(".", "").replace("/", "").replace("-", "")

def to_lower(string: str) -> str | None:
    if not string:return None
    return string.lower()

def to_upper(string: str) -> str | None:
    if not string:return None
    return string.upper()

def to_capitalize(string: str) -> str | None:
    if not string:return None
    return string.title()

def main():
    LOCATION = dirname(realpath(__file__))
    JSON_FILE = join(LOCATION, "data", "canais-de-programacao-de-programadoras-ativos-credenciados.json")
    CSV_FILE = join(LOCATION, "data", "canais-credenciados.csv")
    
    # JSON attributes:
    CHANNEL_NAME = "CANAL"
    CHANNEL_CLASS = "CLASSIFICACAO_CANAL"
    CNPJ = "CNPJ_PROGRAMADORA"
    COMPANY_CLASS = "CLASSIFICACAO_PROGRAMADORA"
    CHANNEL_ID = "NR_IDENTIFICACAO"
    CLIENT_OFFER = "OFERTA_CLIENTE"
    CHANNEL_DENSITY = "DENSIDADE_CANAL"
    COMPANY_COUNTRY = "PAIS_PROGRAMADORA"
    CONTENT_TYPE = "TIPO_CONTEUDO_CANAL"
    COMPANY_NAME = "NOME_PROGRAMADORA"
    START_DATE = "DATA_INICIO_OFERTA"
    
    print(f"Loading data from {JSON_FILE}...")
    with open(JSON_FILE, "r") as file:
        json_dict = json.load(file)
        main_df = pd.DataFrame(json_dict["data"])
    
    print(f"Formatting {CHANNEL_NAME} attribute...")
    main_df[CHANNEL_NAME] = main_df[CHANNEL_NAME].apply(to_upper)
    
    print(f"Formatting {CHANNEL_CLASS} attribute...")
    main_df[CHANNEL_CLASS] = main_df[CHANNEL_CLASS].apply(to_lower)
    
    print(f"Formatting {CNPJ} attribute...")
    main_df[CNPJ] = main_df[CNPJ].apply(process_cnpj)
    
    print(f"Formatting {COMPANY_CLASS} attribute...")
    main_df[COMPANY_CLASS] = main_df[COMPANY_CLASS].apply(to_lower)
    
    print(f"Formatting {CHANNEL_ID} attribute...")
    main_df[CHANNEL_ID] = main_df[CHANNEL_ID].apply(
        lambda n: (n.replace(".", ""), None)[not n]
    )
    
    print(f"Formatting {CLIENT_OFFER} attribute...")
    main_df[CLIENT_OFFER] = main_df[CLIENT_OFFER].apply(to_lower)
    
    print(f"Formatting {CHANNEL_DENSITY} attribute...")
    main_df[CHANNEL_DENSITY] = main_df[CHANNEL_DENSITY].apply(to_upper)
    
    print(f"Formatting {COMPANY_COUNTRY} attribute...")
    main_df[COMPANY_COUNTRY] = main_df[COMPANY_COUNTRY].apply(to_capitalize)
    
    print(f"Formatting {CONTENT_TYPE} attribute...")
    main_df[CONTENT_TYPE] = main_df[CONTENT_TYPE].apply(to_lower)
    
    print(f"Formatting {COMPANY_NAME} attribute...")
    main_df[COMPANY_NAME] = main_df[COMPANY_NAME].apply(to_upper)
    
    print(f"Formatting {START_DATE} attribute...")
    main_df[START_DATE] = main_df[START_DATE].apply(process_date)
    
    print(f"Writing data as CSV to {CSV_FILE}...")
    main_df.to_csv(CSV_FILE, encoding="utf-8", index=False)
    
    print("Data processing complete!")

if __name__ == "__main__":
    main()