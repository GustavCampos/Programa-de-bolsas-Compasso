import json
import pandas as pd
from os.path import join, dirname, realpath


def process_date(date: str) -> str | None:
    if not date:
        return None
    
    day, month, year = date.split("/")
    
    return f"{year}-{month}-{day}"

def process_cnpj(cnpj: str) -> str | None:
    if not cnpj:
        return None
    
    return cnpj.replace(".", "").replace("/", "").replace("-", "")

def main():
    LOCATION = dirname(realpath(__file__))
    JSON_FILE = join(LOCATION, "data.json")
    CSV_FILE = join(LOCATION, "data.csv")
    
    # JSON attributes:
    CNPJ = "CNPJ_PROGRAMADORA"
    NR_ID = "NR_IDENTIFICACAO"
    START_DATE = "DATA_INICIO_OFERTA"
    
    with open(JSON_FILE, "r") as file:
        json_dict = json.load(file)
        main_df = pd.DataFrame(json_dict["data"])
    
    main_df[CNPJ] = main_df[CNPJ].apply(process_cnpj)
    
    main_df[NR_ID] = main_df[NR_ID].apply(
        lambda n: (n.replace(".", ""), None)[not n]
    )
    
    main_df[START_DATE] = main_df[START_DATE].apply(process_date)
    
    print("Data processing complete!")
    
    main_df.to_csv(CSV_FILE, encoding="utf-8", index=False)

if __name__ == "__main__":
    main()