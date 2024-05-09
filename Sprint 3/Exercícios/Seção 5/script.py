from os.path import join as joinpath, dirname, realpath
import re as regex


# Separa itens da linha usando regex
def split_line(line: str) -> list:
    # Regex para encontrar os valores entre as vírgulas
    pattern = r"(\".*\")|([^,]+)"

    # Remove o caractere de quebra de linha e depois remove os espaços
    line_strip = line.strip("\n").strip(" ")

    # Obtém uma lista onde cada item é uma tupla (regra_1_match, regra_2_match)
    # Apenas um elemento em cada tupla terá conteúdo
    pattern_match = regex.findall(pattern, line_strip)
    
    # Retorna uma lista com o elemento da tupla que não é vazio
    return list(map(lambda n: (n[0], n[1])[not n[0]], pattern_match))
    
# Responsável por converter csv file.readlines() em uma
# lista de dicionários para cada linha 
def process_csv(csv_lines: list) -> list[dict]:
    response = [] 
    
    columns = split_line(csv_lines[0])
    
    for line in csv_lines[1:]:        
        splitted_items = split_line(line)
        
        # Remove espaços externos em cada item
        stripped_items = list(map(str.strip, splitted_items))
        
        # Converte itens para tipo correto
        formatted_line = [
            stripped_items[0], 
            float(stripped_items[1]), 
            int(stripped_items[2]), 
            float(stripped_items[3]), 
            stripped_items[4], 
            float(stripped_items[5])
        ]
        
        # Cria um dicionário com chave=nome_da_coluna e valor=item_na_lista
        response.append({c: i for c, i in zip(columns, formatted_line)})
        
    return response

def main():
    # Obtendo o caminho absoluto para este arquivo
    _location = realpath(dirname(__file__))
    
    # Processamento do CSV_____________________________________________________
    print("Processando csv...")
    
    with open(joinpath(_location, "actors.csv"), "r") as file:
        csv_content = process_csv(file.readlines())
    
    print("Processamento csv finalizado.")
    
    # Criando arquivos para registrar as respostas_____________________________
    arquivos = []

    for c in range(1, 6):
        arquivos.append(open(joinpath(_location, f"etapa-{c}.txt"), "w", encoding="utf-8"))
        print(f"Arquivo etapa-{c}.txt criado.")
        
        
    # Etapa 1 - Ator/atriz com maior número de filmes gravados________________
    
    # Ordena csv_content por "Number of Movies" e obtém o primeiro elemento
    stage_1_query_line = sorted(csv_content, key=lambda n: n["Number of Movies"], reverse=True)[0]
    
    arquivos[0].write(f"{stage_1_query_line["Actor"]} é o ator/atriz que mais gravou filmes e possui {stage_1_query_line["Number of Movies"]} participações.")
    
    print(f"Arquivo {arquivos[0].name} Registrado.")
     
    # Etapa 2 - Média do faturamento dos principais filmes_____________________
    gross_mean = sum([n["Gross"] for n in csv_content]) / len(csv_content)
    
    arquivos[1].write(f"A média de receita de bilheteria bruta dos principais filmes é cerca {gross_mean} milhões de dólares")

    print(f"Arquivo {arquivos[1].name} Registrado.")
    
    # Etapa 3 - Ator/atriz com a maior média de faturamento por filme__________
    
    # Ordena csv_content por "Average per Movie" e obtém o primeiro elemento
    stage_3_query_line = sorted(csv_content, key=lambda n: n["Average per Movie"], reverse=True)[0]
    
    arquivos[2].write(f"O ator com maior média de receita de bilheteria bruta é o(a) {stage_3_query_line["Actor"]}, possuindo {stage_3_query_line["Average per Movie"]} milhões de dólares por média de bilheteria em seus filmes.")
    
    print(f"Arquivo {arquivos[2].name} Registrado.")
    
    # Etapa 4 - #1 Filme com maior número de aparições_________________________
    
    # Cria um dicionário onde cada chave é o nome do filme e 
    # adiciona cada linha correspondente a ele
    group_by_best_movie_query = {}
    for line in csv_content:        
        key = line['#1 Movie']
        
        if key in group_by_best_movie_query.keys():
            group_by_best_movie_query[key].append(line)
        else:
            group_by_best_movie_query[key] = [line]
    
    # Mapeia o group_by_best_movie_query para um dicionário "chave: contagem(valores)"
    best_movie_count = {key: len(values) for key, values in group_by_best_movie_query.items()}
        
    # Ordena pela quantidade de aparições em ordem decrescente, depois por nome
    # do filme em ordem crescente
    sorted_best_movie_count = sorted(best_movie_count.items(), key=lambda n: (-n[1], n[0].lower()))
    
    # Escreve as informações no arquivo
    for c in range(len(sorted_best_movie_count)):
        current_item = sorted_best_movie_count[c]
        arquivos[3].write(f"{c + 1:02} - O filme {current_item[0]} aparece {current_item[1]} vez(es) no dataset\n")
        
    print(f"Arquivo {arquivos[3].name} Registrado.")
    
    # Etapa 5 - Ordenar atores por faturamento total___________________________
    
    #Ordena csv_content por "Total Gross"
    stage_5_query = sorted(csv_content, key=lambda n: n["Total Gross"], reverse=True)
    
    # Escreve as informações no arquivo
    for line in stage_5_query:
        arquivos[4].write(f"{line["Actor"]} - {line["Total Gross"]}\n")
        
    print(f"Arquivo {arquivos[4].name} Registrado.")
    
    # Finalizando o programa___________________________________________________ 
    print("Encerrando programa....")
    
    # Encerrando objetos de arquivo criados
    for file in arquivos:
        file.close()

    print("Programa finalizado.")

main()