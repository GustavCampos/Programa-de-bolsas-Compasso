from os.path import join, dirname, realpath


def main():
    file_location = realpath(join(dirname(__file__), "number.txt"))
    
    with open(file_location, "r") as file:
        # Cria uma lista com os números do arquivo
        numbers = file.readlines()
        
    # Convertendo os números para inteiros    
    normalized_numbers = map(int, numbers)

    # Ordenando os valores em ordem decrescente
    sorted_numbers = sorted(normalized_numbers, reverse=True)
    
    # Filtrando os números pares apenas
    even_numbers = filter(lambda n: n%2 == 0, sorted_numbers)

    # 5 Highest even numbers
    top_5 = (list(even_numbers)[:5])

    print(top_5)
    print(sum(top_5))
    
main()