lista_palavras = ['maça', 'arara', 'audio', 'radio', 'radar', 'moto']

for palavra in lista_palavras:
    if palavra == palavra[::-1]:
        print(f"A palavra: {palavra} é um palíndromo")
    else:
        print(f"A palavra: {palavra} não é um palíndromo")
