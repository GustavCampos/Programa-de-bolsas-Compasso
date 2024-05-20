with open("arquivo_texto.txt", 'r', encoding='utf8') as file:
    print(file.read(), end='')
    file.close()
