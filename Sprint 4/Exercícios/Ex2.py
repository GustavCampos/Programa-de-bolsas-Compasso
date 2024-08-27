def conta_vogais(texto: str)-> int:
    only_vogals = filter(lambda n: n in ["a", "e", "i", "o", "u"], texto.lower())
    return len(list(only_vogals))

def main():
    print(conta_vogais("Hello World"))
    
main()