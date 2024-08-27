def maiores_que_media(conteudo: dict)->list:
    mean = sum(conteudo.values()) / len(conteudo)
    filtered_list = filter(lambda n: n[1] > mean, conteudo.items())
    
    return list(sorted(filtered_list, key=lambda n: n[1]))
    

def main():
    inp = {
        "arroz": 4.99,
        "feijão": 3.49,
        "macarrão": 2.99,
        "leite": 3.29,
        "pão": 1.99
    }
    
    print(maiores_que_media(inp))
main()