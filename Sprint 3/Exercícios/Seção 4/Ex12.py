def my_map(l, f) -> list:
    new_list = []
    
    for c in l:
        new_list.append(f(c))
    
    return new_list

lista_teste = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

print(my_map(lista_teste, lambda n: n**2))
