def get_3_sublists(subject: list) -> list:
    offset = int(len(subject) / 3)
        
    list_1 = subject[:offset]
    list_2 = subject[offset:(offset*2)]
    list_3 = subject[(offset*2):]    
        
    return [list_1, list_2, list_3]
    
lista = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
listas = get_3_sublists(lista) 
print(listas[0], listas[1], listas[2])
