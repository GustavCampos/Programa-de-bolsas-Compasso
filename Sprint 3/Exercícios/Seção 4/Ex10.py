def generateListBySet(n: list) -> list:
    return list(set(n))

lista = ['abc', 'abc', 'abc', '123', 'abc', '123', '123']
print(generateListBySet(lista))
