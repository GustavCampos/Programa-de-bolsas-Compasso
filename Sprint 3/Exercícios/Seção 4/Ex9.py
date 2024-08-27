primeirosNomes = ['Joao', 'Douglas', 'Lucas', 'José']
sobreNomes = ['Soares', 'Souza', 'Silveira', 'Pedreira']
idades = [19, 28, 25, 31]

for c in enumerate(zip(primeirosNomes, sobreNomes, idades)):
    print(f"{c[0]} - {c[1][0]} {c[1][1]} está com {c[1][2]} anos")
