import random 
# amostra aleatoriamente 50 números do intervalo 0...500
random_list = random.sample(range(500),50)

random_list.sort()
lenght = len(random_list)
is_even = (lenght % 2 == 0)
mid_index = ((lenght // 2), (lenght // 2) - 1)[is_even]  

valor_minimo = min(random_list)
valor_maximo = max(random_list)
media = sum(random_list) / lenght

mediana = (random_list[mid_index], (random_list[mid_index] + random_list[mid_index + 1]) / 2)[is_even]

print(f"Media: {media}, Mediana: {mediana}, Mínimo: {valor_minimo}, Máximo: {valor_maximo}")
