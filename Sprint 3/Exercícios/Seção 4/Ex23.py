class Calculo(object):
    def soma(self, x, y):
        return x + y
    
    def subtracao(self, x, y):
        return x - y
    
def main():
    calculo = Calculo()
    x = 4
    y = 5
    
    print(f"Somando: {x}+{y} = {calculo.soma(x, y)}")
    print(f"Subtraindo: {x}-{y} = {calculo.subtracao(x, y)}")
    
main()
