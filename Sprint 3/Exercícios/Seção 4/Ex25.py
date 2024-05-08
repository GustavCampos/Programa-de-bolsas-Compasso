class Aviao(object):
    def __init__(self, modelo: str, velocidade_maxima: float, capacidade: int, cor: str="azul"):
        self.modelo = modelo
        self.velocidade_maxima = velocidade_maxima
        self.capacidade = capacidade
        self.cor = cor
        
def main():
    avioes = [
        Aviao("BOIENG456", 1500, 400),
        Aviao("Embraer Praetor 600", 863, 14),
        Aviao("Antonov An-2", 258, 12)
    ]
    
    for a in avioes:
        print(f"O avião de modelo {a.modelo} possui uma velocidade máxima de {a.velocidade_maxima}, capacidade para {a.capacidade} passageiros e é da cor {a.cor}")
    
main()
