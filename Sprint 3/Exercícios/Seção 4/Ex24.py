class Ordenadora(object):
    def __init__(self, listaBaguncada: list) -> None:
        self.listaBaguncada = listaBaguncada
        
    def ordenacaoCrescente(self) -> list:
        return sorted(self.listaBaguncada)
    
    def ordenacaoDecrescente(self) -> list:
        return sorted(self.listaBaguncada, reverse=True)
    
def main():
    crescente = Ordenadora([3,4,2,1,5])
    decrescente = Ordenadora([9,7,6,8])
    
    print(crescente.ordenacaoCrescente())
    print(decrescente.ordenacaoDecrescente())
    
main()
