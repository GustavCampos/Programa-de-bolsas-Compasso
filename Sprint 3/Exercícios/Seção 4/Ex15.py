class Lampada(object):
    def __init__(self, ligada: bool=False) -> None:
        self.ligada = ligada
    
    def liga(self) -> None:
        self.ligada = True
        
    def desliga(self) -> None:
        self.ligada = False
        
    def esta_ligada(self) -> bool:
        return self.ligada
    
def main():
    lampada = Lampada()
    lampada.liga()
    print(f"A lâmpada está ligada? {lampada.esta_ligada()}")
    lampada.desliga()
    print(f"A lâmpada ainda está ligada? {lampada.esta_ligada()}")
    
main()
