class Passaro(object):
    def voar(self) -> None:
        print("Voando...")
        
class Pato(Passaro):
    def emitir_som(self) -> None:
        print("Pato emitindo som...")
        print("Quack Quack")
        
class Pardal(Passaro):
    def emitir_som(self) -> None:
        print("Pardal emitindo som...")
        print("Piu Piu")
        
def main():
    pato = Pato()
    pato.voar()
    pato.emitir_som()
    
    pardal = Pardal()
    pardal.voar()
    pardal.emitir_som()
    
main()
