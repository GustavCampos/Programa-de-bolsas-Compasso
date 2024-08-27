class Pessoa(object):
    def __init__(self, id: int) -> None:
        self.__nome = None
        self.id = id
        
    @property
    def nome(self) -> str:
        return self.__nome
    
    @nome.setter
    def nome(self, nome):
        self.__nome = nome
        
    @nome.getter
    def nome(self) -> str:
        return self.__nome
