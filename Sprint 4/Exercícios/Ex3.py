from functools import reduce


def calcula_saldo(lancamentos: list[tuple]) -> float:
    normalized_values = map(lambda n: (int(n[0]), n[1]), lancamentos)
    
    return reduce(
        lambda acc, n: ((acc + n[0]), (acc - n[0]))[n[1] == "D"] ,
        normalized_values, 
        0
    )

def main():
    lancamentos = [
        (200,'D'),
        (300,'C'),
        (100,'C')
    ]
    
    print(calcula_saldo(lancamentos))
    
main()