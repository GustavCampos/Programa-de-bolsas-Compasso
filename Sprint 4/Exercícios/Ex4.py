def calcular_valor_maximo(operadores: list, operandos: list[tuple]) -> float:
    eval_list = map(lambda n: eval("{1}{0}{2}".format(n[0], *n[1])), zip(operadores, operandos))
    
    return max(eval_list)

def main():
    operadores = ['+','-','*','/','+']
    operandos  = [(3,6), (-7,4.9), (8,-8), (10,2), (8,4)]
    
    print(calcular_valor_maximo(operadores, operandos))

main()