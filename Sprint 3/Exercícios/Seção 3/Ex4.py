def is_prime(n):
    if n == 1: return False
    
    for c in range(2, n):
        if n % c == 0:
            return False
    return True

for c in range(1, 101):
    if is_prime(c):
        print(c)
