def func(*args, **kwargs):
    for value in args:
        print(value)
    
    for key, value in kwargs.items():
        print(value)
        
func(1, 3, 4, 'hello', parametro_nomeado='alguma coisa', x=20)
