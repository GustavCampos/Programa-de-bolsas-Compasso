## Operators

Ternary Operators
```python
#Reduced option:
var = (false_value, true_value)[condition]

#Same as 
var = true_value if condition else false_value
```

Identity operator
```python
#Check two variables refers to the same object
x = 3
y = 3
#True
x is y
```

## Loops

for else: iterates trought for loop and execute if else block if break clause is not called
```python
u_input = int(input())

for c in range(10):
    if u_input == c: break
else:
    print("Input is not in range(10)")
```