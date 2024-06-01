## First Class Functions

### Properties

1. **Assigned to Variables:**
   Functions can be assigned to variables. The function can then be called using the variable name.
   
   ```python
   def greet(name):
       return f"Hello, {name}!"
   
   say_hello = greet
   print(say_hello("Alice"))  # Output: Hello, Alice!
   ```

2. **Stored in Data Structures:**
   Functions can be stored in data structures like lists, dictionaries, etc.
   
   ```python
   def greet(name):
       return f"Hello, {name}!"
   
   def farewell(name):
       return f"Goodbye, {name}!"
   
   actions = [greet, farewell]
   
   for action in actions:
       print(action("Charlie"))
   # Output:
   # Hello, Charlie!
   # Goodbye, Charlie!
   ```

## Higher Order Functions

### Properties

1. **Passed as Arguments:**
   Functions can be passed as arguments to other functions.
   
   ```python
   def call_function(func, value):
       return func(value)
   
   def greet(name):
       return f"Hello, {name}!"
   
   result = call_function(greet, "Bob")
   print(result)  # Output: Hello, Bob!
   ```

2. **Returned from Other Functions:**
   Functions can return other functions.
   
   ```python
   def outer_function():
       def inner_function():
           return "Hello from inner function!"
       return inner_function
   
   my_func = outer_function()
   print(my_func())  # Output: Hello from inner function!
   ```

## Closures

A function that captures the bindings of free variables in its lexical context.

### Components

1. **Nested Function:** A function defined inside another function.
2. **Free Variables:** Variables from the outer function that are used in the nested function.
3. **Returning the Nested Function:** The outer function returns the nested function, which then becomes a closure.

### Example

```python
def outer_function(message):
    def inner_function():
        print(message)
    return inner_function

# Create a closure
closure = outer_function("Hello, World!")
closure()  # Output: Hello, World!
```

In this example:
- `outer_function` takes a parameter `message`.
- `inner_function` is defined inside `outer_function` and uses `message`.
- `outer_function` returns `inner_function`.
- The `closure` variable holds a reference to `inner_function` along with the environment that includes the `message` variable.
