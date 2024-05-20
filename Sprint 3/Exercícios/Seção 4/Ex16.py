def sum_values(string: str) -> int:
    char_array = string.split(",")
    int_array = map(int, char_array)
        
    return sum(int_array)

string = "1,3,4,6,10,76"
print(sum_values(string))
