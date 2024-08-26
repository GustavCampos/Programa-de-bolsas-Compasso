import json

with open('person.json', 'r') as file:
    json_obj = json.load(file)
    
    print(json_obj)

    file.close()
