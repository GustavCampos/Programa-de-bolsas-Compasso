from hashlib import sha1
from sys import argv

def main():
    if len(argv) <= 1:
        print("No arguments provided")
        return

    for arg in argv[1:]:
        input_hash = sha1(arg.encode(encoding="ascii"))
        print(input_hash.hexdigest())
    
main()