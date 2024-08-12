import lambda_function
import dotenv

def main():
    dotenv.load_dotenv()
        
    print(lambda_function.lambda_handler({
        "key1": "value1",
        "key2": "value2"
    }, None))
    
if __name__ == "__main__":
    main()