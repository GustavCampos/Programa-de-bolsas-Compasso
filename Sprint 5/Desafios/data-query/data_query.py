import boto3
from os.path import join, dirname, realpath


def get_credentials(credentials_file: str) -> dict:
    return_dict = {}
    
    with open(credentials_file, 'r') as file:
        for line in file.readlines():
            key, value = line.strip().replace("\n", "").split("=")
            return_dict[key.upper()] = value
            
    return return_dict

def main():
    LOCATION = dirname(realpath(__file__))
    AWS_CREDENTIALS = get_credentials(join(LOCATION, "aws_credentials.env"))
    
    print(AWS_CREDENTIALS)

if __name__ == '__main__':
    main()