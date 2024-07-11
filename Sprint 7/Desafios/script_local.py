import requests
import json
from os.path import realpath, dirname, join


def calc_progress(cur_value: int, end_value: int, size=20) -> str:
    proportion = cur_value / end_value
    
    equals = "=" * round(size * proportion)
    spaces = " " * (size - len(equals))
    progress_bar = "[" + equals + spaces + "]"
    
    return f"{progress_bar} {cur_value}/{end_value} {proportion * 100:2.2f}%"

def generate_json_from_request(start_page: int, pages_per_file: int, header: dict, url: str) -> dict:
    results_list = []
    for page in range(start_page, start_page + pages_per_file):
        new_url = f"{url}&page={page}" 
        
        response = requests.get(new_url, headers=header)
        results_list.extend(response.json()["results"])
    
    return {
        "start_page": start_page,
        "results": results_list
    }

def generate_files_from_request(start_page: int, pages_per_file: int, number_of_files: int, 
                                header: dict, url: str, path_to_save: str=".") -> str:
    temp_start_page = start_page
    for file in range(number_of_files):
        end_page = temp_start_page + pages_per_file
        file_name = f"animacoes_{temp_start_page:04d}-{(end_page - 1):04d}.json"
        print(f"{calc_progress((file + 1), number_of_files)} Generating File: {file_name}")
        
        json_response = generate_json_from_request(
            start_page=         temp_start_page,
            pages_per_file=     pages_per_file,
            header=             header,
            url=                url
        )
        
        with open(join(path_to_save, file_name), "w") as file:
            json.dump(json_response, file, indent=4)
            
        temp_start_page = end_page
        
    return end_page


def main():
    LOCAL = dirname(realpath(__file__))
    PARAMETERS_PATH = join(LOCAL, "request_parameters.json")
    
    with open(PARAMETERS_PATH) as file:
        REQUEST_PARAMETERS = json.load(file)
        
    request_header = {
        "accept": "application/json",
        "Authorization": f"Bearer {REQUEST_PARAMETERS["tokenAPI"]}"
    }
        
    if REQUEST_PARAMETERS["movie"]['currentPage'] < REQUEST_PARAMETERS["movie"]['maxPage']:
        new_current_page = generate_files_from_request(
            start_page=         REQUEST_PARAMETERS["movie"]['currentPage'],
            pages_per_file=     REQUEST_PARAMETERS['pageGroup'],
            number_of_files=    REQUEST_PARAMETERS['filesPerCall'],
            header=             request_header,
            url=                REQUEST_PARAMETERS["movie"]['url'],
            path_to_save=       LOCAL
        )
        
        REQUEST_PARAMETERS["movie"]['currentPage'] = new_current_page
        
        with open(PARAMETERS_PATH, "w") as file:
            json.dump(REQUEST_PARAMETERS, file, indent=4)
            
    elif REQUEST_PARAMETERS["tv"]['currentPage'] < REQUEST_PARAMETERS["tv"]['maxPage']:
        new_current_page = generate_files_from_request(
            start_page=         REQUEST_PARAMETERS["tv"]['currentPage'],
            pages_per_file=     REQUEST_PARAMETERS['pageGroup'],
            number_of_files=    REQUEST_PARAMETERS['filesPerCall'],
            header=             request_header,
            url=                REQUEST_PARAMETERS["tv"]['url'],
            path_to_save=       LOCAL
        )
        
        REQUEST_PARAMETERS["tv"]['currentPage'] = new_current_page
        
        with open(PARAMETERS_PATH, "w") as file:
            json.dump(REQUEST_PARAMETERS, file, indent=4)
    else:
        print("Theres Nothing to Request!")
        
    print("Script finished!")

if __name__ == "__main__":
    main()