import requests
from constants import API_MOVIE_MODIFIER, API_SERIES_MODIFIER

def get_api_modifier(request_parameters, progress_json) -> str | None:
    if progress_json[f"{API_MOVIE_MODIFIER}CurrentPage"] <= request_parameters[API_MOVIE_MODIFIER]["maxPage"]:
        return API_MOVIE_MODIFIER
    elif progress_json[f"{API_SERIES_MODIFIER}CurrentPage"] <= request_parameters[API_SERIES_MODIFIER]["maxPage"]:
        return API_SERIES_MODIFIER
    else:
        return None

def generate_json_files(
    start_page: int, end_page: int, request_url: str, request_header: dict) -> dict:
    
    result_list = []
    for page in range(start_page, (end_page + 1)):
        new_url = f"{request_url}&page={page}"
        
        response = requests.get(new_url, headers=request_header)
        
        if response.status_code != 200:
            return {"status": "error", "code": response.status_code, "message": response.text}
        
        result_list.extend(response.json()["results"])
        
    return {
        "status": "success",
        "newCurrentPage": (end_page + 1),
        "json": {
            "startPage": start_page,
            "endPage": end_page,
            "registryCount": len(result_list),
            "results": result_list
        }
    }
    