import requests
import s3_functions as s3f


def get_api_modifier(request_parameters, progress_json) -> str | None:
    if progress_json["movieCurrentPage"] < request_parameters["movie"]["maxPage"]:
        return "movie"
    elif progress_json["tvCurrentPage"] < request_parameters["tv"]["maxPage"]:
        return "tv"
    else:
        return None

def generate_json_files(start_page: int):
    pass