import requests


#this function downloads the file from the url using request
def download_file(file):
    res = requests.get(f'https://data.gharchive.org/{file}')
    return res

