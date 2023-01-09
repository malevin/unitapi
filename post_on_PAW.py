import requests
from os import walk 
from os.path import abspath, dirname

username = 'pavelmalevin'
token = '50872cbedb63ef8730058f34022f10fcb3e4e940'
path_to_file='/home/pavelmalevin/web/unitapi'
paw_domen='https://www.pythonanywhere.com'

def post_file(filename, filecontent):
    response = requests.post(
        paw_domen+rf'/api/v0/user/{username}/files/path{path_to_file}'+rf'/{filename}',
        headers={'Authorization': f'Token {token}'},
        files={'content': rf'{filecontent}'}
    )
    print(response.status_code, response.content, sep="\n\n")

def get_files_in_dir():
    mypath = dirname(abspath(__file__))
    filenames = next(walk(mypath), (None, None, []))[2]  # [] if no file
    return filenames

for i in get_files_in_dir():
#     with open(i, 'rb') as f:
    with open(i) as f:
        s = f.read()
    post_file(i, s)
