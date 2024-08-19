# src/mov_dynamic/api.py

import requests
import os
import pandas as pd
import json

SAVE_PATH = '/home/nishtala/data/mov_dynamic'

def get_key():
    key = os.getenv("MOVIE_API_KEY") 
    return key

def gen_url(dt="2023", pagenum=1):
    base_url = "http://kobis.or.kr/kobisopenapi/webservice/rest/movie/searchMovieList.json"
    key = get_key()
    pg = str(pagenum)
    url = f"{base_url}?key={key}&openStartDt={dt}&openEndDt={dt}&curPage={pg}"
    print(f"Checking url: {url}")
    return url

def request(dt="2023", pagenum=1):
    url = gen_url(dt, pagenum)
    r = requests.get(url)
    data = r.json()
    return data

def save_json(dt="2023", save_path=SAVE_PATH):
    saving = []
    for i in range(1, 11):
        raw = request(dt, i)
        data = raw['movieListResult']['movieList']
        saving.extend(data)

    with open(f"{save_path}/{dt}", "w", encoding="utf-8") as f:
        json.dump(saving, f, indent=4, ensure_ascii=False)

    return
