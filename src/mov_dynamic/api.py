# src/mov_dynamic/api.py

import requests
import os
import pandas as pd

SAVE_PATH = '/home/nishtala/data/mov_dynamic'

def get_key():
    key = os.getenv("MOVIE_API_KEY") 
    return key

def gen_url(dt="2023", pagenum):
    base_url = "http://kobis.or.kr/kobisopenapi/webservice/rest/movie/searchMovieList.json?"
    key = get_key()
    pg = str(pagenum)
    url = f"{base_url}?key={key}&openStartDt={dt}&openEndDt={dt}&curPage={pg}"
    return url

def request(dt="2023", pagenum=1):
    url = gen_url(dt, pagenum)
    r = requests.get(url)
    data = r.json()
    return data

def save_json(dt="2023", parq_path=SAVE_PATH):
    for i in range(1, 11):
        data = request(dt)
        data_list = data['boxOfficeResult']['dailyBoxOfficeList']
        df = pd.DataFrame(data_list)
        df['page'] = i
        df.to_parquet(parq_path, partition_cols=['page'])
        print(df.head(5))
        print("*"*30)
    return
