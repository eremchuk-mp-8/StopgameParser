import numpy as np
import pandas as pd
from pandas import DataFrame
import requests
from bs4 import BeautifulSoup as BS
from hashlib import sha256
import multiprocessing as mp

def parse_games(username):
    '''
    Arguments:
        - username
    Returns dataframe object with rated games collection of user
    '''
    domain = "https://stopgame.ru"
    gamenames = []
    user_ratings = []
    try:
        response = requests.get(f"{domain}/user/{username}/games/rated")
        soup = BS(response.content, "lxml")
        for i in soup.find_all(class_ = "_container_1mcqg_1"):
            for j in i.find_all(class_ = 'item'):
                response = requests.get(f"{domain}{j.get('href')}")
                soup = BS(response.content, "lxml")
                for k in soup.find_all(class_ = "_card_67304_1"):
                    colors = ['green', 'yellow-rating', 'brand-red']
                    arr = []
                    for l in range(len(colors)):
                        if len(k.find_all(style=f"--star-color: var(--{colors[l]})")) != 0:
                            arr.append(k.find_all(style=f"--star-color: var(--{colors[l]})")[0])
                    user_rating = len(arr[0].find_all('svg'))
                    if str(arr[0].find_all('svg')[len(arr[0].find_all('svg'))-1]).find("0 0 288 512") != -1:
                        user_rating-=0.5
                    gamenames.append(k.get('href')[6:])
                    user_ratings.append(user_rating)
    except:
        print("something went wrong")
    user_codes = np.full(len(gamenames), sha256(str(username).encode()).hexdigest())      
    obj = DataFrame({'id': user_codes,'gamename': gamenames, 'user_rating': user_ratings})
    return obj

if __name__ == '__main__':
    users = pd.read_csv('users.csv')
    df = DataFrame(columns=['id', 'gamename', 'user_rating'])
    pool = mp.Pool(mp.cpu_count())
    buffer = pool.map(parse_games, users.username)
    pool.close()
    for obj in buffer:
        if obj.empty == False:
            df = df.append(obj, ignore_index = True)
    df.to_csv('collection.csv', index=False)
