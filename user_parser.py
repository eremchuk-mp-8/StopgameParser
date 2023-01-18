import numpy as np
import pandas as pd
from pandas import DataFrame
import requests
from bs4 import BeautifulSoup as BS
from hashlib import sha256
import multiprocessing as mp
import time
from datetime import datetime

def parse_user(username):
    '''
    Arguments:
        - username
    Returns dataframe object with information about user
    '''
    domain = "https://stopgame.ru"
    user_string = {'username': [sha256(str(username).encode()).hexdigest()]}
    try:
        response = requests.get(f"{domain}/user/{username}")
        soup = BS(response.content, "lxml")
        user_string['rating'] = [int(soup.find(class_ = "_rating-row_k5w6g_134").next.next.next)]
        user_string['subscribers'] = [int(soup.find(class_ = "_subscribers-row_k5w6g_429").next.next.find(name="span").next)]
        for i in soup.find_all(class_ = "_tab_k5w6g_451"):
            if i.find("sup"):
                user_string[i.next.text.strip()] = [i.find('sup').next]      
        for i in range(4):
            user_string[soup.find_all(class_ = "_stats-container__row_k5w6g_1 _stats-container__row--game_k5w6g_1")[i].text.strip()] = soup.find_all(class_ = "_stats-container__number_k5w6g_1")[i].text.strip()
        user_string['top1'] = None
        user_string['top2'] = None
        user_string['top3'] = None
        user_string['top4'] = None
        if soup.find(class_ = "_my-top__grid_k5w6g_1") is not None:
            top = soup.find(class_ = "_my-top__grid_k5w6g_1").find_all(class_ = "_card_67304_1")
            for i in range(len(top)):
                user_string[f"top{i+1}"] = top[i].get('href')[6:]  
    except:
        f = open('errorlog.txt', 'a')
        f.write(f"{domain}/user/{username} parsing failed\n")
        f.close()
    else:
        print(f"{username} parsed")
        return DataFrame(user_string)

if __name__ == '__main__':
    path = input()
    users = pd.read_csv(path)
    start_time = time.time()
    df = DataFrame()
    f = open('errorlog.txt', 'a')
    f.write(f"----{datetime.now()}----\n")
    f.close()
    for i in users.username:
        obj = parse_user(i)
        if obj.empty == False:
            df = df.append(obj, ignore_index = True)
    df.to_csv('users.csv', index=False)
    t = time.time()-start_time
    print(f"Execution time: {t//60} m, {t%60} s")
