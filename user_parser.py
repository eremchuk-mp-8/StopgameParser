import pandas as pd
from pandas import DataFrame
import requests
from bs4 import BeautifulSoup as BS
import time
from datetime import datetime

user_rating_row = "_rating-row_zz15s_149"
user_subsrcibers_row = "_subscribers-row_zz15s_467"
user_tab = "_tab_zz15s_490"
user_top_grid = "_my-top__grid_zz15s_1"
user_top_card = "_card_1ovwy_1"
user_inner_tab = "_inner-tabs_vrdym_328"
user_games_tab = "_inner-tab_vrdym_328 _games-tab_zz15s_1483"

def parse_user(username):
    '''
    Arguments:
        - username
    Returns dataframe object with information about user
    '''
    domain = "https://stopgame.ru"
    user_string = {'username': [str(username)]}
    try:
        response = requests.get(f"{domain}/user/{username}")
        soup = BS(response.content, "lxml")
        user_string['rating'] = [int(soup.find(class_ = user_rating_row).next.next.next)]
        user_string['subscribers'] = [int(soup.find(class_ = user_subsrcibers_row).next.next.find(name="span").next)]
        
        # парсинг разделов профиля
        for i in soup.find_all(class_ = user_tab):
            if i.find("sup"):
                user_string[str(i.next).strip()] = [int(i.find('sup').next)]
                
        # парсинг топа игра (если есть)
        user_string['top1'] = None
        user_string['top2'] = None
        user_string['top3'] = None
        user_string['top4'] = None
        if soup.find(class_ = user_top_grid) is not None:
            top = soup.find(class_ = user_top_grid).find_all(class_ = user_top_card)
            for i in range(len(top)):
                user_string[f"top{i+1}"] = top[i].get('href')[6:]
        
        # парсинг статусов игр
        response = requests.get(f"{domain}/user/{username}/games/all")
        soup = BS(response.content, "lxml")
        for i in soup.find_all(class_ = user_games_tab):
          user_string[i.get('title')e] = [int(i.text)]
    except:
        f = open('errorlog.txt', 'a')
        f.write(f"{username} parsing failed\n")
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
    f.write(f"----Parsing users {datetime.now()}----\n")
    f.close()
    for i in users.username:
        obj = parse_user(i)
        if obj is not None:
            df = pd.concat([df,obj])
    dt = datetime.now()
    df.to_csv(f'games_{dt.day}_{dt.month}_{dt.year}.csv', index=False)
    t = time.time()-start_time
    print(f"Execution time: {t//60} m, {t%60} s")
