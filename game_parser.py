from pandas import DataFrame
import requests
from bs4 import BeautifulSoup as BS
import time
from datetime import datetime

def parse_game(gamename):
    '''
    Arguments:
        - gamename
    Returns dataframe object with information about game
    '''
    domain = "https://stopgame.ru"
    game_string = {'gamename': [gamename], 'link': [f"{domain}/game/{gamename}"]}
    try:
        response = requests.get(f"{domain}/game/{gamename}")
        soup = BS(response.content, "lxml")
        for i in soup.find(class_='_page-section_1kff8_409').find_all('li'):
          if i.find("sup"):
            arr = i.text.split()
            game_string[arr[0]] = [arr[1]]
        titles = soup.find_all(class_='_info-grid__title_6ftvk_197')
        values = soup.find_all(class_='_info-grid__value_6ftvk_198')
        for i in range(len(titles)):
          game_string[titles[i].text.strip()] = [values[i].text.strip()]
        game_string['subscribers'] = [float(soup.find(class_="_subscribers-info_6ftvk_346").text.strip())]
        count_str = soup.find(class_="_users-rating__count_6ftvk_1").text.strip().split()
        result=''
        for i in count_str:
          if i.isnumeric():
            result+=i
        game_string['rating_count'] = [float(result)]
        game_string['rating'] = [float(soup.find(class_="_users-rating__total_6ftvk_1").text.strip())]
        for i in soup.find_all(class_='_graph__pillar-container_6ftvk_658'):
          game_string[f"star{i.find('span').text.strip()}"] = [float(i.get('title').strip().split()[0])]
        sg = soup.find(class_='_sg-rating_6ftvk_430')
        if sg is not None:
          game_string['sg_rating'] = [sg.next.get('href')[9:]]
    except:
        f = open('errorlog.txt', 'a')
        f.write(f"{domain}/game/{gamename} parsing failed\n")
        f.close()
    else:
        return DataFrame(game_string)

if __name__ == '__main__':
    domain = "https://stopgame.ru"
    start_time = time.time()
    df = DataFrame()
    f = open('errorlog.txt', 'a')
    f.write(f"----Parsing games {datetime.now()}----\n")
    f.close()
    response = requests.get(f"{domain}/games/old")
    soup = BS(response.content, "lxml")
    pages = int(soup.find(class_="prev last").get("data-page"))+1
    for i in range(1,pages+1):
        response = requests.get(f"{domain}/games/old?p={i}")
        soup = BS(response.content, "lxml")
        for j in soup.find(class_="_games-grid_v95ji_304").find_all(class_="_card_67304_1"):
            obj = parse_game(j.get('href')[6:])
            if obj is not None:
                df = df.append(obj, ignore_index = True)
        print(f"Page {i} parsed")
    df.to_csv('games.csv', index=False)
    t = time.time()-start_time
    print(f"Execution time: {t//60} m, {t%60} s")
