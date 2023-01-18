import numpy as np
from pandas import DataFrame
import requests
from bs4 import BeautifulSoup as BS
import multiprocessing as mp
import time
from datetime import datetime

def parse_from_blogs(page):
    '''
    arguments:
        - page - two-integers tuple, representing range of pages
    '''
    domain = "https://stopgame.ru"
    arr = np.array([])
    try:
        for i in range(page[0],page[1]+1):
            response = requests.get(f"{domain}/blogs/all/p{i}")
            soup = BS(response.content, "lxml")
            for j in soup.find_all(class_ = "_card__bottom_8sstg_1"):
                arr = np.append(arr, j.find('a').get('href')[25:])
            for j in soup.find_all(class_ = "_card__content_8sstg_390"):
                response = requests.get(f"{domain}{j.find('a').get('href')}#comments")
                soup = BS(response.content, "lxml")
                for k in soup.find_all(class_ = "_comment__author_18g7w_1"):
                    arr = np.append(arr, k.get('href')[25:])
            print(f"Blogs Page {i} parsed")
    except:
        print('something went wrong')
        return None
    else:
        #Just for backup files. Uncomment if you need.
        #DataFrame(list(set(arr)), columns=["username"]).to_csv(f'backup/blogs_users_pages{page[0]}-{page[1]}.csv', index=False)
        return set(arr)

def parse_from_articles(page):
    '''
    arguments:
        - page - two-integers tuple, representing range of pages
    '''
    domain = "https://stopgame.ru"
    arr = np.array([])
    try:
        for i in range(page[0],page[1]+1):
            response = requests.get(f"{domain}/articles/p{i}")
            soup = BS(response.content, "lxml")
            for j in soup.find_all(class_ = "_card__bottom_8sstg_1"):
                arr = np.append(arr, j.find('a').get('href')[25:])
            for j in soup.find_all(class_ = "_card__content_8sstg_390"):
                response = requests.get(f"{domain}{j.find('a').get('href')}#comments")
                soup = BS(response.content, "lxml")
                for k in soup.find_all(class_ = "_comment__author_18g7w_1"):
                    arr = np.append(arr, k.get('href')[25:])
            print(f"Articles Page {i} parsed") 
    except:
        print('something went wrong')
        return None
    else:
        #Just for backup files. Uncomment if you need.
        #DataFrame(list(set(arr)), columns=["username"]).to_csv(f'backup/articles_users_pages{page[0]}-{page[1]}.csv', index=False)
        return set(arr)

def parse_from_news(page):
    '''
    arguments:
        - page - two-integers tuple, representing range of pages
    '''
    domain = "https://stopgame.ru"
    arr = np.array([])
    try:
        for i in range(page[0],page[1]+1):
            response = requests.get(f"{domain}/news/all/p{i}")
            soup = BS(response.content, "lxml")
            for j in soup.find_all(class_ = "item article-summary"):
                response = requests.get(f"{domain}{j.find('a').get('href')}#comments")
                soup = BS(response.content, "lxml")
                for k in soup.find_all(class_ = "_comment__author_18g7w_1"):
                    arr = np.append(arr, k.get('href')[25:])
            print(f"News Page {i} parsed")
    except:
        print('something went wrong')
        return None
    else:
        #Just for backup files. Uncomment if you need.
        #DataFrame(list(set(arr)), columns=["username"]).to_csv(f'backup/news_users_pages{page[0]}-{page[1]}.csv', index=False)
        return set(arr)

def parse_users():
    domain = "https://stopgame.ru"
    cpu = mp.cpu_count()
    pool = mp.Pool(cpu)
    response1 = requests.get(f"{domain}/news/")
    response2 = requests.get(f"{domain}/blogs/all/")
    response3 = requests.get(f"{domain}/articles/")
    soup = BS(response1.content, "lxml")
    news_page = int(soup.find_all(class_ = "prev last")[0].get('href')[11:])
    soup = BS(response2.content, "lxml")
    blogs_page = int(soup.find_all(class_ = "prev last")[0].get('href')[12:])
    soup = BS(response3.content, "lxml")
    articles_page = int(soup.find_all(class_ = "prev last")[0].get('href')[11:])
    news = []
    blogs = []
    articles = []
    r1 = int(news_page/cpu/5)
    r2 = int(blogs_page/cpu/5)
    r3 = int(articles_page/cpu)
    for i in range(1,news_page,r1):
        news.append((i,i+r1-1))
    for i in range(1,blogs_page,r2):
        blogs.append((i,i+r2-1))
    for i in range(1,articles_page,r3):
        articles.append((i,i+r3-1))
    articles_users = pool.map(parse_from_articles, [i for i in articles])
    blogs_users = pool.map(parse_from_blogs, [i for i in blogs])
    news_users = pool.map(parse_from_news, [i for i in news])
    pool.close()
    return [news_users, blogs_users, articles_users]

if __name__ == '__main__':
    start_time = time.time()
    users=parse_users()
    
    result = set()
    for i in users:
        for j in i:
            result = result | j
    dt = datetime.now()
    DataFrame(list({'sdasdas','sasd21'}), columns=['username']).to_csv(f'usernames_{dt.day}_{dt.month}_{dt.year}.csv', index=False)
    t = time.time()-start_time
    print(f"Execution time: {t//60} m, {t%60} s")
