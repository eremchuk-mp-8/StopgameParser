import numpy as np
from pandas import DataFrame
import requests
from bs4 import BeautifulSoup as BS
import multiprocessing as mp
import time
from datetime import datetime

article_card_bottom = "_card__bottom_6bcao_1"
article_card_info_comment = "_card__info__attribute_6bcao_1"
comment_header = "_comment__header_11d56_1"
comment_user_info = "_user-info_pxqm4_1116 _user-info--medium_pxqm4_1"
blog_card_info_comment = "_card__info__attribute_6bcao_1"
blog_card_info_top = "_card__top_6bcao_468"
news_card_comment = "_comments_11mk8_133"
last_page = "prev last"

def parse_from_blogs(page):
    '''
    arguments:
        - page - two-integers tuple, representing range of pages
    '''
    domain = "https://stopgame.ru"
    arr = np.array([])
    try:
        # постраничная обработка
        for i in range(page[0],page[1]+1):
            response = requests.get(f"{domain}/blogs/all/p{i}")
            soup = BS(response.content, "lxml")
            
            # парсинг никнейма автора блога
            for j in soup.find_all(class_ = blog_card_info_top):
                arr = np.append(arr, j.find('a').get('href')[25:])

            # парсинг никнеймов комментаторов блогов
            for j in soup.find_all(class_ = blog_card_info_comment):
                if int(j.contents[1]) > 0:
                  response = requests.get(f"{domain}{j.get('href')}#comments")
                  soup = BS(response.content, "lxml")
                  for k in soup.find_all(class_ = comment_header):
                    if k.find(class_ = comment_user_info) is not None:
                      arr = np.append(arr, k.find(class_ = comment_user_info).get('href')[25:])
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
        # постраничная обработка
        for i in range(page[0],page[1]+1):
            response = requests.get(f"{domain}/articles/p{i}")
            soup = BS(response.content, "lxml")
            
            # парсинг никнейма автора статьи
            for j in soup.find_all(class_ = article_card_bottom):
                arr = np.append(arr, j.find('a').get('href')[25:])
            
            # парсинг никнеймов комментаторов статей
            for j in soup.find_all(class_ = article_card_info_comment):
                if int(j.contents[1]) > 0:
                  response = requests.get(f"{domain}{j.get('href')}#comments")
                  soup = BS(response.content, "lxml")
                  for k in soup.find_all(class_ = comment_header):
                    if k.find(class_ = comment_user_info) is not None:
                      arr = np.append(arr, k.find(class_ = comment_user_info).get('href')[25:])
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
        # постраничная обработка
        for i in range(page[0],page[1]+1):
            response = requests.get(f"{domain}/news/all/p{i}")
            soup = BS(response.content, "lxml")
            
            # парсинг никнейма автора новости
            for j in soup.find_all(class_ = news_card_comment):
                response = requests.get(f"{domain}{j.get('href')}")
                soup = BS(response.content, "lxml")
                
                # парсинг никнеймов комментаторов новости
                for k in soup.find_all(class_ = comment_header):
                  if k.find(class_ = comment_user_info) is not None:
                    arr = np.append(arr, k.find(class_ = comment_user_info).get('href')[25:])
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
    
    # парсинг количества страниц разделов "Новости", "Блоги", "Статьи"
    soup = BS(response1.content, "lxml")
    news_page = int(soup.find_all(class_ = last_page)[0].get('href')[11:])
    soup = BS(response2.content, "lxml")
    blogs_page = int(soup.find_all(class_ = last_page)[0].get('href')[12:])
    soup = BS(response3.content, "lxml")
    articles_page = int(soup.find_all(class_ = last_page)[0].get('href')[11:])
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
            if j is None:
                continue
            result = result.union(j)
    dt = datetime.now()
    DataFrame(list(result), columns=['username']).to_csv(f'usernames_{dt.day}_{dt.month}_{dt.year}.csv', index=False)
    t = time.time()-start_time
    print(f"Execution time: {t//60} m, {t%60} s")

