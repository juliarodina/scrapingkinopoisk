import requests
from bs4 import BeautifulSoup
import os
import pandas as pd
import re
from datetime import datetime, timedelta


def load_list_data(date):
    url = 'https://www.kinopoisk.ru/news/date/%s/' % date
    #print(url)
    request = requests.get(url)

    return request.text


def contain_news_data(text):
    soup = BeautifulSoup(text, features="lxml")
    news_list = soup.find('div', {'class': 'newsList news_list__a__restyle js-rum-hero article__more'})

    return news_list is not None


def read_file(filename):
    with open(filename, encoding='utf-8') as input_file:
        text = input_file.read()

    return text


def parse_list_datafile(filename):
    results = []
    text = read_file(filename)

    soup = BeautifulSoup(text, features="lxml")
    items = soup.find_all('div', {'class': 'article__more_content'})

    for item in items:
        news_link = item.find('a').get('href')
        news_title = item.find('a').text
        news_id = re.findall('news/(.*?)/', news_link)[0]

        results.append({
            'news_id': news_id,
            'news_title': news_title
        })

    return results


curdate = input('начальная дата гггг-мм-дд: ')
curdate = datetime.strptime(curdate, '%Y-%m-%d')
findate = input('конечная дата гггг-мм-дд: ')
findate = datetime.strptime(findate, '%Y-%m-%d')

while curdate <= findate:
    date = datetime.strftime(curdate,'%Y-%m-%d')
    data = load_list_data(date)

    if contain_news_data(data):
        with open('./list_data/%s.html' % date, 'wb') as output_file:
            output_file.write(data.encode('utf-8'))
            curdate += timedelta(days=1)
    elif contain_news_data(data)==0:
        curdate += timedelta(days=1)
    else:
        break


results = []
for filename in os.listdir('./list_data/'):
    results.extend(parse_list_datafile('./list_data/' + filename))

list_data_df = pd.DataFrame(results)
# print(list_data_df)


def load_news_data(news_id):
    url = 'http://www.kinopoisk.ru/news/%s/' % news_id
    # print(url)
    request = requests.get(url)

    return request.text


news = list_data_df.news_id.unique().tolist()

for news_id in news:
    tmp = load_news_data(news_id)
    with open('./news_data/%s.html' % (news_id), 'wb') as output_file:
        output_file.write(tmp.encode('utf-8'))


def parse_news_datafile(filename):
    with open('./news_data/' + filename, encoding='utf-8') as input_file:
        text = input_file.read()

    news_id = filename.replace('.html', '')

    soup = BeautifulSoup(text, features="lxml")

    subtitle = soup.find('span', {'class': 'article__lead error_report_area'}).text.replace("\xa0", " ").replace("\n", "")
    article = [p.text.replace("\xa0", " ").replace("\n", "") for p in soup.find('div', {'class': "article__content newsContent error_report_area"}).find_all('p')]

    new = {
        'news_id': news_id,
        'subtitle': subtitle,
        'text': article
    }

    return new

news = []

for filename in os.listdir('./news_data/'):
    if filename.find('.html') != -1:
        new = parse_news_datafile(filename)
        news.append(new)

news_data_df = pd.DataFrame(news)
fin_df = list_data_df.merge(news_data_df, on='news_id')
#print(fin_df.text.to_string(index=False))
fin_df.to_csv('news.csv', sep='|')

