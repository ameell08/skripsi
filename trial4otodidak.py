from bs4 import BeautifulSoup
import requests
import pandas as pd
from datetime import datetime

keywords =[
    "Harga daging",
    "Harga cabai",
    "Harga beras",
    "Harga bawang merah",
    "Harga minyak goreng",
    "Harga telur ayam",
    "Harga kedelai",
    "demonstrasi",
    "panen raya",
    "hari raya kurban",
    "hari raya idul fitri",
    "Harga gula pasir",
    "Harga ayam broiler"
]

sources = {
    "kompas" :"https://search/kompas.com/search?q={keyword}",
    "detik" :"https://www.detik.com/search/searchall?query={keyword}",
    "cnn"   :"https://www.cnnindonesia.com/search/?query={keyword}",
}

def scrape_news(source, url, keyword):
    articles = []
    
    try :
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup (response.text, 'html.parser')
    except requests.RequestException as e:
        print(f"Error fetching {url} untuk keyword '{keyword}': {e}")
        return articles
    
    if source == "kompas":
        for item in soup.find_all('div', class_='articlelist -list'):
            link_tag = item.find ('a', class_='article-link')

            title = link_tag.find('h1', class_='read__title').text.strip() if link_tag else 'N/A'
            url = link_tag['href'] if link_tag and 'href' in link_tag.attrs else 'N/A'
                        
            date_tag = item.find('div', class_='read__time')
            date = date_tag.text.strip() if date_tag else datetime.now().strftime('%Y-%m-%d')
            
            summary_tag = item.find('div', class_='read__content') or item.find('p')
            summary = summary_tag.text.strip() if summary_tag else 'N/A'
            
            articles.append({
                'Judul Berita': title,
                'Keyword': keyword,
                'Sumber Berita': 'Kompas.com',
                'Tanggal Terbit': date,
                'URL': url,
                'Ringkasan': summary
            })
    
    elif source == "detik":
        for item in soup.find_all('div', class_='list-content'):
            link_tag = item.find('h3', class_='media__title')

            title = link_tag.find('h1', class_='detail__title').text.strip() if link_tag else 'N/A'
            url = link_tag['href'] if link_tag and 'href' in link_tag.attrs else 'N/A'
            
            date_tag = item.find('div', class_='detail__date')
            date = date_tag.text.strip() if date_tag else datetime.now().strftime('%Y-%m-%d')
            
            summary_tag = item.find('div', class_='detail__body-text itp_bodycontent') or item.find('p')    
            summary = summary_tag.text.strip() if summary_tag else 'N/A'
            
            articles.append({
                'Judul Berita': title,
                'Keyword': keyword,
                'Sumber Berita': 'Detik.com',
                'Tanggal Terbit': date,
                'URL': url,
                'Ringkasan': summary
            })
    
    elif source == "cnn":
        for item in soup.find_all('div', class_='flex flex-col gap-5 nhl-list'):
            link_tag = item.find('a', class_='flex group items-center gap-4')

            title = link_tag.find('h1', class_='mb-2 text-[28px] leading-9 text-cnn_black').text.strip() if link_tag else 'N/A'
            url = link_tag['href'] if link_tag and 'href' in link_tag.attrs else 'N/A'

            date_tag = item.find('div', class_='text-cnn_grey text-sm mb-2.5')
            date = date_tag.text.strip() if date_tag else datetime.now().strftime('%Y-%m-%d')

            summary_tag = item.find('div', class_='detail-text text-cnn_black text-sm grow min-w-0') or item.find('p')
            summary = summary_tag.text.strip() if summary_tag else 'N/A'

            articles.append({
                'Judul Berita': title,
                'Keyword': keyword,
                'Sumber Berita': 'cnnindonesia.com',
                'Tanggal Terbit': date,
                'URL': url,
                'Ringkasan': summary
            })

            return articles
        
if __name__ == "__main__":
    #list menyik=mpan data
    data=[]
    #loop untuk scrape dari semua sumber dan keyword
    for source, url in sources.items():
        for keyword in keywords:
            search_query = keyword.replace(' ', '+')
            url = url.format(keyword=search_query)
            articles = scrape_news(source, url, keyword)
            data.extend(articles)
            print(f"Scraped {len(articles)} articles for keyword '{keyword}' from {source}")

if data:
    df= pd.DataFrame(data)
    df.to_excel('news_results.xlsx', index=False)
    
    print ("data scraping selesai dan disimpan di news_results.xlsx")