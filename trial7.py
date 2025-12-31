from bs4 import BeautifulSoup
import requests
import pandas as pd
from datetime import datetime
import time

keywords = [
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

# URL yang sudah diperbaiki
sources = {
    "detik": "https://www.detik.com/search/searchall?query={keyword}&siteid=2",
    "kumparan": "https://kumparan.com/search/{keyword}",
    "antaranews": "https://www.antaranews.com/search?q={keyword}",
    "tribunnews": "https://www.tribunnews.com/search?q={keyword}",
    "vivanews": "https://www.viva.co.id/search?q={keyword}",
    "liputan6": "https://www.liputan6.com/search?q={keyword}"
}

def scrape_news(source, url, keyword):
    articles = []
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'id-ID,id;q=0.9,en-US;q=0.8,en;q=0.7',
        'Referer': 'https://www.google.com/'
    }
    
    try:
        print(f"Fetching: {url}")
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Debug: simpan HTML untuk inspeksi
        # with open(f'debug_{source}.html', 'w', encoding='utf-8') as f:
        #     f.write(soup.prettify())
        
    except requests.RequestException as e:
        print(f"Error fetching {url} untuk keyword '{keyword}': {e}")
        return articles
    
    if source == "detik":
        search_results = soup.find_all('article') or \
                        soup.find_all('div', class_='list-content__item')
        
        for item in search_results[:10]:
            try:
                title_tag = item.find('h3') or item.find('h2') or item.find('a', class_='media__link')
                link_tag = item.find('a')
                
                if title_tag and link_tag:
                    title = title_tag.get_text(strip=True)
                    url_link = link_tag.get('href', 'N/A')
                    
                    # Cari tanggal jika ada
                    date_tag = item.find('span', class_='date') or item.find('div', class_='date')
                    date = date_tag.get_text(strip=True) if date_tag else datetime.now().strftime('%Y-%m-%d')
                    
                    articles.append({
                        'Judul Berita': title,
                        'Keyword': keyword,
                        'Sumber Berita': 'Detik.com',
                        'Tanggal Terbit': date,
                        'URL': url_link
                    })
            except Exception as e:
                print(f"Error parsing Detik item: {e}")
                continue
    
    elif source == "kumparan":
        search_results = soup.find_all('article') or \
                        soup.find_all('div', style_= 'width: 100%' ) or \
                         soup.find_all('div', class_='sc-6i7ydt-0 BLJqE') or \
                         soup.find_all('div', class_='Viewweb__StyledView-sc-b0snvl-0 cvAqFi  flex')
        
        for item in search_results[:10]:
            try:
                # Cari link dan title
                link_tag = item.find('a')
                title_tag = item.find('h2') or item.find('h3') or item.find('span', class_='Story__title')
                
                if link_tag:
                    title = title_tag.get_text(strip=True) if title_tag else link_tag.get_text(strip=True)
                    url_link = link_tag.get('href', 'N/A')
                    
                    # Pastikan URL lengkap
                    #if url_link != 'N/A' and not url_link.startswith('http'):
                       # url_link = 'https://kumparan.com' + url_link
                    
                    # Cari tanggal
                    date_tag = item.find('time') or item.find('span', class_='Textweb__StyledText-sc-1ed9ao-0 bwfOdo  c275a64364 txacenter txfdefault txwregular txmicro')
                    date = date_tag.get_text(strip=True) if date_tag else datetime.now().strftime('%Y-%m-%d')
                    
                    articles.append({
                        'Judul Berita': title,
                        'Keyword': keyword,
                        'Sumber Berita': 'Kumparan.com',
                        'Tanggal Terbit': date,
                        'URL': url_link
                    })
            except Exception as e:
                print(f"Error parsing Kumparan item: {e}")
                continue
    
    elif source == "antaranews":
        search_results = soup.find_all('article') or \
                        soup.find_all('div', class_='card__post card__post-list card__post__transition mt-30')
        
        for item in search_results[:10]:
            try:
                title_tag = item.find('h3') or item.find('h2') or item.find('a', class_='jeg_post_title')
                link_tag = item.find('a')
                
                if title_tag and link_tag:
                    title = title_tag.get_text(strip=True)
                    url_link = link_tag.get('href', 'N/A')
                    
                    # Cari tanggal
                    date_tag = item.find('u;', class_='list-inline') or \
                               item.find('time') or \
                               item.find('span', class_='post-date')
                    date = date_tag.get_text(strip=True) if date_tag else datetime.now().strftime('%Y-%m-%d')
                    
                    articles.append({
                        'Judul Berita': title,
                        'Keyword': keyword,
                        'Sumber Berita': 'antaranews.com',
                        'Tanggal Terbit': date,
                        'URL': url_link
                    })
            except Exception as e:
                print(f"Error parsing JawaPos item: {e}")
                continue
    
    elif source == "tribunnews":

        search_results = soup.find_all('article') or \
                         soup.find_all('div', class_='gsc-webResult gsc-result') or \
                         soup.find_all('div', class_='gs-webResult gs-result')
        
        for item in search_results[:10]:
            try:
                # Cari title dan link
                title_tag = item.find('h3') or item.find('h2') or item.find('h4')
                link_tag = item.find('a')
                
                if link_tag:
                    title = title_tag.get_text(strip=True) if title_tag else link_tag.get_text(strip=True)
                    url_link = link_tag.get('href', 'N/A')
                    
                    # Pastikan URL lengkap
                    #if url_link != 'N/A' and not url_link.startswith('http'):
                      #  url_link = 'https://www.tribunnews.com' + url_link
                    
                    # Cari tanggal
                    date_tag = item.find('time') or \
                               item.find('span', class_='date') or \
                               item.find('div', class_='jarak-waktu')
                    date = date_tag.get_text(strip=True) if date_tag else datetime.now().strftime('%Y-%m-%d')
                    
                    articles.append({
                        'Judul Berita': title,
                        'Keyword': keyword,
                        'Sumber Berita': 'TribunNews.com',
                        'Tanggal Terbit': date,
                        'URL': url_link
                    })
            except Exception as e:
                print(f"Error parsing TribunNews item: {e}")
                continue

    elif source == "vivanews":
        search_results = soup.find_all('article') or \
                         soup.find_all('div', class_='article-list-row')
        
        for item in search_results[:10]:
            try:
                # Cari title dan link
                title_tag = item.find('h3') or item.find('h2') or item.find('h4')
                link_tag = item.find('a')
                
                if link_tag:
                    title = title_tag.get_text(strip=True) if title_tag else link_tag.get_text(strip=True)
                    url_link = link_tag.get('href', 'N/A')
                    
                    # Pastikan URL lengkap
                    #if url_link != 'N/A' and not url_link.startswith('http'):
                      #  url_link = 'https://www.tribunnews.com' + url_link
                    
                    # Cari tanggal
                    date_tag = item.find('time') or \
                               item.find('div', class_='article-list-date content_center')
                    date = date_tag.get_text(strip=True) if date_tag else datetime.now().strftime('%Y-%m-%d')
                    
                    articles.append({
                        'Judul Berita': title,
                        'Keyword': keyword,
                        'Sumber Berita': 'vivanews.com',
                        'Tanggal Terbit': date,
                        'URL': url_link
                    })
            except Exception as e:
                print(f"Error parsing vivanews item: {e}")
                continue
    
    elif source == "liputan6":
        search_results = soup.find_all('article') or \
                         soup.find_all('article', class_='articles--iridescent-list--item articles--iridescent-list--text-item') or \
                         soup.find_all('aside', class_='articles--iridescent-list--text-item__details') or \
                         soup.find_all('hedaer', class_='articles--iridescent-list--text-item__header')
        
        for item in search_results[:10]:
            try:
                # Cari title dan link
                title_tag = item.find('h3') or item.find('h2') or item.find('h4')
                link_tag = item.find('a')
                
                if link_tag:
                    title = title_tag.get_text(strip=True) if title_tag else link_tag.get_text(strip=True)
                    url_link = link_tag.get('href', 'N/A')
                    
                    # Pastikan URL lengkap
                    #if url_link != 'N/A' and not url_link.startswith('http'):
                      #  url_link = 'https://www.tribunnews.com' + url_link
                    
                    # Cari tanggal
                    date_tag = item.find('time') or \
                               item.find('div', class_='article-list-date content_center')
                    date = date_tag.get_text(strip=True) if date_tag else datetime.now().strftime('%Y-%m-%d')
                    
                    articles.append({
                        'Judul Berita': title,
                        'Keyword': keyword,
                        'Sumber Berita': 'liputan6.com',
                        'Tanggal Terbit': date,
                        'URL': url_link
                    })
            except Exception as e:
                print(f"Error parsing liputan6 item: {e}")
                continue
    
    return articles

if __name__ == "__main__":
    data = []
    
    for source, url_template in sources.items():
        print(f"\n{'='*50}")
        print(f"Scraping {source.upper()}")
        print(f"{'='*50}")
        
        for keyword in keywords:
            # Format keyword untuk URL
            search_query = keyword.replace(' ', '+')
            url = url_template.format(keyword=search_query)
            
            articles = scrape_news(source, url, keyword)
            data.extend(articles)
            
            print(f"✓ Scraped {len(articles)} articles for keyword '{keyword}' from {source}")
            
            # Delay untuk menghindari rate limiting
            time.sleep(2)
    
    # Simpan hasil
    if data:
        df = pd.DataFrame(data)
        df.to_excel('beritasemua.xlsx', index=False)
        df.to_csv('beritasemua.csv', index=False, encoding='utf-8-sig')
        
        print(f"\n{'='*50}")
        print(f"SCRAPING SELESAI!")
        print(f"{'='*50}")
        print(f"Total artikel: {len(data)}")
        print(f"File disimpan: hasilberita.xlsx & hasilberita.csv")
        