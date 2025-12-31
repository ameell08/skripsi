from bs4 import BeautifulSoup
import requests
from datetime import datetime, timedelta
import psycopg2
import time

# Koneksi PostgreSQL
connection = psycopg2.connect(
    user="postgres",
    password="postgres123",
    host="127.0.0.1",
    port="5434",
    database="berita"
)

cursor = connection.cursor()

# ===== KONFIGURASI RANGE TANGGAL =====
start_date = datetime(2025, 7, 1).date()
end_date = datetime(2025, 7, 4).date()
print(f"\n{'='*60}")
print(f"SCRAPER BERITA PERTANIAN")
print(f"Range: {start_date} sampai {end_date}")
print(f"{'='*60}\n")

# Create tables jika belum ada
try:
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS detikcom (
        id SERIAL PRIMARY KEY,
        judul VARCHAR(500) NOT NULL,
        keyword VARCHAR(100),
        tanggal_terbit DATE,
        url TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)
    connection.commit()
    print("✓ Table 'detikcom' ready")
except Exception as e:
    print(f"✗ Error creating detikcom table: {e}")
    connection.rollback()

try:
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS vivanews (
        id SERIAL PRIMARY KEY,
        judul VARCHAR(500) NOT NULL,
        keyword VARCHAR(100),
        tanggal_terbit DATE,
        url TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)
    connection.commit()
    print("✓ Table 'vivanews' ready\n")
except Exception as e:
    print(f"✗ Error creating vivanews table: {e}\n")
    connection.rollback()

keywords = [
    "Harga Cabai Merah",
    "Harga cabai rawit",
    "Harga beras",
    "Harga tomat",
    "Harga telur ayam",
    "Harga naik ",
    "Harga turun",
    "demonstrasi",
    "panen raya",
    "hari raya kurban",
    "hari raya idul fitri"
]

sources = {
    "detik": "https://www.detik.com/search/searchall?query={keyword}&siteid=2",
    # "antaranews": "https://www.antaranews.com/search?q={keyword}",
    "vivanews": "https://www.viva.co.id/search?q={keyword}",
}

def scrape_news(source, url, keyword):
    articles = []
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36', #biar ga dinggap bot
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
    
    # elif source == "antaranews":
    #     search_results = soup.find_all('article') or \
    #                     soup.find_all('div', class_='card__post card__post-list card__post__transition mt-30')
        
    #     for item in search_results[:10]:
    #         try:
    #             title_tag = item.find('h3') or item.find('h2') or item.find('a', class_='jeg_post_title')
    #             link_tag = item.find('a')
                
    #             if title_tag and link_tag:
    #                 title = title_tag.get_text(strip=True)
    #                 url_link = link_tag.get('href', 'N/A')
                    
    #                 # Cari tanggal
    #                 date_tag = item.find('u;', class_='list-inline') or \
    #                            item.find('time') or \
    #                            item.find('span', class_='post-date')
    #                 date = date_tag.get_text(strip=True) if date_tag else datetime.now().strftime('%Y-%m-%d')
                    
    #                 articles.append({
    #                     'Judul Berita': title,
    #                     'Keyword': keyword,
    #                     'Sumber Berita': 'antaranews.com',
    #                     'Tanggal Terbit': date,
    #                     'URL': url_link
    #                 })
    #         except Exception as e:
    #             print(f"Error parsing JawaPos item: {e}")
    #             continue
    
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
    
    return articles

if __name__ == "__main__":
    data = []
    db_count = 0
    db_error = 0
    
    print(f"\n{'='*60}")
    print("SCRAPER BERITA PERTANIAN")
    print(f"{'='*60}\n")
    
    for source, url_template in sources.items():
        print(f"{'='*50}")
        print(f"Scraping {source.upper()}")
        print(f"{'='*50}")
        
        for keyword in keywords:
            # Format keyword untuk URL
            search_query = keyword.replace(' ', '+')
            url = url_template.format(keyword=search_query)
            
            articles = scrape_news(source, url, keyword)
            data.extend(articles)
            
            # Simpan ke PostgreSQL
            for article in articles:
                try:
                    # Parse tanggal
                    try:
                        tanggal_terbit = datetime.strptime(article['Tanggal Terbit'], '%Y-%m-%d').date()
                    except Exception as parse_err:
                        tanggal_terbit = datetime.now().date()
                    
                    # Tentukan tabel berdasarkan source
                    if source == "detik":
                        table_name = "detik_com"
                    elif source == "vivanews":
                        table_name = "vivanews_co_id"
                    else:
                        table_name = "detik_com"  # default
                    
                    # Debug print
                    print(f"  Inserting: {article['Judul Berita'][:50]}... to {table_name}")
                    
                    cursor.execute(f"""
                        INSERT INTO {table_name} (judul, keyword, tanggal_terbit, url)
                        VALUES (%s, %s, %s, %s)
                    """, (
                        article['Judul Berita'],
                        article['Keyword'],
                        tanggal_terbit,
                        article['URL']
                    ))
                    connection.commit()
                    db_count += 1
                except Exception as e:
                    db_error += 1
                    print(f"  ✗ Insert error: {str(e)[:100]}")
                    connection.rollback()
            
            print(f"✓ {len(articles)} articles for '{keyword}' from {source}")
            
            # Delay untuk menghindari rate limiting
            time.sleep(2)
    
    cursor.close()
    connection.close()
    
    print(f"\n{'='*60}")
    print("✓ SCRAPING SELESAI!")
    print(f"{'='*60}")
    print(f"Disimpan ke PostgreSQL: {db_count} items")
    if db_error > 0:
        print(f"Database errors: {db_error}")
    print(f"File juga disimpan: beritasemua.xlsx & beritasemua.csv")
    print(f"{'='*60}\n")
        