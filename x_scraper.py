import asyncio
import psycopg2
from datetime import datetime
from twscrape import API, gather

# Konfigurasi Database PostgreSQL
DB_CONFIG = {
    "user": "postgres",
    "password": "postgres123",
    "host": "127.0.0.1",
    "port": "5434",
    "database": "twitter"
}

# Keywords untuk pencarian
keywords = [
    "harga cabai rawit",
    "harga cabai merah",
    "harga tomat",
    "harga beras",
    "harga telur",
    "hari raya"
]

# Tanggal range
start_date = '2025-07-01'
end_date = '2025-12-31'

# Koneksi Database
def get_connection():
    """Membuat koneksi ke database PostgreSQL"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None

# Buat tabel jika belum ada
def create_table():
    """Membuat tabel untuk menyimpan tweet jika belum ada"""
    conn = get_connection()
    if not conn:
        return False
    
    try:
        cur = conn.cursor()
        create_table_query = '''
        CREATE TABLE IF NOT EXISTS twitter_scrapes (
            id SERIAL PRIMARY KEY,
            tanggal TIMESTAMP,
            kata_kunci VARCHAR(255),
            isi_tweet TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        '''
        cur.execute(create_table_query)
        conn.commit()
        print("✓ Tabel berhasil dibuat/sudah ada")
        cur.close()
        return True
    except Exception as e:
        print(f"Error creating table: {e}")
        return False
    finally:
        conn.close()

# Inisialisasi API twscrape
async def init_api():
    """Inisialisasi API twscrape"""
    api = API()
    # Catatan: twscrape memerlukan accounts yang sudah dikonfigurasi
    # Anda perlu menjalankan: twscrape add_account <username> <password> <email> <email_password>
    # atau menggunakan: await api.add_account(username, password, email, email_password)
    return api

# Scrape tweets untuk satu keyword
async def scrape_keyword(api, keyword):
    """Scrape tweets untuk keyword tertentu"""
    conn = get_connection()
    if not conn:
        print(f"Gagal terhubung ke database untuk keyword: {keyword}")
        return 0
    
    try:
        cur = conn.cursor()
        tweet_count = 0
        
        # Query untuk pencarian
        query = f'"{keyword}" since:{start_date} until:{end_date} lang:id'
        
        print(f"\n{'='*70}")
        print(f"Scraping untuk keyword: {keyword}")
        print(f"Query: {query}")
        print(f"{'='*70}")
        
        # Ambil tweets
        tweets = await gather(api.search(query, limit=100))
        
        for tweet in tweets:
            try:
                tanggal = tweet.date if hasattr(tweet, 'date') else datetime.now()
                isi_tweet = tweet.text if hasattr(tweet, 'text') else tweet.rawContent
                
                insert_query = '''
                INSERT INTO twitter_scrapes (tanggal, kata_kunci, isi_tweet)
                VALUES (%s, %s, %s);
                '''
                cur.execute(insert_query, (tanggal, keyword, isi_tweet))
                conn.commit()
                tweet_count += 1
                print(f"✓ Tweet tersimpan - {tanggal} - {keyword}")
                
            except Exception as e:
                print(f"Error menyimpan tweet: {e}")
                continue
        
        print(f"✓ Total {tweet_count} tweets untuk '{keyword}' berhasil disimpan")
        cur.close()
        return tweet_count
        
    except Exception as e:
        print(f"✗ Error scraping keyword '{keyword}': {e}")
        return 0
    finally:
        conn.close()

# Main function
async def main():
    """Fungsi utama untuk menjalankan scraping"""
    print("Memulai scraping X (Twitter)...")
    print(f"Periode: {start_date} s/d {end_date}")
    
    # Buat tabel
    if not create_table():
        print("Gagal membuat tabel, keluar...")
        return
    
    # Inisialisasi API
    try:
        api = await init_api()
        print("✓ API twscrape berhasil diinisialisasi")
    except Exception as e:
        print(f"✗ Error inisialisasi API: {e}")
        print("\nPastikan sudah menambahkan account:")
        print("  twscrape add_account <username> <password> <email> <email_password>")
        return
    
    # Scrape untuk setiap keyword
    total_tweets = 0
    for keyword in keywords:
        try:
            count = await scrape_keyword(api, keyword)
            total_tweets += count
            # Delay antara keyword untuk menghindari rate limiting
            await asyncio.sleep(5)
        except Exception as e:
            print(f"Error processing keyword '{keyword}': {e}")
            continue
    
    print(f"\n{'='*70}")
    print(f"Scraping selesai!")
    print(f"Total tweets yang disimpan: {total_tweets}")
    print(f"{'='*70}")

# Jalankan program
if __name__ == "__main__":
    print("""
    ╔════════════════════════════════════════════════════════════════════╗
    ║           X (Twitter) Scraper dengan twscrape                       ║
    ║  Data akan disimpan ke PostgreSQL dengan format:                   ║
    ║  - Tanggal tweet                                                    ║
    ║  - Kata kunci pencarian                                            ║
    ║  - Isi tweet                                                        ║
    ╚════════════════════════════════════════════════════════════════════╝
    """)
    
    # Jalankan async main
    asyncio.run(main())
