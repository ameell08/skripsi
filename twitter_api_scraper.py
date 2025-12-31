"""
Twitter API v2 Scraper - Official API
Scrape data X dengan Twitter API v2 yang resmi dan stabil
Data disimpan ke PostgreSQL
"""

import tweepy
import psycopg2
from datetime import datetime
import time

# ==================== KONFIGURASI ====================

# Twitter API v2 Credentials
# Dapatkan dari: https://developer.twitter.com/en/portal/dashboard
BEARER_TOKEN = "YOUR_BEARER_TOKEN_HERE"  # Ganti dengan token Anda

# Database PostgreSQL
DB_CONFIG = {
    "user": "postgres",
    "password": "postgres123",
    "host": "127.0.0.1",
    "port": "5434",
    "database": "twitter"
}

# Keywords
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

# ==================== DATABASE FUNCTIONS ====================

def get_connection():
    """Membuat koneksi ke database PostgreSQL"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        print(f"❌ Error connecting to database: {e}")
        return None

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
            tweet_id VARCHAR(50),
            author VARCHAR(255),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        '''
        cur.execute(create_table_query)
        conn.commit()
        print("✓ Tabel berhasil dibuat/sudah ada")
        cur.close()
        return True
    except Exception as e:
        print(f"❌ Error creating table: {e}")
        return False
    finally:
        conn.close()

def insert_tweet(tanggal, kata_kunci, isi_tweet, tweet_id, author):
    """Menyimpan tweet ke database"""
    conn = get_connection()
    if not conn:
        return False
    
    try:
        cur = conn.cursor()
        insert_query = '''
        INSERT INTO twitter_scrapes (tanggal, kata_kunci, isi_tweet, tweet_id, author)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING;
        '''
        cur.execute(insert_query, (tanggal, kata_kunci, isi_tweet, tweet_id, author))
        conn.commit()
        cur.close()
        return True
    except Exception as e:
        print(f"❌ Error inserting tweet: {e}")
        return False
    finally:
        conn.close()

# ==================== TWITTER API FUNCTIONS ====================

def init_twitter_api():
    """Inisialisasi Twitter API v2 client"""
    if BEARER_TOKEN == "YOUR_BEARER_TOKEN_HERE":
        print("""
        ❌ BEARER TOKEN TIDAK DIKONFIGURASI!
        
        Langkah untuk mendapatkan Bearer Token:
        1. Buka https://developer.twitter.com/en/portal/dashboard
        2. Pilih project dan app Anda
        3. Ke tab "Keys and tokens"
        4. Copy Bearer Token
        5. Paste ke BEARER_TOKEN di script ini
        
        Atau set environment variable:
        set TWITTER_BEARER_TOKEN=your_token_here
        """)
        return None
    
    try:
        client = tweepy.Client(bearer_token=BEARER_TOKEN, wait_on_rate_limit=True)
        print("✓ Twitter API v2 client berhasil diinisialisasi")
        return client
    except Exception as e:
        print(f"❌ Error inisialisasi API: {e}")
        return None

def search_tweets(client, keyword):
    """Scrape tweets untuk satu keyword"""
    try:
        # Query Twitter API v2 format
        query = f'"{keyword}" lang:id -is:retweet'
        start_time = f"{start_date}T00:00:00Z"
        end_time = f"{end_date}T23:59:59Z"
        
        print(f"\n{'='*70}")
        print(f"🔍 Scraping untuk keyword: {keyword}")
        print(f"Query: {query}")
        print(f"Periode: {start_date} s/d {end_date}")
        print(f"{'='*70}")
        
        # Pagination untuk mengambil banyak tweets
        tweet_count = 0
        
        # Gunakan search_recent_tweets (7 hari terakhir) atau search_all_tweets (full archive)
        # search_all_tweets memerlukan Academic Research access
        for response in tweepy.Paginator(
            client.search_recent_tweets,
            query=query,
            start_time=start_time,
            end_time=end_time,
            max_results=100,
            tweet_fields=['created_at', 'author_id', 'public_metrics'],
            expansions=['author_id'],
            user_fields=['username'],
            limit=10  # Ambil maksimal 10 halaman (1000 tweets)
        ):
            if response.data is None:
                break
                
            # Build user map untuk author lookup
            user_map = {}
            if response.includes and response.includes.get('users'):
                for user in response.includes['users']:
                    user_map[user.id] = user.username
            
            for tweet in response.data:
                try:
                    tanggal = tweet.created_at
                    isi_tweet = tweet.text
                    tweet_id = tweet.id
                    author = user_map.get(tweet.author_id, "Unknown")
                    
                    # Simpan ke database
                    if insert_tweet(tanggal, keyword, isi_tweet, tweet_id, author):
                        tweet_count += 1
                        print(f"✓ Tweet tersimpan - {author} - {tanggal}")
                    
                except Exception as e:
                    print(f"⚠ Error processing tweet: {e}")
                    continue
            
            # Rate limiting
            time.sleep(1)
        
        print(f"✓ Total {tweet_count} tweets untuk '{keyword}' berhasil disimpan")
        return tweet_count
        
    except tweepy.TweepyException as e:
        print(f"❌ Twitter API Error untuk '{keyword}': {e}")
        return 0
    except Exception as e:
        print(f"❌ Unexpected error untuk '{keyword}': {e}")
        return 0

# ==================== MAIN FUNCTION ====================

def main():
    """Fungsi utama"""
    print("""
    ╔════════════════════════════════════════════════════════════════════╗
    ║          X (Twitter) Scraper - Official API v2                     ║
    ║  Data akan disimpan ke PostgreSQL dengan kolom:                    ║
    ║  - Tanggal tweet                                                    ║
    ║  - Kata kunci pencarian                                            ║
    ║  - Isi tweet                                                        ║
    ║  - Tweet ID                                                         ║
    ║  - Author/Username                                                 ║
    ╚════════════════════════════════════════════════════════════════════╝
    """)
    
    # Buat tabel
    if not create_table():
        print("Gagal membuat tabel, keluar...")
        return
    
    # Inisialisasi API
    client = init_twitter_api()
    if not client:
        return
    
    # Scrape untuk setiap keyword
    total_tweets = 0
    for keyword in keywords:
        try:
            count = search_tweets(client, keyword)
            total_tweets += count
            # Delay antara keyword untuk menghindari rate limiting
            time.sleep(2)
        except Exception as e:
            print(f"❌ Error processing keyword '{keyword}': {e}")
            continue
    
    print(f"\n{'='*70}")
    print(f"✓ Scraping selesai!")
    print(f"✓ Total tweets yang disimpan: {total_tweets}")
    print(f"{'='*70}")

# ==================== ENTRY POINT ====================

if __name__ == "__main__":
    main()
