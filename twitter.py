import snscrape.modules.twitter as sntwitter
import psycopg2
from datetime import datetime
import time
import random
from snscrape.base import ScraperException

connection = psycopg2.connect(
    user="postgres",
    password="postgres123",
    host="127.0.0.1",
    port="5434",
    database="twitter"
)
  
cur = connection.cursor()

# Keywords to search
keywords = [
    "harga cabai rawit",
    "harga cabai merah",
    "harga tomat",
    "harga beras",
    "harga telur",
    "hari raya"
]

# Date range
start_date = '2025-07-01'
end_date = '2025-12-31'


# Create table if it doesn't exist
create_table_query = '''
CREATE TABLE IF NOT EXISTS twitter_scrapes (
    id SERIAL PRIMARY KEY,
    tanggal TIMESTAMP,
    kata_kunci VARCHAR(255),
    isi_tweet TEXT
);
'''
cur.execute(create_table_query)
connection.commit()

# Function to scrape and insert tweets for a keyword
def scrape_and_insert(keyword, max_retries=3):
    query = f'"{keyword}" since:{start_date} until:{end_date} lang:id'  # Assuming Indonesian language, adjust if needed
    
    for attempt in range(max_retries):
        try:
            tweet_count = 0
            for tweet in sntwitter.TwitterSearchScraper(query).get_items():
                # Add random delay between 1-3 seconds to avoid rate limiting
                time.sleep(random.uniform(1, 3))
                
                tanggal = tweet.date
                isi_tweet = tweet.rawContent
                insert_query = '''
                INSERT INTO twitter_scrapes (tanggal, kata_kunci, isi_tweet)
                VALUES (%s, %s, %s);
                '''
                cur.execute(insert_query, (tanggal, keyword, isi_tweet))
                connection.commit()
                tweet_count += 1
                print(f"Inserted tweet for {keyword} on {tanggal}")
            
            print(f"Successfully scraped {tweet_count} tweets for keyword: {keyword}")
            break  # Exit retry loop on success
            
        except ScraperException as e:
            print(f"Attempt {attempt + 1}/{max_retries} failed for keyword '{keyword}': {str(e)}")
            
            if attempt < max_retries - 1:
                # Exponential backoff: 5s, 15s, 45s
                wait_time = 5 * (3 ** attempt) + random.uniform(0, 5)
                print(f"Retrying in {wait_time:.1f} seconds...")
                time.sleep(wait_time)
            else:
                print(f"Failed to scrape keyword '{keyword}' after {max_retries} attempts. Skipping...")
        
        except Exception as e:
            print(f"Unexpected error for keyword '{keyword}': {str(e)}")
            break

# Scrape for each keyword
for kw in keywords:
    print(f"\n{'='*60}")
    print(f"Scraping for keyword: {kw}")
    print(f"{'='*60}")
    scrape_and_insert(kw)
    
    # Add delay between keywords to avoid aggressive blocking
    print(f"Waiting 10 seconds before next keyword...")
    time.sleep(10)

# Close connection
cur.close()
connection.close()

print("Scraping and insertion completed.")