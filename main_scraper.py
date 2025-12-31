from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import psycopg2
import requests
import time

# Koneksi ke PostgreSQL
connection = psycopg2.connect(
    user="postgres",
    password="postgres123",
    host="127.0.0.1",
    port="5434",
    database="smartfarming"
)

cursor = connection.cursor()

# Create tables if they don't exist
try:
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS "komoditas_ratarata" (
        id SERIAL PRIMARY KEY,
        tanggal DATE NOT NULL,
        kategori_id INTEGER,
        komoditas_nama VARCHAR(255),
        satuan VARCHAR(50),
        harga INTEGER,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)
    connection.commit()
    print("✓ Table 'komoditas_ratarata' ready")
except Exception as e:
    print(f"✗ Error creating komoditas_ratarata table: {e}")
    connection.rollback()

try:
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS komoditas (
        id SERIAL PRIMARY KEY,
        tanggal DATE NOT NULL,
        pasar_id INTEGER,
        kategori_id INTEGER,
        komoditas_nama VARCHAR(255),
        satuan VARCHAR(50),
        harga INTEGER,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)
    connection.commit()
    print("✓ Table 'komoditas' ready")
except Exception as e:
    print(f"✗ Error creating komoditas table: {e}")
    connection.rollback()

try:
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS kategori_komoditas (
        id SERIAL PRIMARY KEY,
        kategori VARCHAR(255) NOT NULL UNIQUE
    );
    """)
    connection.commit()
    print("✓ Table 'kategori_komoditas' ready")
except Exception as e:
    print(f"✗ Error creating kategori_komoditas table: {e}")
    connection.rollback()

# Range tanggal: 2025-07-01 sampai 2025-12-31
start_date = datetime(2025, 7, 1).date()
end_date = datetime(2025, 12, 31).date()
date_list = [(start_date + timedelta(days=i)).strftime("%Y-%m-%d") 
             for i in range((end_date - start_date).days + 1)]

print(f"\n{'='*60}")
print(f"SCRAPER KOMODITAS PERTANIAN")
print(f"{'='*60}")
print(f"Range: {start_date} sampai {end_date}")
print(f"Total hari yang akan diproses: {len(date_list)}")
print(f"{'='*60}\n")

def array_data_komoditas_mean(tanggal):
    """Ambil data rata-rata harga komoditas"""
    url = "https://siskaperbapo.jatimprov.go.id/harga/tabel.nodesign/"
    post_data = {"tanggal": tanggal}
    
    try:
        page = requests.post(url, data=post_data, timeout=10)
        soup = BeautifulSoup(page.content, 'html.parser')
        rows = soup.find_all('tr')
        
        kategori = ""
        data_komoditas_mean = []
        
        for row in rows:
            arr_komoditas = []
            for td in row:
                str_val = td.string or ""
                arr_komoditas.append(str_val.strip())
            
            if len(arr_komoditas) < 13:
                continue
            
            nomor = arr_komoditas[1]
            nama = arr_komoditas[3].replace('- ', '')
            satuan = arr_komoditas[5]
            hrg_current = arr_komoditas[9].replace('.', '')
            
            if nomor:
                # Kategori header
                kategori = nama
            else:
                # Produk sub-item
                if satuan and kategori:
                    data_komoditas_mean.append([tanggal, kategori, nama, satuan, hrg_current])
        
        return data_komoditas_mean
    except Exception as e:
        print(f"  ✗ Error fetching mean data: {str(e)[:50]}")
        return []

def get_data_pasar(my_kabkota, retries=3, timeout=10):
    """Ambil data pasar dari API"""
    url = f"https://siskaperbapo.jatimprov.go.id/harga/pasar.json/{my_kabkota}"
    
    for attempt in range(retries):
        try:
            res = requests.get(url, timeout=timeout)
            res.raise_for_status()
            return res.json()
        except requests.exceptions.RequestException as e:
            if attempt < retries - 1:
                time.sleep(2)
            else:
                print(f"  ✗ Failed to fetch pasar data for {my_kabkota}")
                return []

def array_data_komoditas(tanggal, id_kabkota, nm_kabkota, keycode_kabkota, id_pasar, nm_pasar, retries=2, timeout=10):
    """Ambil data komoditas per pasar"""
    url = "https://siskaperbapo.jatimprov.go.id/harga/tabel.nodesign/"
    
    for attempt in range(retries):
        try:
            post_data = {"tanggal": tanggal, "kabkota": keycode_kabkota, "pasar": id_pasar}
            page = requests.post(url, data=post_data, timeout=timeout)
            page.raise_for_status()
            break
        except requests.exceptions.RequestException as e:
            if attempt < retries - 1:
                time.sleep(2)
            else:
                return []
    
    try:
        soup = BeautifulSoup(page.content, 'html.parser')
        rows = soup.find_all('tr')
        
        kategori = ""
        data_komoditas = []
        
        for row in rows:
            arr_komoditas = []
            for td in row:
                str_val = td.string or ""
                arr_komoditas.append(str_val.strip())
            
            if len(arr_komoditas) < 12:
                continue
            
            nomor = arr_komoditas[1]
            nama = arr_komoditas[3].replace('- ', '')
            satuan = arr_komoditas[5]
            hrg_last = arr_komoditas[7].replace('.', '')
            hrg_current = arr_komoditas[9].replace('.', '')
            
            if nomor:
                # Kategori header
                kategori = nama
            else:
                # Produk sub-item
                if satuan:
                    data_komoditas.append([tanggal, id_kabkota, nm_kabkota, id_pasar, nm_pasar, kategori, nama, satuan, hrg_last, hrg_current])
        
        return data_komoditas
    except Exception as e:
        print(f"  ✗ Error parsing komoditas data: {str(e)[:50]}")
        return []

# Daftar kabupaten/kota
list_kabkota = ["bangkalankab", "banyuwangikab", "blitarkab", "bojonegorokab", "bondowosokab",
    "gresikkab", "jemberkab", "jombangkab", "kedirikab", "lamongankab", "lumajangkab", "madiunkab",
    "magetankab", "malangkab", "mojokertokab", "nganjukkab", "ngawikab", "pacitankab", "pamekasankab",
    "pasuruankab", "ponorogokab", "probolinggokab", "sampangkab", "sidoarjokab", "situbondokab", "sumenepkab",
    "trenggalekkab", "tubankab", "tulungagungkab", "batukota", "blitarkota", "kedirikota", "madiunkota",
    "malangkota", "mojokertokota", "pasuruankota", "probolinggokota", "surabayakota"]

# Query INSERT
query_mean = """
INSERT INTO "komoditas_ratarata" (tanggal, kategori_id, komoditas_nama, satuan, harga)
VALUES (%s, %s, %s, %s, %s)
"""

query_komoditas = """
INSERT INTO komoditas (tanggal, pasar_id, kategori_id, komoditas_nama, satuan, harga)
VALUES (%s, %s, %s, %s, %s, %s)
"""

# Main scraping loop
for idx, tanggal in enumerate(date_list, 1):
    print(f"[{idx}/{len(date_list)}] Processing {tanggal}")
    
    # ===== RATA-RATA DATA =====
    data_komoditas_mean = array_data_komoditas_mean(tanggal)
    mean_count = 0
    mean_error = 0
    
    for item in data_komoditas_mean:
        try:
            # Cari atau buat kategori
            cursor.execute("SELECT id FROM kategori_komoditas WHERE kategori ILIKE %s", (item[1],))
            result = cursor.fetchone()
            
            if result:
                kategori_id = result[0]
            else:
                cursor.execute("INSERT INTO kategori_komoditas (kategori) VALUES (%s) RETURNING id", (item[1],))
                kategori_id = cursor.fetchone()[0]
                connection.commit()
            
            # Insert data rata-rata
            val = (item[0], kategori_id, item[2], item[3], item[4])
            cursor.execute(query_mean, val)
            connection.commit()
            mean_count += 1
        except Exception as e:
            mean_error += 1
            connection.rollback()
    
    if mean_count > 0:
        print(f"  ✓ Mean: {mean_count} items")
    if mean_error > 0:
        print(f"  ✗ Mean errors: {mean_error}")
    
    # ===== PASAR DATA =====
    pasar_total = 0
    pasar_error = 0
    
    for my_kabkota in list_kabkota:
        data_pasar = get_data_pasar(my_kabkota, retries=3, timeout=15)
        
        if not data_pasar:
            continue
        
        for pasar in data_pasar:
            try:
                id_pasar = pasar["psr_id"]
                nm_pasar = pasar["psr_nama"]
                id_kabkota = pasar["kab_id"]
                nama_kabkota = pasar["kab_nama"]
                
                data_komoditas = array_data_komoditas(tanggal, id_kabkota, nama_kabkota, my_kabkota, id_pasar, nm_pasar)
                
                for item in data_komoditas:
                    try:
                        # Cari atau buat kategori
                        cursor.execute("SELECT id FROM kategori_komoditas WHERE kategori ILIKE %s", (item[5],))
                        result = cursor.fetchone()
                        
                        if result:
                            kategori_id = result[0]
                        else:
                            cursor.execute("INSERT INTO kategori_komoditas (kategori) VALUES (%s) RETURNING id", (item[5],))
                            kategori_id = cursor.fetchone()[0]
                            connection.commit()
                        
                        # Insert data komoditas
                        val = (item[0], item[3], kategori_id, item[6], item[7], item[9])
                        cursor.execute(query_komoditas, val)
                        connection.commit()
                        pasar_total += 1
                    except Exception as e:
                        pasar_error += 1
                        connection.rollback()
            except Exception as e:
                pasar_error += 1
        
        time.sleep(1)  # Rate limiting
    
    if pasar_total > 0:
        print(f"  ✓ Pasar: {pasar_total} items")
    if pasar_error > 0:
        print(f"  ✗ Pasar errors: {pasar_error}")

cursor.close()
connection.close()

print(f"\n{'='*60}")
print("✓ SCRAPING SELESAI!")
print(f"{'='*60}")
