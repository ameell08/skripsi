import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import psycopg2
from psycopg2.extras import execute_values

# ==================== KONFIGURASI ====================
DB_CONFIG = {
    "user": "postgres",
    "password": "postgres123",
    "host": "127.0.0.1",
    "port": "5434",
    "database": "smartfarming"
}

# Komoditas yang ingin diambil (sesuaikan exact nama di situs)
TARGET_KOMODITAS = {
    "Cabe Merah Keriting",
    "Cabe Rawit Merah",
    "Tomat",
    "Beras premium",
    "beras medium",
    "Telur Ayam Ras"  # atau "Telur Ayam Negeri" tergantung situs
}

# Rentang tanggal
START_DATE = datetime(2025, 7, 1).date()
END_DATE = datetime(2025, 12, 31).date()

# List kabupaten/kota (key dari situs)
LIST_KABKOTA = [
    "bangkalankab", "banyuwangikab", "blitarkab", "bojonegorokab", "bondowosokab",
    "gresikkab", "jemberkab", "jombangkab", "kedirikab", "lamongankab", "lumajangkab",
    "madiunkab", "magetankab", "malangkab", "mojokertokab", "nganjukkab", "ngawikab",
    "pacitankab", "pamekasankab", "pasuruankab", "ponorogokab", "probolinggokab",
    "sampangkab", "sidoarjokab", "situbondokab", "sumenepkab", "trenggalekkab",
    "tubankab", "tulungagungkab", "batukota", "blitarkota", "kedirikota",
    "madiunkota", "malangkota", "mojokertokota", "pasuruankota", "probolinggokota",
    "surabayakota"
]

# ==================== KONEKSI DB ====================
conn = psycopg2.connect(**DB_CONFIG)
cur = conn.cursor()

# ==================== BUAT TABEL JIKA BELUM ADA ====================
cur.execute("""
CREATE TABLE IF NOT EXISTS kategori_komoditas (
    id SERIAL PRIMARY KEY,
    kategori VARCHAR(100) UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS komoditas_rata_rata (
    id SERIAL PRIMARY KEY,
    tanggal DATE NOT NULL,
    kategori_id INTEGER REFERENCES kategori_komoditas(id),
    komoditas_nama VARCHAR(100) NOT NULL,
    satuan VARCHAR(20),
    harga INTEGER,
    UNIQUE(tanggal, kategori_id, komoditas_nama)
);

CREATE TABLE IF NOT EXISTS komoditas (
    id SERIAL PRIMARY KEY,
    tanggal DATE NOT NULL,
    pasar_id INTEGER NOT NULL,
    kategori_id INTEGER REFERENCES kategori_komoditas(id),
    komoditas_nama VARCHAR(100) NOT NULL,
    satuan VARCHAR(20),
    harga INTEGER
);
""")
conn.commit()

# ==================== FUNGSI BANTUAN ====================
def get_or_create_kategori(nama_kategori):
    cur.execute("SELECT id FROM kategori_komoditas WHERE kategori = %s", (nama_kategori,))
    result = cur.fetchone()
    if result:
        return result[0]
    else:
        cur.execute("INSERT INTO kategori_komoditas (kategori) VALUES (%s) RETURNING id", (nama_kategori,))
        conn.commit()
        return cur.fetchone()[0]

def clean_price(price_str):
    if not price_str or price_str == '-' or price_str == '':
        return None
    return int(price_str.replace('.', ''))

# ==================== SCRAPING RATA-RATA PROVINSI ====================
def scrape_rata_rata(tanggal):
    url = "https://siskaperbapo.jatimprov.go.id/harga/tabel.nodesign/"
    response = requests.post(url, data={"tanggal": tanggal})
    soup = BeautifulSoup(response.content, 'html.parser')
    
    data = []
    kategori = ""
    
    for row in soup.find_all('tr'):
        cells = [cell.get_text(strip=True) for cell in row.find_all('td')]
        if len(cells) < 12:
            continue
            
        nomor = cells[1] if len(cells) > 1 else ""
        nama = cells[3].replace('- ', '') if len(cells) > 3 else ""
        satuan = cells[5] if len(cells) > 5 else ""
        hrg_current = clean_price(cells[9] if len(cells) > 9 else "")
        
        if nomor:  # Baris kategori utama
            kategori = nama
            if nama in TARGET_KOMODITAS and satuan and hrg_current is not None:
                data.append((tanggal, kategori, nama, satuan, hrg_current))
        else:
            if nama in TARGET_KOMODITAS and satuan and hrg_current is not None:
                data.append((tanggal, kategori, nama, satuan, hrg_current))
    
    return data

# ==================== SCRAPING PER PASAR ====================
def get_pasar_list(kabkota_key):
    url = f"https://siskaperbapo.jatimprov.go.id/harga/pasar.json/{kabkota_key}"
    try:
        return requests.get(url).json()
    except:
        return []

def scrape_per_pasar(tanggal, kabkota_key):
    pasar_list = get_pasar_list(kabkota_key)
    all_data = []
    
    for pasar in pasar_list:
        id_pasar = pasar["psr_id"]
        nm_pasar = pasar["psr_nama"]
        id_kab = pasar["kab_id"]
        nm_kab = pasar["kab_nama"]
        
        response = requests.post(
            "https://siskaperbapo.jatimprov.go.id/harga/tabel.nodesign/",
            data={"tanggal": tanggal, "kabkota": kabkota_key, "pasar": id_pasar}
        )
        soup = BeautifulSoup(response.content, 'html.parser')
        
        kategori = ""
        for row in soup.find_all('tr'):
            cells = [cell.get_text(strip=True) for cell in row.find_all('td')]
            if len(cells) < 12:
                continue
                
            nomor = cells[1] if len(cells) > 1 else ""
            nama = cells[3].replace('- ', '') if len(cells) > 3 else ""
            satuan = cells[5] if len(cells) > 5 else ""
            hrg_current = clean_price(cells[9] if len(cells) > 9 else "")
            
            if nomor:
                kategori = nama
                if nama in TARGET_KOMODITAS and satuan and hrg_current is not None:
                    all_data.append((tanggal, id_pasar, kategori, nama, satuan, hrg_current))
            else:
                if nama in TARGET_KOMODITAS and satuan and hrg_current is not None:
                    all_data.append((tanggal, id_pasar, kategori, nama, satuan, hrg_current))
    
    return all_data

# ==================== MAIN LOOP ====================
date_list = [START_DATE + timedelta(days=i) for i in range((END_DATE - START_DATE).days + 1)]

for current_date in date_list:
    tanggal_str = current_date.strftime("%Y-%m-%d")
    print(f"Memproses tanggal: {tanggal_str}")
    
    # --- Rata-rata Provinsi ---
    data_mean = scrape_rata_rata(tanggal_str)
    values_mean = []
    for tanggal, kat, nama, satuan, harga in data_mean:
        kat_id = get_or_create_kategori(kat)
        values_mean.append((tanggal, kat_id, nama, satuan, harga))
        
    if values_mean:
        execute_values(cur, """
            INSERT INTO "komoditas_rata-rata"
            (tanggal,kategori_id,komoditas_nama,satuan,harga)
            VALUES (%s, %s, %s, %s, %s)
        """, values_mean)
    
    # --- Per Pasar ---
    for kabkota in LIST_KABKOTA:
        data_pasar = scrape_per_pasar(tanggal_str, kabkota)
        values_pasar = []
        for tanggal, pasar_id, kat, nama, satuan, harga in data_pasar:
            kat_id = get_or_create_kategori(kat)
            values_pasar.append((tanggal, pasar_id, kat_id, nama, satuan, harga))
        
        if values_pasar:
            execute_values(cur, """
                INSERT INTO komoditas
                (tanggal, pasar_id,kategori_id,komoditas_nama,satuan,harga)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, values_pasar)
    
    conn.commit()
    print(f"Selesai untuk {tanggal_str}")

# ==================== SELESAI ====================
cur.close()
conn.close()
print("Semua data berhasil diambil dan disimpan!")