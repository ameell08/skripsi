from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import psycopg2
import requests
import pathlib
import time

connection = psycopg2.connect(
    user="postgres",
    password="postgres123",
    host="127.0.0.1",
    port="5434",
    database="smartfarming"
)


  
cursor = connection.cursor()

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
    print("Table 'komoditas_ratarata' created or already exists")
except Exception as e:
    print(f"Error creating komoditas_ratarata table: {e}")
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
    print("Table 'komoditas' created or already exists")
except Exception as e:
    print(f"Error creating komoditas table: {e}")
    connection.rollback()

try:
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS kategori_komoditas (
        id SERIAL PRIMARY KEY,
        kategori VARCHAR(255) NOT NULL
    );
    """)
    connection.commit()
    print("Table 'kategori_komoditas' created or already exists")
except Exception as e:
    print(f"Error creating kategori_komoditas table: {e}")
    connection.rollback()

def get_data_tanggal():
  getDate = """
  SELECT tanggal 
  FROM "komoditas_ratarata" 
  ORDER BY tanggal DESC 
  LIMIT 1

  """
  cursor.execute(getDate)
  date = cursor.fetchone()  # Ambil satu hasil
  start_date = date[0] + timedelta(days=1) if date else datetime(2021, 1, 1).date()
  print("Mulai dari tanggal: ", start_date)
  return start_date

# Tentukan tanggal awal
#start_date = get_data_tanggal()
# Tentukan tanggal akhir (kemarin)
# end_date = datetime.now().date() - timedelta(days=1)
#end_date = start_date
# Buat list tanggal di antara start_date dan end_date
start_date = datetime(2025, 7, 1).date()
end_date = datetime(2025, 12, 31).date()  # PRODUKSI: full year. Untuk test ubah ke datetime(2025, 1, 1).date()
date_list = [(start_date + timedelta(days=i)).strftime("%Y-%m-%d") 
             for i in range((end_date - start_date).days + 1)]
print(f"Total hari yang akan diproses: {len(date_list)}")
print(f"Range: {start_date} hingga {end_date}")

def array_data_komoditas_mean(tanggal):
  url = "https://siskaperbapo.jatimprov.go.id/harga/tabel.nodesign/"
  post_data = {"tanggal":tanggal}
  #headers = { "User-Agent" : ua.random } 
  page = requests.post(url, data=post_data)
  soup = BeautifulSoup(page.content, 'html.parser')
  rows = soup.find_all('tr')
  # print(rows)
  kategori = ""
  data_komoditas_mean=[]
  for row in rows:
    arr_komoditas = []
    for td in row:
      str_val = td.string or ""
      arr_komoditas.append(str_val.strip())
    if len(arr_komoditas) < 13:
      continue
    nomor = arr_komoditas[1]
    nama = arr_komoditas[3].replace('- ','')
    satuan = arr_komoditas[5]

    hrg_current = arr_komoditas[9].replace('.','')
    perubahan = arr_komoditas[11].replace('.','')
    if nomor:
      kategori = nama
      if satuan:
        #print(kategori+" -> "+nama)
        print(f"      [FOUND MEAN] kategori: {kategori}, nama: {nama}, satuan: {satuan}")
        data_komoditas_mean.append([tanggal, kategori, nama, satuan, hrg_current])
    else:
      if satuan and kategori:
        print(f"      [FOUND MEAN] kategori: {kategori}, nama: {nama}, satuan: {satuan}")
        data_komoditas_mean.append([tanggal, kategori, nama, satuan, hrg_current])
  return data_komoditas_mean

def get_data_pasar(my_kabkota, retries=3, timeout=10):
  url = "https://siskaperbapo.jatimprov.go.id/harga/pasar.json/{}".format(my_kabkota)
  for attempt in range(retries):
    try:
      res = requests.get(url, timeout=timeout)
      res.raise_for_status()
      return res.json()
    except requests.exceptions.RequestException as e:
      if attempt < retries - 1:
        print(f"      ⚠ Retry {attempt+1}/{retries} for {my_kabkota}: {str(e)[:50]}")
        import time
        time.sleep(2)
      else:
        print(f"      ✗ Failed to fetch pasar data for {my_kabkota}: {str(e)[:80]}")
        return []

def array_data_komoditas(tanggal, id_kabkota, nm_kabkota, keycode_kabkota, id_pasar, nm_pasar, retries=2, timeout=10):
  url = "https://siskaperbapo.jatimprov.go.id/harga/tabel.nodesign/"
  for attempt in range(retries):
    try:
      post_data = {"tanggal":tanggal, "kabkota":keycode_kabkota, "pasar":id_pasar}
      page = requests.post(url, data=post_data, timeout=timeout)
      page.raise_for_status()
      break
    except requests.exceptions.RequestException as e:
      if attempt < retries - 1:
        time.sleep(2)
      else:
        print(f"        ⚠ Failed to fetch komoditas data: {str(e)[:50]}")
        return []
  
  try:
    soup = BeautifulSoup(page.content, 'html.parser')
    rows = soup.find_all('tr')
    kategori = ""
    data_komoditas=[]
    for row in rows:
      arr_komoditas = []
      for td in row:
        str_val = td.string or ""
        arr_komoditas.append(str_val.strip())
      if len(arr_komoditas) < 12:
        continue
      nomor = arr_komoditas[1]
      nama = arr_komoditas[3].replace('- ','')
      satuan = arr_komoditas[5]
      hrg_last = arr_komoditas[7].replace('.','')
      hrg_current = arr_komoditas[9].replace('.','')
      perubahan = arr_komoditas[11].replace('.','')
      if nomor:
        kategori = nama
        if satuan:
          data_komoditas.append([tanggal, id_kabkota, nm_kabkota, id_pasar, nm_pasar, kategori, nama, satuan, hrg_last, hrg_current, perubahan])
      else:
        if satuan:
          data_komoditas.append([tanggal, id_kabkota, nm_kabkota, id_pasar, nm_pasar, kategori, nama, satuan, hrg_last, hrg_current, perubahan])
    return data_komoditas
  except Exception as e:
    print(f"        ⚠ Error parsing komoditas data: {str(e)[:50]}")
    return []

list_kabkota = ["bangkalankab", "banyuwangikab", "blitarkab", "bojonegorokab", "bondowosokab",
    "gresikkab", "jemberkab", "jombangkab", "kedirikab", "lamongankab", "lumajangkab", "madiunkab",
    "magetankab", "malangkab", "mojokertokab", "nganjukkab", "ngawikab", "pacitankab", "pamekasankab",
    "pasuruankab", "ponorogokab", "probolinggokab", "sampangkab", "sidoarjokab", "situbondokab", "sumenepkab",
    "trenggalekkab", "tubankab", "tulungagungkab", "batukota", "blitarkota", "kedirikota", "madiunkota",
    "malangkota", "mojokertokota", "pasuruankota", "probolinggokota", "surabayakota"]

query_mean = """
INSERT INTO "komoditas_ratarata"
(tanggal,kategori_id,komoditas_nama,satuan,harga)
VALUES (%s, %s, %s, %s, %s)
"""

query = """
INSERT INTO komoditas
(tanggal, pasar_id,kategori_id,komoditas_nama,satuan,harga)
VALUES (%s, %s, %s, %s, %s, %s)
"""

for tanggal in date_list:
  print("--> INSERT DATA Tanggal: "+tanggal)
  data_komoditas_mean = array_data_komoditas_mean(tanggal)
  print(f"    Data ditemukan: {len(data_komoditas_mean)} items")
  
  insert_mean_count = 0
  insert_mean_error = 0
  
  for item in data_komoditas_mean:
    # Query untuk memfilter kategori
    queryFilter = "SELECT id FROM kategori_komoditas WHERE kategori LIKE %s"
    
    # Tambahkan wildcard (%) di sekitar item['kategori']
    search_pattern = f"%{item[1]}%"
    
    # Eksekusi queryFilter
    cursor.execute(queryFilter, (search_pattern,))
    kategori_result = cursor.fetchone()  # Ambil hasil query
    
    if kategori_result:  # Jika data ditemukan
        kategori_id = kategori_result[0]  # Ambil ID dari hasil query
    else:
        # Jika kategori tidak ditemukan, insert kategori baru
        try:
            cursor.execute("INSERT INTO kategori_komoditas (kategori) VALUES (%s) RETURNING id", (item[1],))
            kategori_id = cursor.fetchone()[0]
            print(f"    ✓ INSERT kategori baru: {item[1]}")
        except Exception as e:
            print(f"    ✗ ERROR INSERT kategori: {e}")
            connection.rollback()
            insert_mean_error += 1
            continue
    
    val = (
        item[0], 
        kategori_id,  # Gunakan kategori_id dari query atau baru
        item[2], 
        item[3], 
        item[4], 
    )
    
    print(f"DEBUG MEAN INSERT: item={item}, val={val}, nama(item[2])={item[2]}")
    
    # Eksekusi queryInsert
    try:
        cursor.execute(query_mean, val)
        insert_mean_count += 1
    except Exception as e:
        print(f"    ✗ ERROR INSERT komoditas_ratarata: {e}")
        insert_mean_error += 1
  
  # Commit semua insert komoditas_ratarata sekaligus
  try:
    connection.commit()
    print(f"    ✓ INSERT komoditas_ratarata: {insert_mean_count} berhasil, {insert_mean_error} error")
  except Exception as e:
    print(f"    ✗ ERROR COMMIT komoditas_ratarata: {e}")
    connection.rollback()

  for my_kabkota in list_kabkota:
    try:
      print(f"--> FETCH DATA Kab_Kota: {my_kabkota}")
      data_pasar = get_data_pasar(my_kabkota, retries=3, timeout=15)
      if not data_pasar:
        print(f"  ⚠ SKIP {my_kabkota}: API returned empty")
        continue
      print(f"--> INSERT DATA Kab_Kota: {my_kabkota} ({len(data_pasar)} pasar)")
      
      pasar_insert_count = 0
      pasar_error_count = 0
      
      for my_pasar in data_pasar:
        try:
          id_pasar=my_pasar["psr_id"]
          nm_pasar=my_pasar["psr_nama"]
          id_kabkota=my_pasar["kab_id"]
          nama_kabkota=my_pasar["kab_nama"]
          
          print(f"    → {nm_pasar} ({id_pasar})")
          data_komoditas = array_data_komoditas(tanggal,id_kabkota,nama_kabkota,my_kabkota,id_pasar,nm_pasar)
          
          pasar_batch_count = 0
          for item in data_komoditas:
            try:
              queryFilter = "SELECT id FROM kategori_komoditas WHERE kategori LIKE %s"
              search_pattern = f"%{item[5]}%"
              cursor.execute(queryFilter, (search_pattern,))
              kategori_result = cursor.fetchone()
              
              if kategori_result:
                  kategori_id = kategori_result[0]
              else:
                  try:
                      cursor.execute("INSERT INTO kategori_komoditas (kategori) VALUES (%s) RETURNING id", (item[5],))
                      kategori_id = cursor.fetchone()[0]
                  except:
                      pasar_error_count += 1
                      continue
              
              val = (item[0], item[3], kategori_id, item[6], item[7], item[9])
              print(f"DEBUG KOMODITAS INSERT: item={item}, val={val}, nama(item[6])={item[6]}")
              cursor.execute(query, val)
              pasar_batch_count += 1
            except Exception as e:
              pasar_error_count += 1
          
          if pasar_batch_count > 0:
            pasar_insert_count += pasar_batch_count
            print(f"      ✓ {pasar_batch_count} items inserted")
            
        except Exception as e:
          pasar_error_count += 1
          print(f"    ERROR processing pasar: {str(e)[:80]}")
          
      connection.commit()
      print(f"  ✓ {my_kabkota} INSERT: {pasar_insert_count} items, ERRORS: {pasar_error_count}")
      
    except Exception as e:
      print(f"  ⚠ ERROR {my_kabkota}: {str(e)[:80]}")
      connection.rollback()
      time.sleep(2)
      continue

cursor.close()
connection.close()

