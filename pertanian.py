from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import psycopg2
import requests
import pathlib

connection = psycopg2.connect(
     user="postgres",
    password="postgres123",
    host="127.0.0.1",
    port="5434",
    database="smartfarming"
)
  
cursor = connection.cursor()
def get_data_tanggal():
  try:
    getDate = """
    SELECT tanggal 
    FROM "komoditas_rata-rata" 
    ORDER BY tanggal DESC 
    LIMIT 1

    """
    cursor.execute(getDate)
    date = cursor.fetchone()  # Ambil satu hasil
    start_date = date[0] + timedelta(days=1) if date else datetime(2025, 7, 1).date()
  except psycopg2.errors.UndefinedTable:
    # If table doesn't exist, start from default date
    start_date = datetime(2025, 7, 1).date()
  print("Mulai dari tanggal: ", start_date)
  return start_date


# Tentukan tanggal awal
start_date = get_data_tanggal()
# Tentukan tanggal akhir (kemarin)
# end_date = datetime.now().date() - timedelta(days=1)
end_date = start_date
# Buat list tanggal di antara start_date dan end_date
date_list = [(start_date + timedelta(days=i)).strftime("%Y-%m-%d") 
             for i in range((end_date - start_date).days + 1)]

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
      str = td.string or ""
      arr_komoditas.append(str.strip())
    nomor = arr_komoditas[1]
    nama = arr_komoditas[3].replace('- ','')
    satuan = arr_komoditas[5]

    hrg_current = arr_komoditas[9].replace('.','')
    perubahan = arr_komoditas[11].replace('.','')
    if nomor:
      kategori = nama
      if satuan:
        #print(kategori+" -> "+nama)
        data_komoditas_mean.append([tanggal, kategori, nama, satuan, hrg_current])
    else:
      if satuan:
        data_komoditas_mean.append([tanggal, kategori, nama, satuan, hrg_current])
  return data_komoditas_mean

def get_data_pasar(my_kabkota):
  url = "https://siskaperbapo.jatimprov.go.id/harga/pasar.json/{}".format(my_kabkota)
  res = requests.get(url)
  return res.json()

def array_data_komoditas(tanggal, id_kabkota, nm_kabkota, keycode_kabkota, id_pasar, nm_pasar):
  url = "https://siskaperbapo.jatimprov.go.id/harga/tabel.nodesign/"
  post_data = {"tanggal":tanggal, "kabkota":keycode_kabkota, "pasar":id_pasar}
  #headers = { "User-Agent" : ua.random }
  page = requests.post(url, data=post_data)
  soup = BeautifulSoup(page.content, 'html.parser')
  rows = soup.find_all('tr')
  # print(rows)
  kategori = ""
  data_komoditas=[]
  for row in rows:
    arr_komoditas = []
    for td in row:
      str = td.string or ""
      arr_komoditas.append(str.strip())
    nomor = arr_komoditas[1]
    nama = arr_komoditas[3].replace('- ','')
    satuan = arr_komoditas[5]
    hrg_last = arr_komoditas[7].replace('.','')
    hrg_current = arr_komoditas[9].replace('.','')
    perubahan = arr_komoditas[11].replace('.','')
    if nomor:
      # print('nomor dan nama',nomor)
      kategori = nama
      # print('kategori',kategori+" -> "+nama)
      if satuan:
        # print('satuan',satuan)
        #print(kategori+" -> "+nama)
        data_komoditas.append([tanggal, id_kabkota, nm_kabkota, id_pasar, nm_pasar, kategori, nama, satuan, hrg_last, hrg_current, perubahan])
    else:
      if satuan:
        data_komoditas.append([tanggal, id_kabkota, nm_kabkota, id_pasar, nm_pasar, kategori, nama, satuan, hrg_last, hrg_current, perubahan])
  return data_komoditas

list_kabkota = ["bangkalankab", "banyuwangikab", "blitarkab", "bojonegorokab", "bondowosokab",
    "gresikkab", "jemberkab", "jombangkab", "kedirikab", "lamongankab", "lumajangkab", "madiunkab",
    "magetankab", "malangkab", "mojokertokab", "nganjukkab", "ngawikab", "pacitankab", "pamekasankab",
    "pasuruankab", "ponorogokab", "probolinggokab", "sampangkab", "sidoarjokab", "situbondokab", "sumenepkab",
    "trenggalekkab", "tubankab", "tulungagungkab", "batukota", "blitarkota", "kedirikota", "madiunkota",
    "malangkota", "mojokertokota", "pasuruankota", "probolinggokota", "surabayakota"]

query_mean = """
INSERT INTO "komoditas_rata-rata"
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
    
        val = (
            item[0], 
            kategori_id,  # Gunakan kategori_id dari query
            item[2], 
            item[3], 
            item[4], 
        )
        
        # Eksekusi queryInsert
        cursor.execute(query_mean, val)

  for my_kabkota in list_kabkota:
    data_pasar = get_data_pasar(my_kabkota)
    print("--> INSERT DATA Kab_Kota: "+my_kabkota) 
    for my_pasar in data_pasar:
      id_pasar=my_pasar["psr_id"]
      nm_pasar=my_pasar["psr_nama"]
      id_kabkota=my_pasar["kab_id"]
      nama_kabkota=my_pasar["kab_nama"]
      data_komoditas = array_data_komoditas(tanggal,id_kabkota,nama_kabkota,my_kabkota,id_pasar,nm_pasar)
      for item in data_komoditas:
        # print(item)
        # Query untuk memfilter kategori
        queryFilter = "SELECT id FROM kategori_komoditas WHERE kategori LIKE %s"
        
        # Tambahkan wildcard (%) di sekitar item['kategori']
        search_pattern = f"%{item[5]}%"
        
        # Eksekusi queryFilter
        cursor.execute(queryFilter, (search_pattern,))
        kategori_result = cursor.fetchone()  # Ambil hasil query
        
        if kategori_result:  # Jika data ditemukan
            kategori_id = kategori_result[0]  # Ambil ID dari hasil query
            val = (
                item[0], 
                item[3], 
                kategori_id,  # Gunakan kategori_id dari query
                item[6], 
                item[7], 
                item[9], 
            )
            # Eksekusi queryInsert
            cursor.execute(query, val)
      # print(data_komoditas)
  connection.commit()

cursor.close()
connection.close()

