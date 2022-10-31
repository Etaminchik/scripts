import psycopg2
from datetime import datetime

hierarchy = 0 # 0/1

con = psycopg2.connect(
  database="vasexperts",
  user="oim_admin",
  password="admin",
  host="127.0.0.1",
  port="54321"
)

cur = con.cursor()
cur.execute("SELECT subs_external_identifier FROM oims.subscribers")
rows = cur.fetchall()
if hierarchy == 0:
  file = open(f'deleted_abonents_' + datetime.now().strftime("%d%m%Y%H%M%S") + '.txt', 'w')
  for row in rows:
    file.write('"' + row[0] + '";"";' + '\n')
  file.close()

if hierarchy == 1:
  file = open(f'deleted_abonents_' + datetime.now().strftime("%d%m%Y%H%M%S") + '.txt', 'w')
  for row in rows:
    file.write('"' +row[0].replace('_','";"') + '";' + '\n')
  file.close()
con.close()