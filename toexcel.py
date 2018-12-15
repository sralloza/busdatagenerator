import os
import time

from pandas import read_sql, ExcelWriter

from busdatagenerator import DataBase

db = DataBase()
db.usar()

df = read_sql('select linea,ta,tr,id_parada from busstats order by ta, linea', db.con)

print(df)

ew = ExcelWriter('busstats.xlsx')
df.to_excel(ew, index=None)
try:
    ew.save()
except PermissionError:
    os.system('taskkill -f -im excel.exe')
    time.sleep(0.2)
    ew.save()
