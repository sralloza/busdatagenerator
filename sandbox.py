import datetime

from rpi.dinamic_attributes import DinamicAttributes as DA

a = [
    DA(line='2', stop_id=833, arrival_date=datetime.date(2018, 12, 26),arrival_time=datetime.time(8, 4)),
    DA(line='2', stop_id=833, arrival_date=datetime.date(2018, 12, 13),arrival_time=datetime.time(8, 9)),
    DA(line='2', stop_id=833, arrival_date=datetime.date(2018, 12, 15),arrival_time=datetime.time(8, 14)),
    DA(line='2', stop_id=833, arrival_date=datetime.date(2018, 12, 14),arrival_time=datetime.time(8, 19)),
    DA(line='2', stop_id=833, arrival_date=datetime.date(2018, 12, 13),arrival_time=datetime.time(8, 24)),
    DA(line='2', stop_id=833, arrival_date=datetime.date(2018, 12, 13),arrival_time=datetime.time(8, 29)),
    DA(line='2', stop_id=833, arrival_date=datetime.date(2018, 12, 17),arrival_time=datetime.time(8, 34)),
    DA(line='2', stop_id=833, arrival_date=datetime.date(2018, 12, 14),arrival_time=datetime.time(8, 39)),
    DA(line='2', stop_id=833, arrival_date=datetime.date(2018, 12, 13),arrival_time=datetime.time(8, 44)),
    DA(line='2', stop_id=833, arrival_date=datetime.date(2018, 12, 16),arrival_time=datetime.time(8, 49)),
    DA(line='2', stop_id=833, arrival_date=datetime.date(2018, 12, 13),arrival_time=datetime.time(8, 54)),
    DA(line='2', stop_id=833, arrival_date=datetime.date(2018, 12, 18),arrival_time=datetime.time(8, 59)),
    DA(line='2', stop_id=833, arrival_date=datetime.date(2018, 12, 16),arrival_time=datetime.time(9, 4)),
    DA(line='2', stop_id=833, arrival_date=datetime.date(2018, 12, 26),arrival_time=datetime.time(9, 9)),
]

a.sort(key=lambda x: x.arrival_date)

for i in range(len(a)):
    a[i].arrival_date = str(a[i].arrival_date)
    a[i].arrival_time = str(a[i].arrival_time)

for elem in a:
    print(elem)
