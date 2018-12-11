import json
from dataclasses import dataclass, field
from datetime import time, timedelta, datetime, date

import matplotlib.pyplot as plt
from numpy import mean

from busdatagenerator import Dato


@dataclass
class ExtraDato(Dato):
    dt: datetime = field(init=False)

    def __post_init__(self):
        hora, minuto = [int(x) for x in self.ta.split(':')]
        self.dt = (datetime.combine(date.today(), time(hora, minuto)) + timedelta(minutes=self.tr))  # .time()


with open('data.json', 'rt', encoding='utf-8') as fh:
    datos = json.load(fh)

# noinspection PyArgumentList
datos = [ExtraDato(**x) for x in datos]

# datos2 = []
# for x in datos:
#     print(x)
#     datos2.append(ExtraDato())
#
# del datos
# datos = datos2
# del datos2
# p = []
# for d in datos:
#     hora, minuto = [int(x) for x in d.ta.split(':')]
#     dt = (datetime.combine(date.today(), time(hora, minuto)) + timedelta(minutes=d.tr))  # .time()
#     print(dt)
#     d.dt = dt
#     p.append(dt.hour + dt.minute/60)
#
# print(type(p[0]))
# plt.scatter(range(len(p)), p)
# plt.show()

datos.sort(key=lambda x: x.dt)

medias = []
temp = []
for i in range(len(datos)):
    # print(datos[i])
    try:
        if abs(datos[i].dt - datos[i + 1].dt) <= timedelta(minutes=5):
            temp.append(datos[i])
        else:
            datetimes = [x.dt for x in temp]
            medias.append(mean([x.dt - min(datetimes) for x in temp]) + min(datetimes))
            temp = []
    except IndexError:
        datetimes = [x.dt for x in temp]
        medias.append(mean([x.dt - min(datetimes) for x in temp]) + min(datetimes))
        temp = []

medias = [x.time() for x in medias]
for i in range(len(medias)):
    medias[i] = medias[i].replace(microsecond=0)

for foo in medias:
    print(foo)

plt.scatter(range(len(medias)), medias)
plt.show()
