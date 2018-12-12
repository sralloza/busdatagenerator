import json
from dataclasses import dataclass, field, astuple
from datetime import time, timedelta, datetime, date

import matplotlib.pyplot as plt
from pandas import DataFrame

from busdatagenerator import Dato


@dataclass
class ExtraDato(Dato):
    dt: datetime = field(init=False)

    def __post_init__(self):
        hora, minuto = [int(x) for x in self.ta.split(':')]
        self.dt = (datetime.combine(date.today(), time(hora, minuto)) + timedelta(minutes=self.tr))  # .time()

def analizar():
    with open('data.json', 'rt', encoding='utf-8') as fh:
        datos = json.load(fh)

    # noinspection PyArgumentList
    datos = [ExtraDato(**x) for x in datos]

    datos.sort(key=lambda x: x.dt)

    medidas = DataFrame([astuple(x) for x in datos], columns=['linea', 'ta', 'tr', 'id_parada', 'dtm'])
    # medias = []
    # temp = []
    # for i in range(len(datos)):
    #     print(datos[i])
    #     try:
    #         if abs(datos[i].dt - datos[i + 1].dt) <= timedelta(minutes=5):
    #             temp.append(datos[i])
    #         else:
    #             datetimes = [x.dt for x in temp]
    #             medias.append(mean([x.dt - min(datetimes) for x in temp]) + min(datetimes))
    #             temp = []
    #     except IndexError:
    #         datetimes = [x.dt for x in temp]
    #         medias.append(mean([x.dt - min(datetimes) for x in temp]) + min(datetimes))
    #         temp = []
    #
    # medias = [x.time() for x in medias]
    # for i in range(len(medias)):
    #     medias[i] = medias[i].replace(microsecond=0)
    #
    # plt.scatter(range(len(medias)), medias)
    # plt.show()

    medidas.set_index('ta')
    medidas.loc[:, 'dtm'] = medidas.apply(lambda x: x.dtm.time(), axis=1)
    print(medidas)

    medidas.plot()
    plt.show()

if __name__ == '__main__':
    analizar()