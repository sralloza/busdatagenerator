import re
from dataclasses import dataclass, field, astuple
from datetime import datetime, timedelta

import matplotlib.pyplot as plt
from bs4 import BeautifulSoup as Soup
from pandas import DataFrame
from rpi.downloader import Downloader

from busdatagenerator import Register, DataBase


class InvalidStopIdError(Exception):
    """Número de parada inválida."""


@dataclass
class ExtraDato(Register):
    dt: datetime = field(init=False)
    ta: datetime

    def __post_init__(self):
        self.ta = datetime.strptime(self.ta, '%Y-%m-%d %H:%M:%S')
        self.ta = self.ta.replace(second=0)
        self.dt = self.ta + timedelta(seconds=self.delay_minutes * 60)


class GestorDatos(list):
    def __str__(self):
        return '\n'.join([repr(x) for x in self])

    @staticmethod
    def id_parada_to_str(id_parada):
        d = Downloader()
        r = d.get('http://www.auvasa.es/parada.asp?codigo=' + str(id_parada))
        s = Soup(r.content, 'html.parser')

        if 'No hay información de líneas en servicio para la parada indicada' in r.text:
            raise InvalidStopIdError

        c = s.findAll('h5')

        return re.search(r'[\w\s]+', c[1].text.strip()).group().strip()

    @classmethod
    def cargar(cls):
        self = GestorDatos.__new__(cls)
        self.__init__()
        self.database = DataBase()
        self.database.use()
        datos = self.database.cur.execute("select * from busstats")

        # with open(JSON_PATH, 'rt', encoding='utf-8') as fh:
        #     datos = json.load(fh)

        for d in datos:
            self.append(ExtraDato(*d[1:]))

        self.sort(key=lambda k: k.dt)

        return self

    def filtrar(self, linea=None, id_parada=None):
        o = []
        for elem in self:
            if linea is not None:
                if elem.linea != str(linea):
                    continue

            if id_parada is not None:
                if elem.id_parada != id_parada:
                    continue

            o.append(elem)

        self.__init__(o)

    def analizar(self):
        medidas = DataFrame([astuple(x) for x in self], columns=['linea', 'ta', 'tr', 'id_parada', 'dtm'])
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
    gc = GestorDatos.cargar()
    gc.analizar()
