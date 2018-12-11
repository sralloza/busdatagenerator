import json
from dataclasses import dataclass
from datetime import datetime

from bs4 import BeautifulSoup as Soup
from rpi.conexiones import Conexiones
from rpi.downloader import Downloader
from rpi.rpi_logging import Logger

logger = Logger.get(__file__, __name__)


@dataclass
class Dato:
    linea: str
    ta: str  # Tiempo actual
    tr: int  # Tiempo restante
    id_parada: int

    def __post_init__(self):
        self.linea = str(self.linea)
        self.ta = str(self.ta)
        self.tr = int(self.tr)
        self.id_parada = int(self.id_parada)

    def save(self, filename=None):
        if filename is None:
            filename = 'data.json'

        try:
            with open(filename, 'rt', encoding='utf-8') as fh:
                data = json.load(fh)
        except FileNotFoundError:
            data = []

        data.append(vars(self))

        with open(filename, 'wt', encoding='utf-8') as fh:
            json.dump(data, fh, ensure_ascii=False, indent=4, sort_keys=True)


def get_data(numero_parada, lineas=None):
    if lineas is None:
        lineas = None
    elif isinstance(lineas, int):
        lineas = (str(lineas),)
    elif isinstance(lineas, str):
        lineas = (lineas,)
    else:
        lineas = tuple([str(x) for x in lineas])

    d = Downloader()
    r = d.get(f'http://www.auvasa.es/parada.asp?codigo={numero_parada}')
    s = Soup(r.content, 'html.parser')

    search = s.findAll('tr')
    o = []

    for item in search:
        search2 = list(item.findAll('td'))
        if search2 is None:
            continue
        if len(search2) == 0:
            continue
        t = [x.text for x in search2]
        try:
            if '+' in t[-1]:
                t[-1] = 999
            dato = Dato(t[0], datetime.today().strftime('%H:%M'), int(t[-1]), numero_parada)
        except ValueError:
            continue

        if lineas is not None:
            if dato.linea in lineas:
                o.append(dato)
        else:
            o.append(dato)

    return tuple(o)


if __name__ == '__main__':
    try:
        datos = get_data(numero_parada=686, lineas=2)
        datos += get_data(numero_parada=812, lineas=(2, 8))

        for foo in datos:
            print(foo)
            foo.save()
    except Exception as e:
        logger.critical(str(e))
        Conexiones.enviar_email('sralloza@gmail.com', 'Error en la generación de datos del bus',
                                'se ha producido la siguiente excepción:\n\n\n' + str(e))
