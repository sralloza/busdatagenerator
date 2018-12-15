import hashlib
import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from sqlite3 import IntegrityError

from bs4 import BeautifulSoup as Soup
from rpi.conexiones import Conexiones
from rpi.downloader import Downloader
from rpi.rpi_logging import Logger

DATABASE_PATH = 'D:/PYTHON/.development/busdatagenerator/busstats.sqlite'
JSON_PATH = 'D:/PYTHON/.development/busdatagenerator/data.json'


class DataBase:
    def __init__(self):
        self.con = None
        self.cur = None

    def usar(self, database_path=None):
        if self.con is not None:
            return
        if database_path is None:
            database_path = DATABASE_PATH
        self.con = sqlite3.connect(database_path)

        self.cur = self.con.cursor()
        self.cur.execute("""create table if not exists busstats (
        id varchar primary key,
        linea varchar not null,
        ta varchar not null,
        tr integer not null,
        id_parada integer not null)""")

    def nuevo_dato(self, dato, quiet=False):
        data = (dato.id, dato.linea, dato.ta, dato.tr, dato.id_parada)
        try:
            self.cur.execute("insert into busstats values(?,?,?,?,?)", data)
            self.con.commit()
            return True
        except IntegrityError:
            if quiet is False:
                print('Ya existe en la base de datos: ' + str(dato))
            return False

    @staticmethod
    def get_ids():
        self = DataBase.__new__(DataBase)
        self.__init__()
        self.usar()

        self.cur.execute('select id from busstats')
        datos = [x[0] for x in self.cur.fetchall()]
        return tuple(datos)


db = DataBase()


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

    @property
    def id(self):
        p = (self.linea, self.ta, self.id_parada)
        return hashlib.sha1(str(p).encode()).hexdigest()

    def to_database(self, quiet=False):
        db.usar()
        return db.nuevo_dato(self, quiet)

    def save(self, filename=None):
        if filename is None:
            filename = JSON_PATH

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
            dato = Dato(t[0], datetime.today().strftime('%Y-%m-%d %H:%M:%S'), int(t[-1]), numero_parada)
        except ValueError:
            continue

        if lineas is not None:
            if dato.linea in lineas:
                o.append(dato)
        else:
            o.append(dato)

    return tuple(o)


logger = Logger.get(__file__, __name__)

if __name__ == '__main__':
    try:
        datos = get_data(numero_parada=686, lineas=2)
        datos += get_data(numero_parada=812, lineas=(2, 8))
        datos += get_data(numero_parada=833, lineas=(2, 8))

        for foo in datos:
            foo.save()
    except Exception as e:
        logger.critical(str(e))
        Conexiones.enviar_email('sralloza@gmail.com', 'Error en la generación de datos del bus',
                                'se ha producido la siguiente excepción:\n\n\n' + str(e))
