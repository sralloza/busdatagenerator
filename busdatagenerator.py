import argparse
import hashlib
import json
import os
import platform
import sqlite3
import sys
import time
import traceback
from dataclasses import dataclass
from datetime import datetime
from sqlite3 import IntegrityError
from typing import Iterable

from bs4 import BeautifulSoup as Soup
from pandas import read_sql, ExcelWriter
from rpi.conexiones import Conexiones
from rpi.downloader import Downloader
from rpi.rpi_logging import Logger

if platform.system() == 'Linux':
    DATABASE_PATH = None
    JSON_PATH = '/home/pi/data.json'
else:
    DATABASE_PATH = 'D:/PYTHON/.development/busdatagenerator/busstats.sqlite'
    JSON_PATH = 'D:/Sistema/Downloads/data.json'


class InvalidPlatformError(Exception):
    """Plataforma inválida"""


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

    def insert_multiple_data(self, data: Iterable):
        values = []
        ids = self.get_ids()

        for d in data:
            if d.id not in ids:
                values.append((d.id, d.linea, d.ta, d.tr, d.id_parada))

        values = tuple(values)

        self.cur.executemany("insert into busstats values(?,?,?,?,?)", values)
        self.con.commit()
        return len(values)

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


def generate_data():
    try:
        datos = []
        datos += get_data(numero_parada=686, lineas=2)  # Gamazo
        datos += get_data(numero_parada=682, lineas=8)  # Fray luis de león
        datos += get_data(numero_parada=812, lineas=(2, 8))  # Fuente dorada
        datos += get_data(numero_parada=833, lineas=(2, 8))  # Clínico
        datos += get_data(numero_parada=880, lineas=2)  # Donde nos deja el 2 en ciencias
        datos += get_data(numero_parada=1191, lineas=8)  # Parada anterior a la del campus
        datos += get_data(numero_parada=1358, lineas=8)  # Campus miguel delibes

        for foo in datos:
            foo.save()
    except Exception as e:
        logger.critical(str(e))
        Conexiones.enviar_email('sralloza@gmail.com', 'Error en la generación de datos del bus',
                                'se ha producido la siguiente excepción:\n\n\n' + traceback.format_exc())


def to_excel_main():
    db.usar()
    df = read_sql('select linea,ta,tr,id_parada from busstats order by ta, linea', db.con)

    print(f'Dimensiones: {df.shape}')

    ew = ExcelWriter('busstats.xlsx')
    df.to_excel(ew, index=None)
    try:
        ew.save()
    except PermissionError:
        os.system('taskkill -f -im excel.exe')
        time.sleep(0.2)
        ew.save()


def update_database():
    with open(JSON_PATH) as fh:
        data = json.load(fh)

    data = [Dato(**x) for x in data]

    ids_guardadas = DataBase.get_ids()
    ids_nuevas = [x.id for x in data if x.id not in ids_guardadas]

    total_registros = len(ids_nuevas)

    print(f'Encontrados {total_registros} registros nuevos')

    db.usar()

    registros_guardados = db.insert_multiple_data(data)

    return total_registros, registros_guardados


def main_update_database():
    if platform.system() == 'Linux':
        raise InvalidPlatformError('Sólo se puede usar en windows')
    from rpi.tiempo import segs_to_str
    t0 = time.time()
    total = 0
    guardado = 0
    try:
        total, guardado = update_database()
    except KeyboardInterrupt:
        pass
    finally:
        if total == 0:
            print(f"No se han guardado registros")
        else:
            print(f'Guardados {guardado} registros')

        print(f'Ejecutado en {segs_to_str(time.time() - t0)}')

        if total != 0:
            print(f'Velocidad media: {total / (time.time() - t0):.2f} registros/s')

        if total == guardado:
            os.remove(JSON_PATH)
            print(f'Eliminado archivo {JSON_PATH!r}')
        else:
            print(f'Archivo {JSON_PATH!r} no eliminado (total != guardado, {total} != {guardado})')

        exit(0)


def enviar_por_correo(path=None):
    if path is None:
        path = JSON_PATH

    segundos = datetime.today().second
    i = 0

    while segundos != 15:
        if i == 0:
            estimado = 15 - segundos
            while estimado < 0:
                estimado += 60
            print(f'Esperando a segundos=15 ({estimado})')
        time.sleep(0.5)
        segundos = datetime.today().second
        i += 1

    print('Enviando')

    if os.path.isfile(path) is False:
        print(f'No existe el archivo {path!r}')
        return

    r = Conexiones.enviar_email('sralloza@gmail.com', 'Datos de autobuses', '', files=path)

    if r is True:
        os.remove(path)
        print('Archivo eliminado')
    else:
        print('No se puede eliminar el archivo')


if __name__ == '__main__':

    if len(sys.argv) == 1 and platform.system() == 'Linux':
        sys.argv.append('-generar')

    parser = argparse.ArgumentParser(prog='BusStats')
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('-generar', action='store_true')
    group.add_argument('-actualizar', '-updatedatabase', action='store_true')
    group.add_argument('-numeroregistros', action='store_true')
    group.add_argument('-toexcel', '-excel', action='store_true')
    group.add_argument('-mail', '-enviar', '-correo', action='store_true')

    opt = vars(parser.parse_args())

    if opt['generar'] is True:
        generate_data()
        exit()
    elif opt['actualizar'] is True:
        main_update_database()
        exit()
    elif opt['toexcel'] is True:
        to_excel_main()
        exit()
    elif opt['mail'] is True:
        enviar_por_correo()
        exit()
