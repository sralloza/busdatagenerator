#!/usr/bin/python

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

    def use(self, database_path=None):
        if self.con is not None:
            return
        if database_path is None:
            database_path = DATABASE_PATH
        self.con = sqlite3.connect(database_path)

        self.cur = self.con.cursor()
        self.cur.execute("""create table if not exists busstats (
        id varchar primary key,
        line varchar not null,
        actual_time varchar not null,
        delay_minutes integer not null,
        stop_id integer not null)""")

    def new_register(self, register, quiet=False):
        data = (register.id, register.line, register.actual_time, register.delay_minutes, register.stop_id)
        try:
            self.cur.execute("insert into busstats values(?,?,?,?,?)", data)
            self.con.commit()
            return True
        except IntegrityError:
            if quiet is False:
                print('Register already exists: ' + str(register))
            return False

    def insert_multiple_registers(self, data):
        """Saves multiple registers at once.

        :type data: Iterable[Register]
        """

        values = []
        ids = self.get_ids()

        for d in data:
            if d.id not in ids:
                values.append((d.id, d.line, d.actual_time, d.delay_minutes, d.stop_id))

        values = tuple(values)

        self.cur.executemany("insert into busstats values(?,?,?,?,?)", values)
        self.con.commit()
        return len(values)

    @staticmethod
    def get_ids():
        self = DataBase.__new__(DataBase)
        self.__init__()
        self.use()

        self.cur.execute('select id from busstats')
        registers = [x[0] for x in self.cur.fetchall()]
        return tuple(registers)


db = DataBase()


@dataclass
class Register:
    line: str
    actual_time: str
    delay_minutes: int
    stop_id: int

    def __post_init__(self):
        self.line = str(self.line)
        self.actual_time = str(self.actual_time)
        self.delay_minutes = int(self.delay_minutes)
        self.stop_id = int(self.stop_id)

    @property
    def id(self):
        p = (self.line, self.actual_time, self.stop_id)
        return hashlib.sha1(str(p).encode()).hexdigest()

    def to_database(self, quiet=False):
        db.use()
        return db.new_register(self, quiet)

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


def analyse_stop(stop_number: int, lines=None):
    if lines is None:
        lines = None
    elif isinstance(lines, int):
        lines = (str(lines),)
    elif isinstance(lines, str):
        lines = (lines,)
    else:
        lines = tuple([str(x) for x in lines])

    d = Downloader()
    r = d.get(f'http://www.auvasa.es/parada.asp?codigo={stop_number}')
    s = Soup(r.content, 'html.parser')

    search = s.findAll('tr')
    output = []

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
            register = Register(t[0], datetime.today().strftime('%Y-%m-%d %H:%M:%S'), int(t[-1]), stop_number)
        except ValueError:
            continue

        if lines is not None:
            if register.line in lines:
                output.append(register)
        else:
            output.append(register)

    return tuple(output)


logger = Logger.get(__file__, __name__)


def generate_data():
    try:
        data = []
        data += analyse_stop(stop_number=686, lines=2)  # Gamazo
        data += analyse_stop(stop_number=682, lines=8)  # Fray luis de león
        data += analyse_stop(stop_number=812, lines=(2, 8))  # Fuente dorada
        data += analyse_stop(stop_number=833, lines=(2, 8))  # Clínico
        data += analyse_stop(stop_number=880, lines=2)  # Donde nos deja el 2 en ciencias
        data += analyse_stop(stop_number=1191, lines=8)  # Parada anterior a la del campus
        data += analyse_stop(stop_number=1358, lines=8)  # Campus miguel delibes

        for register in data:
            register.save()
    except Exception as e:
        logger.critical(str(e))
        Conexiones.enviar_email('sralloza@gmail.com', 'Error en la generación de datos del bus',
                                'se ha producido la siguiente excepción:\n\n\n' + traceback.format_exc())


def to_excel_main():
    db.use()
    df = read_sql('select linea,ta,tr,id_parada from busstats order by ta, linea', db.con)

    print(f'Dimensions: {df.shape}')

    ew = ExcelWriter('busstats.xlsx')
    df.to_excel(ew, index=None)
    try:
        ew.save()
    except PermissionError:
        os.system('taskkill -f -im excel.exe')
        time.sleep(0.2)
        ew.save()


def update_database():
    try:
        with open(JSON_PATH) as fh:
            data = json.load(fh)
    except FileNotFoundError:
        print(f'File {JSON_PATH!r} does not exist')
        return 0, 0, False

    data = [Register(**x) for x in data]

    saved_ids = DataBase.get_ids()
    new_ids = [x.id for x in data if x.id not in saved_ids]

    registers_number = len(new_ids)

    print(f'Found {registers_number} new registers')

    db.use()

    saved = db.insert_multiple_registers(data)

    return registers_number, saved, True


def main_update_database():
    if platform.system() == 'Linux':
        raise InvalidPlatformError('Database can only be used in Windows')
    from rpi.tiempo import segs_to_str
    t0 = time.time()
    total = 0
    saved = 0
    secure_token = False

    try:
        total, saved, secure_token = update_database()
    except KeyboardInterrupt:
        pass
    except Exception:
        raise
    finally:
        if saved == 0:
            print(f"No registers have been saved")
        else:
            print(f'Saved {saved} registers')

        print(f'Executed in {segs_to_str(time.time() - t0)}')

        if total != 0:
            print(f'Mean speed: {total / (time.time() - t0):.2f} registers/s')

        if total == saved and secure_token is True:
            os.remove(JSON_PATH)
            print(f'Deleted file {JSON_PATH!r}')
        else:
            print(f'File {JSON_PATH!r} has not been removed (total != saved'
                  f', {total} != {saved}, securetoken={secure_token})')


#

def send_by_email(path=None):
    if path is None:
        path = JSON_PATH

    seconds = datetime.today().second
    i = 0

    while seconds != 15:
        if i == 0:
            estimation = 15 - seconds
            while estimation < 0:
                estimation += 60
            print(f'Waiting for seconds=15 ({estimation})')
        time.sleep(0.5)
        seconds = datetime.today().second
        i += 1

    print('Sending...')

    if os.path.isfile(path) is False:
        print(f'File {path!r} does not exist')
        return

    r = Conexiones.enviar_email('sralloza@gmail.com', 'Datos de autobuses', '', files=path)

    if r is True:
        os.remove(path)
        print('File deleted')
    else:
        print('File can not be deleted')


if __name__ == '__main__':
    if len(sys.argv) == 1 and platform.system() == 'Linux':
        sys.argv.append('-generate')

    parser = argparse.ArgumentParser(prog='BusStats')
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('-generate', action='store_true')
    group.add_argument('-update', '-updatedatabase', action='store_true')
    group.add_argument('-registers', action='store_true')
    group.add_argument('-toexcel', '-excel', action='store_true')
    group.add_argument('-mail', '-send', action='store_true')

    opt = vars(parser.parse_args())

    if opt['generate'] is True:
        generate_data()
        exit()
    elif opt['update'] is True:
        main_update_database()
        exit()
    elif opt['toexcel'] is True:
        to_excel_main()
        exit()
    elif opt['mail'] is True:
        send_by_email()
        exit()
