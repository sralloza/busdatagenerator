#!/usr/bin/python

"""Bus stats analyser. Made for getting bus timeouts stats."""
import argparse
import hashlib
import logging
import os
import platform
import re
import sqlite3
import sys
import time
import traceback
from csv import DictReader, DictWriter
from datetime import datetime
from typing import Iterable

import requests
from bs4 import BeautifulSoup as Soup
from dataclasses import dataclass
from rpi.connections import Connections
from rpi.custom_logging import configure_logging
from rpi.downloader import Downloader
from rpi.encryption import encrypt
from rpi.filesize import size


if platform.system() == 'Linux':
    LINUX = True
    DATABASE_PATH = None
    CSV_PATH = '/home/pi/busstats/busstats.csv'
    configure_logging(filename='/home/pi/busstats/busstats.log')

else:
    LINUX = False
    DATABASE_PATH = 'D:/.database/sql/busstats.sqlite'
    CSV_PATH = 'D:/Sistema/Downloads/busstats.csv'
    configure_logging(name='busstats')

SERVER_ADDRESS = 'http://sralloza.sytes.net:5415'


class InvalidPlatformError(Exception):
    """Invalid platform"""


class DataBase:
    """Manages the connection with the database"""

    def __init__(self):

        self.con = None
        self.cur = None

    def use(self, database_path=None):
        """Starts connection with database, if it wasn't connected yet."""
        if LINUX is True:
            raise InvalidPlatformError('Database can only be used in windows')

        if self.con is not None:
            return
        if database_path is None:
            database_path = DATABASE_PATH
        self.con = sqlite3.connect(database_path)

        self.cur = self.con.cursor()
        self.cur.execute("""create table if not exists busstats (
        id varchar primary key,
        line varchar not null,
        actual_datetime varchar not null,
        delay_minutes integer not null,
        stop_id integer not null)""")

    def insert_multiple_registers(self, data):
        """Saves multiple registers at once to the database.

        Args:
            data (Iterable[Register]): iterable containing registers to save to database.
        """
        values = []
        ids = self.get_ids()

        for element in data:
            if element.id not in ids:
                values.append((element.id, element.line, element.actual_datetime,
                               element.delay_minutes, element.stop_id))

        values = tuple(values)

        self.cur.executemany("insert into busstats values(?,?,?,?,?)", values)
        self.con.commit()
        return len(values)

    @staticmethod
    def get_ids():
        """Returns a tuple with all the register's IDs saved in the database."""
        self = DataBase.__new__(DataBase)
        self.__init__()
        self.use()

        self.cur.execute('select id from busstats')
        registers = [x[0] for x in self.cur.fetchall()]
        return tuple(registers)


DB = DataBase()


def get_length_database():
    """Returns the number of registers saved in the database."""
    DB.use()
    DB.cur.execute("select count(id) from busstats")
    total = DB.cur.fetchone()[0]

    return total


@dataclass
class Register:
    """Represents a Bus Stat Register"""
    line: str
    actual_datetime: str
    delay_minutes: int
    stop_id: int

    def __post_init__(self):
        self.line = str(self.line)
        self.actual_datetime = str(self.actual_datetime)
        self.delay_minutes = int(self.delay_minutes)
        self.stop_id = int(self.stop_id)

    @property
    def id(self):
        """Returns the id of a register made with sha1"""
        p = (self.line, self.actual_datetime, self.stop_id)
        return hashlib.sha1(str(p).encode()).hexdigest()


def load_registers() -> list:
    """Returns a tuple with all the registers found in the csv file."""
    try:
        if not LINUX:
            with open(CSV_PATH, 'r', encoding='utf-8') as csv_file:
                number_of_lines = len(csv_file.read().splitlines()) - 2
            print(f'Preliminar scan found {number_of_lines} new registers')

        with open(CSV_PATH, 'r', encoding='utf-8') as csv_file:
            csv_reader = DictReader(csv_file)
            next(csv_reader)

            output = []
            for row in csv_reader:
                output.append(Register(**row))
            return output
    except FileNotFoundError:
        if LINUX is False:
            print(f'File not found: {CSV_PATH!r}')
        return []
    except StopIteration:
        return []


def save_registers(registers):
    """Saves the registers to the csv file."""
    with open(CSV_PATH, 'w', encoding='utf-8') as csv_file:
        fieldnames = ['line', 'actual_datetime', 'delay_minutes', 'stop_id']
        csv_writer = DictWriter(csv_file, fieldnames, quotechar='|', lineterminator='\n')

        csv_writer.writeheader()

        csv_writer.writerows([vars(register) for register in registers])


def analyse_stop(stop_number: int, lines=None):
    """Gets data from a bus stop.

    Args:
        stop_number (int): stop id to get data from.
        lines (int, Iterable): line or lines to get data from.
    """
    if lines is None:
        lines = None
    elif isinstance(lines, int):
        lines = (str(lines),)
    elif isinstance(lines, str):
        lines = (lines,)
    else:
        lines = tuple([str(x) for x in lines])

    d = Downloader(silenced=True)
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
            register = Register(t[0], datetime.today().strftime('%Y-%m-%d %H:%M:%S'), int(t[-1]),
                                stop_number)
        except ValueError:
            continue

        if lines is not None:
            if register.line in lines:
                output.append(register)
        else:
            output.append(register)

    return tuple(output)


def generate_data():
    """Gets all the data from some set bus stops."""
    logger = logging.getLogger(__name__)

    try:
        registers = load_registers()
        registers += analyse_stop(stop_number=686, lines=2)  # Gamazo
        registers += analyse_stop(stop_number=682, lines=8)  # Fray luis de león
        registers += analyse_stop(stop_number=812, lines=(2, 8))  # Fuente dorada
        registers += analyse_stop(stop_number=833, lines=(2, 8))  # Clínico
        registers += analyse_stop(stop_number=880, lines=2)  # Donde nos deja el 2 en ciencias
        registers += analyse_stop(stop_number=1191, lines=8)  # Parada anterior a la del campus
        registers += analyse_stop(stop_number=1358, lines=8)  # Campus miguel delibes

        save_registers(registers)
    except Exception as e:
        if LINUX is False:
            raise
        logger.critical(str(e))
        Connections.send_email(
            'sralloza@gmail.com', 'Error en la generación de datos del bus',
            'Se ha producido la siguiente excepción:\n\n\n' + traceback.format_exc())


def to_excel_main():
    """Saves the data from the database to an excel file."""

    if LINUX is True:
        raise InvalidPlatformError('Can not be used in linux, only in Windows')

    from pandas import read_sql, ExcelWriter

    DB.use()
    data_frame = read_sql('select linea,ta,tr,id_parada from busstats order by ta, linea', DB.con)

    print(f'Dimensions: {data_frame.shape}')

    ew = ExcelWriter('busstats.xlsx')
    data_frame.to_excel(ew, index=None)
    try:
        ew.save()
    except PermissionError:
        os.system('taskkill -f -im excel.exe')
        time.sleep(0.2)
        ew.save()


def update_database():
    """Updates the database with the registers found in the csv file."""

    if LINUX:
        raise InvalidPlatformError('Can only be used in windows')

    data = load_registers()

    saved_ids = DataBase.get_ids()
    new_ids = [x.id for x in data if x.id not in saved_ids]

    registers_number = len(new_ids)

    print(f'Found {registers_number} new registers')

    DB.use()

    saved = DB.insert_multiple_registers(data)

    return registers_number, saved, True


def main_update_database():
    """Main function."""
    if LINUX is True:
        raise InvalidPlatformError('Database can only be used in Windows')
    from rpi.time_operations import secs_to_str
    t0 = time.time()
    total = 0
    saved = 0
    secure_token = False

    try:
        total, saved, secure_token = update_database()
    except KeyboardInterrupt:
        pass
    finally:
        if saved == 0:
            print(f"No registers have been saved")
        else:
            print(f'Saved {saved} registers')

        print(f'Executed in {secs_to_str(time.time() - t0)}')

        if total != 0:
            print(f'Mean speed: {total / (time.time() - t0):.2f} registers/s')

        if total == saved and secure_token is True:
            try:
                os.remove(CSV_PATH)
            except FileNotFoundError:
                print(f'File not found: {CSV_PATH!r}')
            print(f'Deleted file {CSV_PATH!r}')
        else:
            print(f'File {CSV_PATH!r} has not been removed (total != saved'
                  f', {total} != {saved}, securetoken={secure_token})')


def get_auto():
    """Using HTTP, gets the csv file from the server and deletes it if the transmission was correct.
    """

    if LINUX is True:
        raise InvalidPlatformError('Database can only be used in Windows')

    def create_token():
        print('Creating token')
        today = datetime.today()
        anything = (today.year, today.month, today.day)

        token = encrypt(repr(anything))
        return token

    downloader = Downloader()
    file_request = downloader.get(SERVER_ADDRESS)

    print(f'status code: {file_request.status_code!r}')

    if file_request.status_code == 200:
        print(f'Saving file ({size(len(file_request.content))})')
        # noinspection PyBroadException
        try:
            with open(CSV_PATH, 'wb') as f:
                f.write(file_request.content)
        except Exception as e:
            print(f'Exception caught ({e.__class__.__name__}): {e}')
        else:
            print('Requesting delete')
            delete_request = requests.delete(SERVER_ADDRESS, data={'token': create_token()})
            print(f'delete result: {delete_request.status_code}')
    else:
        temp_pat = re.compile(r'(?:<p>)(?:Error code explanation:)(.+)(?:</p>)')
        print(f'Error getting file: {temp_pat.search(file_request.text).group(1)}')


def bus_stats_interface():
    if len(sys.argv) == 1 and LINUX is True:
        sys.argv.append('-generate')

    parser = argparse.ArgumentParser(prog='BusStats')
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('-generate', action='store_true')
    group.add_argument('-update', action='store_true')
    group.add_argument('-registers', '-number', action='store_true')
    group.add_argument('-toexcel', '-excel', action='store_true')
    group.add_argument('-get', action='store_true')
    group.add_argument('-all', action='store_true', help='union of -get and -update')

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
    elif opt['get'] is True:
        get_auto()
        exit()
    elif opt['registers'] is True:
        print(f'{get_length_database()} registers saved in database')
        exit()
    elif opt['all'] is True:
        get_auto()
        main_update_database()
        exit()


if __name__ == '__main__':
    bus_stats_interface()
