import datetime
import logging
import re
import time
from typing import Iterable, List

from bs4 import BeautifulSoup as Soup
from dataclasses import dataclass, field
from rpi.custom_logging import configure_logging
from rpi.downloader import Downloader

from busdatagenerator import DataBase, Register

configure_logging(name='analyser')
logger = logging.getLogger(__name__)


class InvalidStopIdError(Exception):
    """Provided an invalid stop id."""


@dataclass
class UltimateRegister(Register):
    actual_datetime: datetime.datetime
    actual_date: datetime.date = field(init=False)
    actual_time: datetime.time = field(init=False)

    arrival_datetime: datetime.datetime = field(init=False)
    arrival_date: datetime.date = field(init=False)
    arrival_time: datetime.time = field(init=False)

    def __post_init__(self):
        self.actual_datetime = datetime.datetime.strptime(self.actual_datetime, '%Y-%m-%d %H:%M:%S')
        self.actual_date = self.actual_datetime.date()
        self.actual_time = self.actual_datetime.time()

        self.arrival_datetime = self.actual_datetime + datetime.timedelta(
            seconds=self.delay_minutes * 60)

        if self.delay_minutes == 999:
            self.arrival_datetime = datetime.datetime.max

        self.arrival_date = self.arrival_datetime.date()
        self.arrival_time = self.arrival_datetime.time()


class DataManager(list):
    def __str__(self):
        return '\n'.join([repr(x) for x in self])

    @staticmethod
    def stop_id_to_str(id_parada):
        d = Downloader()
        r = d.get('http://www.auvasa.es/parada.asp?codigo=' + str(id_parada))
        s = Soup(r.content, 'html.parser')

        if 'No hay información de líneas en servicio para la parada indicada' in r.text:
            raise InvalidStopIdError

        c = s.findAll('h5')

        return re.search(r'[\w\s]+', c[1].text.strip()).group().strip()

    @classmethod
    def load(cls, n=None):
        self = DataManager.__new__(cls)
        self.__init__()
        self.database = DataBase()
        self.database.use()

        if n is not None:
            data = self.database.cur.execute(f"select * from busstats limit {n}")
        else:
            data = self.database.cur.execute(f"select * from busstats")

        for d in data:
            self.append(UltimateRegister(*d[1:]))

        self.sort(key=lambda k: k.actual_datetime)

        return self

    def filter_lines(self, lines: Iterable[int]):
        o = []
        for register in self:
            if register.line in lines:
                o.append(register)

        self.__init__(o)

    def filter_stops(self, stops: Iterable[int]):
        o = []
        for register in self:
            if register.stop_id in stops:
                o.append(register)

        self.__init__(o)

    def filter_times(self, time1: datetime.time, time2: datetime.time):
        o = []
        for register in self:
            if time1 <= register.actual_time <= time2:
                o.append(register)

        self.__init__(o)


def analyse():
    data: List[UltimateRegister] = DataManager.load()

    output = {}
    epsilon = datetime.timedelta(seconds=5 * 60)

    for register in data:
        key = (register.line, register.arrival_date)
        if key not in output:
            output[key] = register.arrival_datetime
        else:
            if abs(output[key] - register.arrival_datetime) < epsilon:
                logger.debug('merging \'%s\' and \'%s\'', output[key], register.arrival_datetime)
                output[key] = max((output[key], register.arrival_datetime))
                logger.debug('merged: \'%s\'', output[key])

    dates = {date for (line, date) in output.keys()}

    bus_catched = 0
    bus_missed = 0
    indetermined = 0

    for date in dates:
        try:
            time_for_2 = output[('2', date)]
        except KeyError:
            logger.error('Missing data for line 2 of date %s', date)
            continue

        try:
            time_for_8 = output[('8', date)]
        except KeyError:
            logger.error('Missing data for line 8 of date %s', date)
            continue

        if time_for_2 > time_for_8:
            logger.debug('Missed bus  (%s)', date)
            bus_missed += 1
        elif time_for_2 == time_for_8:
            logger.debug('Indetermined (%s)', date)
            indetermined += 1
        else:
            logger.debug('Bus catched (%s)', date)
            bus_catched += 1

    prob = (bus_catched + indetermined * 0.5) * 100 / (bus_catched + bus_missed + indetermined)

    print(f'Catched:      {bus_catched}')
    print(f'Missed:       {bus_missed}')
    print(f'Indetermined: {indetermined}')
    print('-------------------------------')
    print(f'Probability:  {prob:.2f} %')


if __name__ == '__main__':
    t0 = time.time()
    analyse()
    logger.debug('Analysis execution time: %.2f', time.time() - t0)
