"""Module made to analyse bus statistics.

Todo:
    * Fix to_excel function

"""
import datetime
import logging
import os
import re
import time
from typing import Iterable, Dict, Callable

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
    # noinspection PyUnresolvedReferences
    """Representation of a busstats register with more information.

        Notes:
            This class only works for registers with delay_minutes=0.

        Args:
            line (str): bus line identifier.
            delay_minutes (int): minutes that will take the bus to arrive at the stop
            stop_id (int): identifier of the bus stop.
            actual_datetime (datetime.datetime): The datetime when the register was created. As
                delay_minutes is always 0, it is supposed to coincide with the datetime when the bus
                arrives at the stop.
            date (datetime.date): date when the register was created
            time (datetime.time): time when the register was created, with seconds=0.
            key (float): total minutes of the time, used as key to sort and calculate time
                differences.

        """
    stop_id: int = field(repr=False)
    delay_minutes: int = field(repr=False)
    actual_datetime: datetime.datetime = field(repr=False)
    date: datetime.date = field(init=False, repr=False)
    time: datetime.time = field(init=False, repr=False)

    key: float = field(init=False, repr=False)

    def __lt__(self, other):
        assert isinstance(other, UltimateRegister)
        return self.key < other.key

    def __repr__(self):
        return f"UltimateRegister(line={self.line!r}, date='{self.date}', time='{self.time}')"

    def __post_init__(self):
        self.actual_datetime = datetime.datetime.strptime(self.actual_datetime, '%Y-%m-%d %H:%M:%S')
        self.actual_datetime = self.actual_datetime.replace(second=0)
        self.date = self.actual_datetime.date()
        self.time = self.actual_datetime.time()

        assert self.delay_minutes == 0

        self.key = self.time.hour * 60 + self.time.minute

    def distance(self, other):
        """Calculates distances between two registers, self and other.

        Args:
            other (UltimateRegister): the other register.

        Returns:
            float: distance between registers.

        """
        assert isinstance(other, UltimateRegister)

        return abs(self.key - other.key)


class DataManager(list):
    def __str__(self):
        return '\n'.join([repr(x) for x in self])

    def __add__(self, other):
        assert isinstance(other, DataManager), ValueError
        return DataManager(list(self) + list(other))

    @staticmethod
    def stop_id_to_str(stop_id: int) -> str:
        """Returns the information of a bus stop given its id.

        Args:
            stop_id: identification of the bus stop

        Returns:
            str: information of the bus stop.

        """
        d = Downloader()
        r = d.get(f'http://www.auvasa.es/parada.asp?codigo={stop_id}')
        s = Soup(r.content, 'html.parser')

        if 'No hay información de líneas en servicio para la parada indicada' in r.text:
            raise InvalidStopIdError('No info found!')

        c = s.findAll('h5')

        return re.search(r'[\w\s]+', c[1].text.strip()).group().strip()

    @classmethod
    def load(cls, n: int = None):
        """Loads the manager with data from the database.

        Args:
            n: number of registers to get from the database (if it is None, all registers will be
                used).

        """
        self = DataManager.__new__(cls)
        self.__init__()
        self.database = DataBase()
        self.database.use()

        if n is not None:
            data = self.database.cur.execute(
                f"select * from busstats limit {n} where delay_minutes=0")
        else:
            data = self.database.cur.execute("select * from busstats where delay_minutes=0")

        for d in data:
            self.append(UltimateRegister(*d[1:]))

        self.sort(key=lambda k: k.actual_datetime)

        return self

    def filter_lines(self, lines: Iterable[int]):
        """Deletes all registries in the managers except the ones its line is on the iterable.

        Args:
            lines (Iterable[int]): set of lines to keep.
        """
        o = []
        for register in self:
            if register.line in lines:
                o.append(register)

        self.__init__(o)

    def filter_stops(self, stops: Iterable[int]):
        """Deletes all registries in the managers except the ones its stop_id is on the iterable.

        Args:
            stops (Iterable[int]): set of stops identification to keep.
        """
        o = []
        for register in self:
            if register.stop_id in stops:
                o.append(register)

        self.__init__(o)

    def filter_times(self, time1: datetime.time, time2: datetime.time):
        """Deletes every register if its time is not between time1 and time2.

        Args:
            time1 (datetime.date): lower filter.
            time2 (datetime.date): upper filter.
        """
        o = []
        for register in self:
            if time1 <= register.time <= time2:
                o.append(register)

        self.__init__(o)

    def group(self, line, stop_id: int = 833, epsilon: int = 2, selector: Callable = max):
        """Groups the registers according to its time. Due to grouping problems, it will also
        filter registers by line and stop_id. The default stop_id is 833, which is the id of the
        hospital's bus stop.

        Args:
            line (str | int): bus line to filter.
            stop_id (int): stop_id to filter.
            epsilon (int): maximum time difference between registers of the same group.
            selector (Callable): funtion that selects which of the registers in the group will
                remain.

        Returns:
            DataManager
        """
        line = str(line)

        def func(x):
            return x.line == line and x.stop_id == stop_id and \
                   x.actual_datetime != datetime.datetime.max

        self.__init__(list(filter(func, self)))
        self.sort(key=lambda x: (x.date, x.key))

        groups = []
        temp_group = []
        i = 0

        while i < len(self):
            k = i
            while self[i].distance(self[k]) < epsilon:
                temp_group.append(self[k])
                k += 1
                if k == len(self):
                    break

            i = k
            logger.debug('Created group (len=%d): %r', len(temp_group), temp_group)
            groups.append(temp_group)
            temp_group = []

        self.__init__([selector(x) for x in groups])
        self.sort(key=lambda x: (x.date, x.key))
        message = f'After grouping (line={line!r}, stop_id={stop_id!r}, e={epsilon!r})'
        print()
        print(f'{message:-^62s}')
        print(self)
        print('-' * 62)

    def compare(self, time1=datetime.time(8, 35), time2=datetime.time(8, 50)):
        """Calculates the probability of the line 2 coming to a stop before line 8. Filters data
        from time1 to time2.

        Args:
            time1 (datetime.time): lower filter.
            time2 (datetime.time): upper filter.

        """
        self.filter_times(time1, time2)

        message = f'After filtering ({time1} -> {time2})'
        print()
        print(f'{message:-^62s}')
        print(self)
        print('-' * 62)

        comparation = {}
        for register in self:
            try:
                actual = comparation[(register.line, register.date)]
                new = max([actual, register.time])
                logger.warning('Value already exists for %s (%s): %s -> %s', register.date,
                               register.line,
                               actual, new)
                comparation[(register.line, register.date)] = new
            except KeyError:
                comparation[(register.line, register.date)] = register.time

        dates = {date for (line, date) in comparation.keys()}

        bus_catched = 0
        bus_missed = 0
        indetermined = 0

        for date in dates:
            try:
                time_for_2 = comparation[('2', date)]
            except KeyError:
                logger.error('Missing data for line 2 of date %s', date)
                continue

            try:
                time_for_8 = comparation[('8', date)]
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

        to_excel(comparation)


def to_excel(data: Dict[tuple, datetime.datetime]):
    """Saves the data from the database to an excel file.

    Notes:
        Todo: fix function and transform it into a method

    Args:
        data:
    """

    from pandas import ExcelWriter, DataFrame

    @dataclass
    class DataFrameInterface:
        line: int
        datetime: datetime.datetime
        date: datetime.date = field(init=False)
        time: datetime.time = field(init=False)

        hours: float = field(init=False)

        def __post_init__(self):
            self.line = int(self.line)
            self.date = self.datetime.date()
            self.time = self.datetime.time()

            seconds = self.time.hour * 3600 + self.time.minute * 60 + self.time.second

            self.hours = datetime.timedelta(seconds=seconds).total_seconds() / 3600

    _time: datetime.time
    interfaces = (DataFrameInterface(line, datetime.datetime.combine(date, _time)) for
                  (line, date), _time in data.items())

    message = f'Interfaces'
    print()
    print(f'{message:-^62s}')
    print('\n'.join([str(x) for x in interfaces]))
    print('-' * 62)

    # data_frame1 = DataFrame(columns=['date', 'time_2', 'time_8'])
    # data_frame1.set_index(['date'], inplace=True)
    data_frame2 = DataFrame(columns=['date', 'time_2', 'time_8'])

    data_frame2.set_index(['date'], inplace=True)

    for interface in interfaces:
        if interface.line == 8:
            # data_frame1.loc[interface.date, 'time_8'] = interface.hours
            try:
                print(data_frame2.loc[interface.date, 'time_8'], interface.date, interface.time)
            except KeyError:
                print('skipped', interface.date, interface.time)
            data_frame2.loc[interface.date, 'time_8'] = interface.time
        if interface.line == 2:
            # data_frame1.loc[interface.date, 'time_2'] = interface.hours
            data_frame2.loc[interface.date, 'time_2'] = interface.time

    data_frame2.sort_index(inplace=True)
    data_frame2['result'] = data_frame2['time_2'] < data_frame2['time_8']

    print(data_frame2)
    # ew1 = ExcelWriter('busstats.hours.xlsx')
    ew2 = ExcelWriter('busstats.times.xlsx')
    # data_frame1.to_excel(ew1)
    data_frame2.to_excel(ew2)

    try:
        # ew1.save()
        ew2.save()
    except PermissionError:
        os.system('taskkill -f -im excel.exe > nul')
        time.sleep(0.2)
        # ew1.save()
        ew2.save()


if __name__ == '__main__':
    t0 = time.time()
    dm1 = DataManager.load()
    dm2 = DataManager.load()

    dm1.group(2)
    dm2.group(8)

    dm: DataManager = dm1 + dm2

    dm.compare()
    logger.debug('Analysis execution time: %.2f', time.time() - t0)
