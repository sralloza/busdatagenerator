import datetime
import logging
import os
import re
import time
from typing import Iterable, List, Dict

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
    """Representation of a busstats register with more information.

    A normal register only handles the data extracted from the generator. Instead, the
    UltimateRegister handles every possible data that can be calculated from the bare data, as the
    arrivals times.

    """
    delay_minutes: int = field(repr=False)
    actual_datetime: datetime.datetime = field(repr=False)
    actual_date: datetime.date = field(init=False, repr=False)
    actual_time: datetime.time = field(init=False, repr=False)

    arrival_datetime: datetime.datetime = field(init=False, repr=False)
    arrival_date: datetime.date = field(init=False)
    arrival_time: datetime.time = field(init=False)
    key: float = field(init=False, repr=False)

    def __lt__(self, other):
        assert isinstance(other, UltimateRegister)
        return self.key < other.key

    def __post_init__(self):
        self.actual_datetime = datetime.datetime.strptime(self.actual_datetime, '%Y-%m-%d %H:%M:%S')
        self.actual_datetime = self.actual_datetime.replace(second=0)
        self.actual_date = self.actual_datetime.date()
        self.actual_time = self.actual_datetime.time()

        self.arrival_datetime = self.actual_datetime + datetime.timedelta(
            seconds=self.delay_minutes * 60)

        if self.delay_minutes == 999:
            self.arrival_datetime = datetime.datetime.max

        self.arrival_date = self.arrival_datetime.date()
        self.arrival_time = self.arrival_datetime.time()

        self.key = self.arrival_time.hour * 60 + self.arrival_time.minute

    def distance(self, other):
        assert isinstance(other, UltimateRegister)

        return abs(self.key - other.key)


class DataManager(list):
    def __str__(self):
        return '\n'.join([repr(x) for x in self])

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
            data = self.database.cur.execute(f"select * from busstats limit {n}")
        else:
            data = self.database.cur.execute("select * from busstats")

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

    @staticmethod
    def group(line, stop_id=833, epsilon=5, selector=max):
        line = str(line)
        data: List[UltimateRegister] = DataManager.load()

        # output = {}

        # Old algorithm
        # for register in data:
        #     if register.arrival_datetime == datetime.datetime.max:
        #         logger.warning('Detected max datetime')
        #         continue
        #     key = (register.line, register.arrival_date)
        #     if key not in output:
        #         output[key] = register.arrival_datetime
        #     else:
        #         if abs(output[key] - register.arrival_datetime) < epsilon:
        #             logger.debug('merging \'%s\' and \'%s\'', output[key], register.arrival_datetime)
        #             output[key] = max((output[key], register.arrival_datetime))

        data = DataManager(
            filter(lambda x: x.line == line and x.stop_id == stop_id and \
                             x.arrival_datetime != datetime.datetime.max, data)
        )
        data.sort(key=lambda x: x.key)

        groups = []
        temp_group = []
        i = 0

        while i < len(data):
            k = i
            while data[i].distance(data[k]) < epsilon:
                temp_group.append(data[k])
                k += 1
                if k == len(data):
                    break

            i = k
            logger.debug('Created group (len=%d): %r', len(temp_group), temp_group)
            groups.append(temp_group)
            temp_group = []

        data.__init__([selector(x) for x in groups])
        print(data)

        # dates = {date for (line, date) in output.keys()}
        #
        # bus_catched = 0
        # bus_missed = 0
        # indetermined = 0
        #
        # for date in dates:
        #     try:
        #         time_for_2 = output[('2', date)]
        #     except KeyError:
        #         logger.error('Missing data for line 2 of date %s', date)
        #         continue
        #
        #     try:
        #         time_for_8 = output[('8', date)]
        #     except KeyError:
        #         logger.error('Missing data for line 8 of date %s', date)
        #         continue
        #
        #     if time_for_2 > time_for_8:
        #         logger.debug('Missed bus  (%s)', date)
        #         bus_missed += 1
        #     elif time_for_2 == time_for_8:
        #         logger.debug('Indetermined (%s)', date)
        #         indetermined += 1
        #     else:
        #         logger.debug('Bus catched (%s)', date)
        #         bus_catched += 1
        #
        # prob = (bus_catched + indetermined * 0.5) * 100 / (bus_catched + bus_missed + indetermined)
        #
        # print(f'Catched:      {bus_catched}')
        # print(f'Missed:       {bus_missed}')
        # print(f'Indetermined: {indetermined}')
        # print('-------------------------------')
        # print(f'Probability:  {prob:.2f} %')
        #
        # to_excel(output)

        return data


def to_excel(data: Dict[tuple, datetime.datetime]):
    """Saves the data from the database to an excel file."""

    from pandas import ExcelWriter, DataFrame

    @dataclass
    class DataFrameInterface:
        line: int
        datetime: datetime.datetime
        date: datetime.date = field(init=False)
        time: datetime.time = field(init=False)

        hours: float = field(init=False)

        def __post_init__(self):
            datetime.datetime.today().date()

            self.line = int(self.line)
            self.date = self.datetime.date()
            self.time = self.datetime.time()

            seconds = self.time.hour * 3600 + self.time.minute * 60 + self.time.second

            self.hours = datetime.timedelta(seconds=seconds).total_seconds() / 3600

    interfaces = (DataFrameInterface(line, dt) for (line, date), dt in data.items())
    data_frame1 = DataFrame(columns=['date', 'time_2', 'time_8'])
    data_frame1.set_index(['date'], inplace=True)
    data_frame2 = DataFrame(columns=['date', 'time_2', 'time_8'])

    data_frame2.set_index(['date'], inplace=True)

    for interface in interfaces:
        if interface.line == 8:
            data_frame1.loc[interface.date, 'time_8'] = interface.hours
            data_frame2.loc[interface.date, 'time_8'] = interface.time
        if interface.line == 2:
            data_frame1.loc[interface.date, 'time_2'] = interface.hours
            data_frame2.loc[interface.date, 'time_2'] = interface.time

    print(data_frame2)
    ew1 = ExcelWriter('busstats.hours.xlsx')
    ew2 = ExcelWriter('busstats.times.xlsx')
    data_frame1.to_excel(ew1)
    data_frame2.to_excel(ew2)

    try:
        ew1.save()
        ew2.save()
    except PermissionError:
        os.system('taskkill -f -im excel.exe > nul')
        time.sleep(0.2)
        ew1.save()
        ew2.save()


if __name__ == '__main__':
    t0 = time.time()
    gc1 = DataManager.group(2, )
    logger.debug('Analysis execution time: %.2f', time.time() - t0)
