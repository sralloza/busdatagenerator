import datetime
import logging
import time

from rpi.custom_logging import configure_logging

from busdataanalysis import DataManager
from busdatagenerator import DataBase

configure_logging(name='converter')
logger = logging.getLogger('converter')


def convert():
    today = datetime.datetime.today()

    output_db = DataBase()

    output_db.use(database_path=f'{today}.sqlite')

    logger.debug('Loading data')
    dm = DataManager.load()
    logger.debug('Data loaded')

    logger.debug('Filtering data')
    dm.filter_times(datetime.time(8, 0), datetime.time(9, 0))
    logger.debug('Data filtered')

    logger.debug('Inserting data')
    output_db.insert_multiple_registers(dm)
    logger.debug('Data inserted')

    logger.debug('Saving and closing database')
    output_db.con.commit()
    output_db.con.close()
    logger.debug('Database saved and closed')


if __name__ == '__main__':
    t0 = time.time()
    convert()
    logger.debug("Converter's execution time: %.2f", time.time() - t0)
