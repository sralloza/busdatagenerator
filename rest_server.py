import logging
import os
import platform
import socketserver
import time
from datetime import datetime
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import unquote

from cryptography.fernet import InvalidToken
from rpi.encryption import decrypt


def get_logger(name='rest_server'):
    logger = logging.getLogger(name)

    if not logger.hasHandlers():
        handler = logging.FileHandler(filename=LOG_PATH, encoding='utf-8')
        handler.setLevel(logging.DEBUG)
        handler.setFormatter(logging.Formatter('[%(asctime)s] %(levelname)-8s: %(message)s'))
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)

    return logger


if platform.system() == 'Linux':
    LOG_PATH = '/home/pi/busstats/rest_server.log'
    SOURCE_CSV = '/home/pi/busstats/busstats.csv'
    OUTPUT_CSV = '/home/pi/busstats/busstats.test.csv'
    SERVER_ADDRESS = 'http://sralloza.sytes.net:5415'
else:
    LOG_PATH = 'rest_server.log'
    SOURCE_CSV = 'D:/Sistema/Downloads/busstats.csv'
    OUTPUT_CSV = 'D:/Sistema/Downloads/busstats.test.csv'
    SERVER_ADDRESS = 'http://localhost:5415'


def get_bus_data(get=False, delete=False):
    logger = get_logger()

    if get and delete:
        raise ValueError('Only one option, get or delete')

    if get:
        if os.path.isfile(SOURCE_CSV) is False:
            logger.debug(f'File {SOURCE_CSV!r} does not exist')
            raise FileNotFoundError(f'File {SOURCE_CSV!r} does not exist')

    i = 0

    if get:
        logger.debug('Extracting bus data from path=%r', SOURCE_CSV)

        with open(SOURCE_CSV, 'rb') as fh:
            content = fh.read()

        return content

    if delete:
        seconds = datetime.today().second
        while not (10 <= seconds <= 45):
            if i == 0:
                estimation = 10 - seconds
                while estimation < 0:
                    estimation += 60
                logger.debug(
                    'Waiting for seconds in range (10 - 45) to delete file (estimated %r seconds)',
                    estimation)
            time.sleep(0.5)
            seconds = datetime.today().second
            i += 1

        logger.debug('Ready to delete')
        try:
            os.remove(SOURCE_CSV)
            logger.debug('File deleted')
            return True
        except FileNotFoundError:
            logger.debug('File %r deleted (file not found)', SOURCE_CSV)
            return False


# noinspection PyPep8Naming,PyProtectedMember
class MyServer(BaseHTTPRequestHandler):
    wfile: socketserver._SocketWriter
    logger = get_logger()

    def __init__(self, request, client_address, server):

        super().__init__(request, client_address, server)

    def parse_post_data(self):
        data_string = self.rfile.read(int(self.headers['Content-Length'])).decode()

        post = {}
        for pair in data_string.split('&'):
            key, value = pair.split('=')
            post[key] = unquote(value)

        return post

    def log_message(self, _format, *args):
        self.logger.debug('%s - - %s', self.address_string(), _format % args)

    def log_error(self, _format, *args):
        self.logger.critical('%s - - %s', self.address_string(), _format % args)

    def do_GET(self):
        if 'favicon.ico' in self.path:
            return self.favicon()

        if self.path != '/':
            self.send_error(400, 'Invalid URL', 'BUS STATS REST SERVER')

        try:
            content = get_bus_data(get=True)
        except FileNotFoundError:
            self.send_error(code=404, message='File not found.',
                            explain='File has just been deleted, please wait about 2 min')
            return
        except Exception as e:
            self.send_error(code=500, message=f'Exception caught: {e.__class__.__name__}',
                            explain=str(e))
            return

        self.send_response(200)
        self.send_header('Content-type', 'text/plain')
        self.send_header('Content-Disposition', 'attachment; filename="busstats.csv"')
        self.end_headers()

        self.wfile.write(content)

    def send_invalid_token(self):
        self.send_error(403, message='Invalid token')

    def do_DELETE(self):
        post = self.parse_post_data()
        if 'token' not in post:
            self.logger.debug('Missing token')
            self.send_error(403, message='Missing token')
            return

        token = post.get('token')

        try:
            data = decrypt(token)
        except InvalidToken:
            self.logger.critical('Invalid token (decrypting)')
            return self.send_invalid_token()

        try:
            data = eval(data)
        except NameError:
            self.logger.critical('Invalid token (eval)')
            return self.send_invalid_token()

        today = datetime.today()
        if data != (today.year, today.month, today.day):
            self.logger.critical('Invalid token (comparing)')
            return self.send_invalid_token()

        result = get_bus_data(delete=True)

        if result is True:
            self.send_response(200)
        else:
            self.send_response(500)
        self.logger.debug('Result of delete request: %r', result)
        self.send_header('Content-type', 'text/html')
        self.end_headers()
        self.wfile.write(f'Result: {result}'.encode())
        return

    def favicon(self):
        self.send_response(200)
        self.send_header('Content-type', 'image/png')
        self.end_headers()

        with open('favicon.png', 'rb') as fh:
            self.wfile.write(fh.read())

        return


def start_server():
    my_server = HTTPServer(('0.0.0.0', 5415), MyServer)
    try:
        my_server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        my_server.server_close()


if __name__ == '__main__':
    start_server()
