import json
import time

from busdatagenerator import Dato, JSON_PATH

if __name__ == '__main__':
    t0 = time.time()
    with open(JSON_PATH) as fh:
        data = json.load(fh)

    data = [Dato(**x) for x in data]
    total = len(data)
    print(f'Hay {total} registros')

    i = 1
    for d in data:
        d.to_database(quiet=True)
        print(f'\r{i * 100 / total:.2f}% completado', end='')
        # print('\r%.2f %% completado' % (i * 100 / total, ), end='')
        i += 1
        # print(d.id)
    print()

    print(f'Ejecutado en {time.time() - t0:.2f} segundos')
