import json
import time

from rpi.tiempo import segs_to_str

from busdatagenerator import Dato, JSON_PATH, DataBase

AVERAGE_SPEED = 12
if __name__ == '__main__':
    t0 = time.time()
    with open(JSON_PATH) as fh:
        data = json.load(fh)

    data = [Dato(**x) for x in data]

    ids_guardadas = DataBase.get_ids()
    ids_nuevas = tuple([x.id for x in data if x.id not in ids_guardadas])

    total = len(ids_nuevas)

    print(f'Encontrados {total} registros nuevos')
    print(f'Tiempo estimado: {segs_to_str(total / AVERAGE_SPEED)}')

    i = 1
    guardado = 0

    if total != 0:
        for d in data:
            if d.id not in ids_nuevas:
                continue
            o = d.to_database(quiet=True)
            if o is True:
                guardado += 1
            print(f'\r{i * 100 / total:.2f}% completado', end='')
            # print('\r%.2f %% completado' % (i * 100 / total, ), end='')
            i += 1
            # print(d.id)
        print()

    print(f'Ejecutado en {segs_to_str(time.time() - t0)}')

    if total == 0:
        print(f"No se han guardado registros")
    else:
        print(f'Guardados {guardado} registros')
        print(f'Velocidad media: {guardado / (time.time() - t0):.2f} registros/s')
