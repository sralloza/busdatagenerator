import time

from busdataanalysis import GestorDatos
import numpy as np

np.set_printoptions(threshold=np.nan)
id_parada = 686

datos = GestorDatos.cargar()

t0 = time.time()

datos.filtrar(linea=2, id_parada=id_parada)

print(time.time() - t0)

# print(GestorDatos.id_parada_to_str(id_parada))

# a = np.array([x.ta.time().__str__()[:-3] for x in datos])

# print(a)
