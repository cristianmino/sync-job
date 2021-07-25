'''
@author Cristian Mino <cristian.mino@icloud.com>
'''
import os
import requests
import mysql.connector
from mysql.connector import errorcode
from dotenv import load_dotenv
from datetime import datetime
import time

load_dotenv()


MYSQL_ORIGEN_HOST= os.getenv('MYSQL_ORIGEN_HOST')
MYSQL_ORIGEN_PUERTO= os.getenv('MYSQL_ORIGEN_PUERTO')
MYSQL_ORIGEN_BASE= os.getenv('MYSQL_ORIGEN_BASE')
MYSQL_ORIGEN_USUARIO= os.getenv('MYSQL_ORIGEN_USUARIO')
MYSQL_ORIGEN_PASSWORD= os.getenv('MYSQL_ORIGEN_PASSWORD')

#conexion a la base de datos origen PRODUCCION
cnxOrigen = mysql.connector.connect(user=MYSQL_ORIGEN_USUARIO, password=MYSQL_ORIGEN_PASSWORD,
                              host=MYSQL_ORIGEN_HOST,
                              database=MYSQL_ORIGEN_BASE, port=MYSQL_ORIGEN_PUERTO)

MYSQL_DESTINO_HOST= os.getenv('MYSQL_DESTINO_HOST')
MYSQL_DESTINO_PUERTO= os.getenv('MYSQL_DESTINO_PUERTO')
MYSQL_DESTINO_BASE= os.getenv('MYSQL_DESTINO_BASE')
MYSQL_DESTINO_USUARIO= os.getenv('MYSQL_DESTINO_USUARIO')
MYSQL_DESTINO_PASSWORD= os.getenv('MYSQL_DESTINO_PASSWORD')

#conexion a la base de datos destino
cnxDestino = mysql.connector.connect(user=MYSQL_DESTINO_USUARIO, password=MYSQL_DESTINO_PASSWORD,
                              host=MYSQL_DESTINO_HOST,
                              database=MYSQL_DESTINO_BASE, port=MYSQL_DESTINO_PUERTO)

#funcion para sincronizar
def sincronizar(empr, tcbt):
  try:
    print('{} - Empresa: {} - Tipo Comprobante: {} - Inicio'.format(datetime.now(), empr, tcbt))
    queryCount = ("SELECT max(ncbt) maximo FROM docout WHERE nloc = 1 and empr = %s and tcbt = %s")
    cursorCountDestino = cnxDestino.cursor()
    cursorCountDestino.execute(queryCount, (empr,tcbt))
    cantidadDestino = cursorCountDestino.fetchone()

    cursorCountOrigen = cnxOrigen.cursor()
    cursorCountOrigen.execute(queryCount, (empr,tcbt))
    cantidadOrigen = cursorCountOrigen.fetchone()

    if(cantidadOrigen[0] == None):
      return

    queryOrigen = ("SELECT * FROM docout WHERE nloc = 1 and empr = %s and tcbt = %s and ncbt between %s and %s ORDER BY ncbt")

    insertDestino = ("INSERT INTO docout(empr, tcbt, nloc, ncbt, tdoc, pfsri, ndoc, autr, faut, femi, clac, toff, stus, stusw, vtot, xml, clie, email, obse, piva, ulactd) "
                  "VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")

    cursorDestino = cnxDestino.cursor()
    cursorOrigen = cnxOrigen.cursor()
    cursorOrigen.execute(queryOrigen, (empr,tcbt,cantidadDestino[0] + 1, cantidadOrigen[0]))
    for (empr, tcbt, nloc, ncbt, tdoc, pfsri, ndoc, autr, faut, femi, clac, toff, stus, stusw, vtot, xml, clie, email, obse, piva, ulactd) in cursorOrigen:
      cursorDestino.execute(insertDestino,(empr, tcbt, nloc, ncbt, tdoc, pfsri, ndoc, autr, faut, femi, clac, toff, stus, stusw, vtot, xml, clie, email, obse, piva, ulactd))
      cnxDestino.commit()
      if(stus == 'A' and (tcbt == 'EB' or tcbt == 'IB')):
        urlMail = "http://192.130.1.51:8765/facturas/mail?documento="+tdoc+"-"+pfsri+"-"+str(ndoc)+"&ciruc="+clie
        response = requests.get(urlMail)
        print(urlMail, response.status_code)
    cursorCountOrigen.close()
    cursorCountDestino.close()
    cursorOrigen.close()
    cursorDestino.close()
    cantidad = cantidadOrigen[0] - cantidadDestino[0]
    print(f"{datetime.now()} - Empresa {empr} - Tipo Comprobante {tcbt} - Cantidad: {cantidad}")
  except mysql.connector.Error as err:
    print(err)
    # exit(1)

# soncronizacion de los dos tipos de documentos
sincronizar('DJV', 'EB')
sincronizar('DJV', 'IB')
sincronizar('DJV', 'CS')
sincronizar('DJV', 'GR')
sincronizar('DJV', 'RT')
time.sleep(5)
sincronizar('PML', 'EB')
sincronizar('PML', 'IB')
sincronizar('PML', 'CS')
sincronizar('PML', 'GR')
sincronizar('PML', 'RT')
time.sleep(5)
sincronizar('PRD', 'EB')
sincronizar('PRD', 'IB')
sincronizar('PRD', 'CS')
sincronizar('PRD', 'GR')
sincronizar('PRD', 'RT')



# cerra conexiones
cnxOrigen.close()
cnxDestino.close()