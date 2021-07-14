'''
@author Cristian Mino <cristian.mino@icloud.com>
'''
import os
import requests
import mysql.connector
from mysql.connector import errorcode
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()


MYSQL_ORIGEN_HOST= os.getenv('MYSQL_ORIGEN_HOST')
MYSQL_ORIGEN_PUERTO= os.getenv('MYSQL_ORIGEN_PUERTO')
MYSQL_ORIGEN_BASE= os.getenv('MYSQL_ORIGEN_BASE')
MYSQL_ORIGEN_USUARIO= os.getenv('MYSQL_ORIGEN_USUARIO')
MYSQL_ORIGEN_PASSWORD= os.getenv('MYSQL_ORIGEN_PASSWORD')

#conexion a la base de datos origen
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
def sincronizar(tipo_doc):
  try:
    print('{} - Tipo documento: {}'.format(datetime.now(), tipo_doc))
    queryCount = ("SELECT max(ndoc) maximo FROM docout WHERE tdoc = %s and pfsri='001702'")
    cursorCountDestino = cnxDestino.cursor()
    cursorCountDestino.execute(queryCount, (tipo_doc,))
    cantidadDestino = cursorCountDestino.fetchone()

    cursorCountOrigen = cnxOrigen.cursor()
    cursorCountOrigen.execute(queryCount, (tipo_doc,))
    cantidadOrigen = cursorCountOrigen.fetchone()

    queryOrigen = ("SELECT * FROM docout WHERE tdoc = %s and pfsri='001702' and ndoc between %s and %s ORDER BY ndoc")

    insertDestino = ("INSERT INTO docout(empr, tcbt, nloc, ncbt, tdoc, pfsri, ndoc, autr, faut, femi, clac, toff, stus, stusw, vtot, xml, clie, email, obse, piva, ulactd) "
                  "VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")

    cursorDestino = cnxDestino.cursor()
    cursorOrigen = cnxOrigen.cursor()
    cursorOrigen.execute(queryOrigen, (tipo_doc,cantidadDestino[0] + 1, cantidadOrigen[0]))
    for (empr, tcbt, nloc, ncbt, tdoc, pfsri, ndoc, autr, faut, femi, clac, toff, stus, stusw, vtot, xml, clie, email, obse, piva, ulactd) in cursorOrigen:
      cursorDestino.execute(insertDestino,(empr, tcbt, nloc, ncbt, tdoc, pfsri, ndoc, autr, faut, femi, clac, toff, stus, stusw, vtot, xml, clie, email, obse, piva, ulactd))
      cnxDestino.commit()
      urlMail = "http://192.130.1.51:8765/facturas/mail?documento="+tdoc+"-"+pfsri+"-"+str(ndoc)+"&ciruc="+clie
      print(urlMail)
      response = requests.get(urlMail)
      print(response)
    cursorCountOrigen.close()
    cursorCountDestino.close()
    cursorOrigen.close()
    cursorDestino.close()
    cantidad = cantidadOrigen[0] - cantidadDestino[0]
    print("{} - Cantidad de registros ingresados: {}, tipo: {}".format(datetime.now(), cantidad, tipo_doc))
  except mysql.connector.Error as err:
    print(err)
    exit(1)

# soncronizacion de los dos tipos de documentos
sincronizar('FT')
sincronizar('NC')

# cerra conexiones
cnxOrigen.close()
cnxDestino.close()