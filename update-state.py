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

#conexion a la base de datos destino Base replica
cnxDestino = mysql.connector.connect(user=MYSQL_DESTINO_USUARIO, password=MYSQL_DESTINO_PASSWORD,
                              host=MYSQL_DESTINO_HOST,
                              database=MYSQL_DESTINO_BASE, port=MYSQL_DESTINO_PUERTO)

#funcion para actualizar estado
def actualizar():
  try:
    print(f'{datetime.now()}  Inicio Actualización estado')
    #Consulta base de datos replica
    queryDestinoNoAutorizados = ("select * from docout where stus != 'A'")
    cursorDestinoNoAutorizados = cnxDestino.cursor()
    cursorDestinoNoAutorizados.execute(queryDestinoNoAutorizados)

    
    

    #cursos consulta destino
    cursorOrigenData = cnxOrigen.cursor()
    contador=0
    for (empr, tcbt, nloc, ncbt, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _) in cursorDestinoNoAutorizados:
      # print(empr, tcbt, nloc, ncbt)
      queryOrigenData = ("select * from docout where empr=%s AND tcbt=%s AND nloc=%s AND ncbt=%s")
      cursorOrigenData.execute(queryOrigenData, (empr, tcbt, nloc, ncbt))
      records = cursorOrigenData.fetchall()
      for(empr, tcbt, nloc, ncbt, tdoc, pfsri, ndoc, autr, faut, femi, clac, toff, stus, stusw, vtot, xml, clie, email, obse, piva, ulactd) in records:
        if(stus == 'A'):
          contador += 1
          cursorDestino = cnxDestino.cursor()
          updateDestino = ("UPDATE docout SET  autr=%s, faut=%s, femi=%s, clac=%s, toff=%s, stus=%s, stusw=%s, vtot=%s, xml=%s, clie=%s, email=%s, obse=%s, piva=%s, ulactd=%s) "
                  "WHERE empr=%s AND tcbt=%s AND nloc=%s AND ncbt=%s")
          cursorDestino.execute(updateDestino,(empr, tcbt, nloc, ncbt, tdoc, pfsri, ndoc, autr, faut, femi, clac, toff, stus, stusw, vtot, xml, clie, email, obse, piva, ulactd))
          cnxDestino.commit()
          urlMail = "http://192.130.1.51:8765/facturas/mail?documento="+tdoc+"-"+pfsri+"-"+str(ndoc)+"&ciruc="+clie
          response = requests.get(urlMail)
          print(urlMail, response)
    # cursorOrigen.close()
    cursorDestinoNoAutorizados.close()
    cursorOrigenData.close()
    
    print(f"{datetime.now()} - Cantidad de registros actualizados {contador}")
  except mysql.connector.Error as err:
    print(err)
    # exit(1)

actualizar()

# cerra conexiones
cnxOrigen.close()
cnxDestino.close()