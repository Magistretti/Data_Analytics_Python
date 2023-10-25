import mysql.connector
from mysql.connector import Error
import pyodbc

import logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        , level=logging.INFO
)
logger = logging.getLogger(__name__)


def conectorMySQL(datos):
    """
    This function will create a connection to a MySQL Server, should be 
    provided with a list with the following strings in this exact order:
        datos=["host","port","database","user","password"]
    """
    try:
        conexMySQL = mysql.connector.connect(
            host=datos[0]
            ,port=datos[1]
            ,database=datos[2]
            ,user=datos[3]
            ,password=datos[4]
        )
        if conexMySQL.is_connected():
            db_Info = conexMySQL.get_server_info()
            logger.info("\nConnected to MySQL Server version " + db_Info)
            cursor = conexMySQL.cursor()
            cursor.execute("select database();")
            record = cursor.fetchone()
            logger.info("You're connected to database: " + record[0])
            cursor.close()
            return conexMySQL
            
    except Error as e:
        logger.error("\nError while connecting to MySQL\n" + e, exc_info=1)
            

def conectorMSSQL(datos):
    """
    This function will create a connection to a Microsoft SQL Server, should 
    be provided with a list with the following strings in this exact order:
        datos=["server","database","username","password"]
    """
    try:
        conexMSSQL = pyodbc.connect(
            "DRIVER={ODBC Driver 17 for SQL Server};\
            SERVER="+datos[0]+";\
            DATABASE="+datos[1]+";\
            UID="+datos[2]+";\
            PWD="+ datos[3]
        )
        logger.info(
            "\nConnected to server: "
            + datos[0]
            + "\nDatabase: " 
            + datos[1]
        )
        
        return conexMSSQL

    except Exception as e:
        listaErrores = e.args[1].split(".")
        logger.error("\nOcurrió un error al conectar a SQL Server: ")
        for i in listaErrores:
            logger.error(i)
        