##########################
#
#    RUMAOS_INFO_BOT
#
##########################

import os
import pathlib
import mysql.connector
ubic = str(pathlib.Path(__file__).parent)+"\\"

from DatosTelegram import id_Autorizados, bot_token, testbot_token
from DatosTelegram import testrumaos, rumaos_info, rumaos_info_com, rumaos_Info_Periferia, rumaos_Control_info, rumaos_Margenes,rumaosMasYPF
from DatosTelegram import rumaos_MBC_gerencial,rumaos_MBC_operativo,rumaos_Comer_retail,rumaos_Abastecimiento,rumaos_Grandes_clientes,rumaosCalibracion_Control
from DatosTelegram import rumaos_Informes_Globales,rumaos_Presupuestos,rumaos_Metricas,rumaos_Informes_Financieros, rumaos_Informes_MBC
from DatosTelegram import rumaos_cheques, rumaos_info_CFO
from DatosTelegram import lapuchesky, rumaos_conciliaciones
from DatosTelegram import rumaos_Info_EPresu
from runpy import run_path
import datetime as dt
import pytz
argTime = pytz.timezone('America/Argentina/Salta')

import logging
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram import ChatAction
from telegram.ext import Updater, CommandHandler
from telegram.ext import MessageHandler, Filters 
from telegram.ext import CallbackQueryHandler, CallbackContext
from telegram.ext import Defaults

from functools import wraps

from InfoLubri_y_RedMas.InfoPenetracionRedMas import penetracionRedMas
from InfoLubri_y_RedMas.InfoLubri import ventaLubri

from InfoCheques.ChequesAyer import cheques_ayer

from InfoSemanal.InfoPenetracionRMSemanal import penetracionRMSemanal
from InfoSemanal.InfoRedControl import redControlSemanal
from InfoSemanal.InfoVtasProyGranClient import vtaProyGranClient
from InfoSemanal.InfoPerifericos import perifericoSemanal

from InfoKamel.ArqueosSGFin import arqueos
from InfoKamel.ChequesSaldosSGFin import chequesSaldos
from InfoKamel.DeudaComercial import condicionDeudores
from InfoKamel.BancosSaldos import bancosSaldos
from InfoKamel.UsosSGFin import usos_SGFin
from InfoKamel.ActivosCorrientes import activosCorrientes
from InfoKamel.PasivosCorrientes import pasivosCorrientes
from InfoKamel.BalanceYER import balanceYER



#####//////////////######
# BOT Token selection for testing:
# 0 = RUMAOS_Info_bot
# 1 = RUMAOStest_bot
MODE = 0

if MODE == 1:
    token = testbot_token
    destinatarios = [testrumaos]
    print("\n//////////","USANDO TEST BOT","//////////\n")
else:
    token = bot_token
    destinatarios = [rumaos_info]
######//////////////######


# Function to find complete path to files
def find(name, path, type="file"):
    '''
    Find the path to folders or files.

    Args:
    name: name of file or folder
    path: starting path of search
     type: "file" or "dir" (default: "file")

    Return a string of the complete path to folder or file
    '''
    for root, dirs, files in os.walk(path):
        if type == "file":
            if name in files:
                return os.path.join(root, name)
        elif type == "dir":
            if name in dirs:
                return os.path.join(root, name)
        else:
            raise ValueError("'type' must be 'file' or 'dir'")


######//////////////######
# Where to find the report image files of:
# "Info_Morosos.png", "Info_VolumenVentas.png", etc
filePath_InfoVtaComb = find("InfoVtaCombustibles", ubic, "dir") + "\\"
filePath_InfoGrandesDeudas = find("InfoDeuda", ubic, "dir") + "\\"
filePath_Info_Despachos_Camioneros = \
    find("DespachosCamionerosRedmas", ubic, "dir") + "\\"
filePath_Info_Presu_Gas = find("PRESUPUESTO",ubic, "dir") + "\\"
filePath_Info_Pen_Pan= find("PenetracionPanaderia",ubic,"dir")+ "\\"
filePath_Info_Pen_Lubri= find("Penetracion Lubricantes",ubic,"dir")+ "\\"
filePath_Info_Pen_Salon= find("Penetracion Salon",ubic,"dir")+ "\\"
filePath_Info_Control_Info= find("Control",ubic,"dir")+ "\\"
filePath_Info_Margenes = find("MARGEN", ubic, "dir")+ "\\"
filePath_Info_MargenesGasoleos = find("Margen_Playa", ubic, "dir")+ "\\"
filePath_Info_Descargas = find("Informe descargas y volumenes", ubic, "dir")+ "\\"
filePath_Info_RH = find("RH", ubic, "dir")+ "\\"
filePath_Pen_APP_YPF = find("PenetracionAppYPF", ubic, "dir")+ "\\"
filePath_Pen_RED_PAGO = find("Red Pago", ubic, "dir")+ "\\"
filePath_KAMEL = find("InfoKamel", ubic, "dir")+ "\\"
filePath_YPF = find("MasYPF", ubic, "dir")+ "\\"
filePath_RedMas = find("InfoLubri_y_RedMas", ubic, "dir")+ "\\"
filePath_InfoSemanal = find("InfoSemanal", ubic, "dir")+ "\\"
filePath_Eduardo = find("DB_Eduardo", ubic, "dir")+ "\\"
filePath_SHEET =  find("DB_Sheet", ubic, "dir")+ "\\"

######//////////////######


##################################################
# LOGGING MESSAGES
##################################################

# logging.basicConfig(
#     format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
#         , level=logging.INFO
# )
# logger = logging.getLogger(__name__)

import logging.handlers
FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
DATEFMT = "%Y-%m-%d %H:%M:%S"

if not os.path.exists(ubic + "log"):
    os.mkdir(ubic + "log")

# set up logging to file
logging.basicConfig(level=logging.INFO,
                    format=FORMAT,
                    datefmt=DATEFMT)
# define a Handler which writes INFO messages to a log file
filelog = logging.handlers.TimedRotatingFileHandler(
    ubic + "log\\bot_activity.log"
    , when="midnight"
    , backupCount=5
)
filelog.setLevel(logging.INFO)
# set a format
formatter = logging.Formatter(fmt=FORMAT, datefmt=DATEFMT)
# tell the handler to use this format
filelog.setFormatter(formatter)
# add the handler to the root logger
logging.getLogger("").addHandler(filelog)
logger = logging.getLogger(__name__) # Add modules names when executed


# Define a filter to avoid cluttering due to "KeepAlive" job
class noKeepAlive(logging.Filter):
    def filter(self, record):
        # return not record.getMessage().startwith("Added")
        if "keepAlive" in record.getMessage() \
                and record.levelname == "INFO" \
                and record.name == "apscheduler.executors.default":
            return False
        else:
            return True

# Apply filter to handlers in root to avoid modules loggers bypass
for handler in logging.root.handlers:
    handler.addFilter(noKeepAlive())



##################################################
# FUNCTION DECORATORS
##################################################

def restricted(func):
    """
        This will use a wrapper to create a decorator "@restricted", this 
        decorator can be placed prior to every function that needs to be 
        locked by user ID
    """
    @wraps(func)
    def wrapped(update, context, *args, **kwargs):
        user_id = update.effective_user.id
        if user_id not in id_Autorizados:
            print("Unauthorized user, access denied for ID {}".format(user_id))
            update.message.reply_text("USUARIO NO AUTORIZADO")
            return
        return func(update, context, *args, **kwargs)
    return wrapped


def developerOnly(func):
    """
        This decorator will grant function access only to the developer
    """
    @wraps(func)
    def wrapped(update, context, *args, **kwargs):
        user_id = update.effective_user.id
        if user_id not in [id_Autorizados[0]]:
            print("Unauthorized user, access denied for ID {}".format(user_id))
            update.message.reply_text("USUARIO NO AUTORIZADO")
            return
        return func(update, context, *args, **kwargs)
    return wrapped


def send_action(action):
    """
        Create decorator for handlers that sends "action" while processing
        func command.
            action: examples are ChatAction.TYPING or ChatAction.UPLOAD_PHOTO
    """
    def decorator(func):
        @wraps(func)
        def command_func(update, context, *args, **kwargs):
            context.bot.send_chat_action(update.effective_message.chat_id
                , action
            )
            return func(update, context,  *args, **kwargs)
        return command_func
    
    return decorator



##################################################
# INLINE KEYBOARD BUTTONS /start function
##################################################

@restricted # NOTE: Access restricted to "start" function!
def start(update, context) -> None:
    # This will create inline buttons
    keyboard = [
        [
            InlineKeyboardButton("Info Grandes Deudas"
                , callback_data="Info Grandes Deudas")
            , InlineKeyboardButton("Volumen Ventas Ayer"
                , callback_data="Volumen Ventas Ayer")
        ]
        , [
            InlineKeyboardButton("Info Deudas Comerciales"
                , callback_data="Info Deudas Comerciales")
            , InlineKeyboardButton("Info Penetración"
                , callback_data="Info Penetración")
        ]
        , [
            InlineKeyboardButton("Info Despachos Camioneros"
                , callback_data="Info Despachos Camioneros")
            , InlineKeyboardButton("Info Ventas Lubri"
                , callback_data="Info Ventas Lubri")
        ]
        ,[
            InlineKeyboardButton("Balance YER"
                , callback_data="Balance YER")
            , InlineKeyboardButton("Info Penetración Lubricantes"
                , callback_data="Info Penetración Lubricantes")
        ]
        ,[
            InlineKeyboardButton("Info Volumenes Proyectados"
                , callback_data="Info Volumenes Proyectados")
            ,InlineKeyboardButton("Info MBC Liquidos Dolarizada"
                , callback_data="Info MBC Liquidos Dolarizada")
        ]
        , [
            InlineKeyboardButton("Balance Economico Descargas"
                , callback_data="Balance Economico Descargas")
            ,InlineKeyboardButton("+ YPF"
                , callback_data="+ YPF")
        ]
        , [
            InlineKeyboardButton("Salir"
                , callback_data="Salir")
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    update.message.reply_text("Por favor elija una opción:"
        , reply_markup=reply_markup
    )



##################################################
# INLINE KEYBOARD BUTTONS /resend function
##################################################

@restricted # NOTE: Access restricted to "resend" function!
def resend(update, context) -> None:
    # This will create inline buttons
    keyboard = [
        [
            InlineKeyboardButton("Info Globales"
                , callback_data="Info Globales")
            , InlineKeyboardButton("Info Presupuestos"
                , callback_data="Info Presupuestos")
        ]
        , [
            InlineKeyboardButton("Metricas"
                , callback_data="Metricas")
            , InlineKeyboardButton("Informes Financieros"
                , callback_data="Informes Financieros")
        ]
        , [
            InlineKeyboardButton("Info Semanal"
                , callback_data="Info Semanal")
            , InlineKeyboardButton("Info MBC"
                , callback_data="Info MBC")
        ]
        , [
            InlineKeyboardButton("Info Abastecimiento"
                , callback_data="Info Abastecimiento")
        ]
        ,[
            InlineKeyboardButton("Info Comercial Retail"
                , callback_data="Info Comercial Retail")
            ,InlineKeyboardButton("Info Grandes Clientes"
                , callback_data="Info Grandes Clientes")
        ]
        , [
            InlineKeyboardButton("Salir"
                , callback_data="Salir")
        ]
    ]

    reply_markup = InlineKeyboardMarkup(keyboard)

    update.message.reply_text("Por favor elija una opción:"
        , reply_markup=reply_markup
    )



##################################################
# INLINE KEYBOARD BUTTONS ACTIONS /start
##################################################

@send_action(ChatAction.UPLOAD_PHOTO)
def button(update, context) -> None:
    """Parses the CallbackQuery and updates the message text."""
    query = update.callback_query

    # CallbackQueries need to be answered to avoid errors, even with empty
    query.answer()

    query.edit_message_text(text=f"Opción Seleccionada: {query.data}")


    ####################################################
    # CallbackQuery responses for /start /informes
    ####################################################


    # INFO DEUDAS COMERCIALES
    if query.data == "Info Deudas Comerciales":
        try:
            condicionDeudores()
            run_path(filePath_InfoGrandesDeudas+"DeudaClientes.py")
            query.bot.send_photo(update.effective_chat.id
                , open(find("Deuda_Comercial.png", ubic), "rb")
            )
            query.bot.send_photo(update.effective_chat.id
                , open(find("Deudores_Morosos.png", ubic), "rb")
            )
            query.bot.send_photo(update.effective_chat.id
                , open(find("Deudores_MorososG.png", ubic), "rb")
            )
            query.bot.send_photo(update.effective_chat.id
                , open(find("Deudores_GestionJ.png", ubic), "rb")
            )
            query.bot.send_document(update.effective_chat.id
                , open(find("Clientes_Deudores.xlsx", ubic), "rb")
                , "Clientes_Deudores.xlsx"
            )

        except Exception as e:
            query.bot.send_message(update.effective_chat.id
                , text="Algo falló, revisar consola")
            logger.error("", exc_info=1)

    # INFO VOLUMEN VENTAS AYER
    elif query.data == "Volumen Ventas Ayer":
        try:
            run_path(filePath_InfoVtaComb+"TotalesPorCombustible.py")
            query.bot.send_photo(update.effective_chat.id
                , open(filePath_InfoVtaComb+"Info_VolumenVentas.png"
                    , "rb"
                )
            )
        except Exception as e:  
            query.bot.send_message(update.effective_chat.id
                , text="Algo falló, revisar consola")
            logger.error("", exc_info=1)

    # INFO GRANDES DEUDAS
    elif query.data == "Info Grandes Deudas":
        try:
            run_path(filePath_InfoGrandesDeudas+"GrandesDeudas.py")
            query.bot.send_photo(update.effective_chat.id
                , open(filePath_InfoGrandesDeudas+"Info_GrandesDeudores.png"
                    , "rb"
                )
            )
            query.bot.send_photo(update.effective_chat.id
                , open(filePath_InfoGrandesDeudas+"Info_GrandesDeudasPorVend.png"
                    , "rb"
                )
            )
        except Exception as e:  
            query.bot.send_message(update.effective_chat.id
                , text="Algo falló, revisar consola")
            logger.error("", exc_info=1)
    
    # INFO DESPACHOS CAMIONEROS
    elif query.data == "Info Despachos Camioneros":
        try:
            run_path(filePath_Info_Despachos_Camioneros+"DespachosCamion.py")
            query.bot.send_photo(update.effective_chat.id
                , open(filePath_Info_Despachos_Camioneros
                    + "Info_Despachos_Camioneros.png"
                    , "rb"
                )
            )
        except Exception as e:  
            query.bot.send_message(update.effective_chat.id
                , text="Algo falló, revisar consola")
            logger.error("", exc_info=1)

    # INFO PENETRACION
    elif query.data == "Info Penetración":
        try:
            penetracionRedMas()
            query.bot.send_photo(update.effective_chat.id
                , open(find("Info_PenetracionRedMas.png", ubic)
                    , "rb"
                )
            )
        except Exception as e:  
            query.bot.send_message(update.effective_chat.id
                , text="Algo falló, revisar consola")
            logger.error("", exc_info=1)
    
    # INFO VENTAS LUBRI
    elif query.data == "Info Ventas Lubri":
        try:
            ventaLubri()
            query.bot.send_photo(update.effective_chat.id
                , open(find("Info_VentaLubri.png", ubic)
                    , "rb"
                )
            )
        except Exception as e:  
            query.bot.send_message(update.effective_chat.id
                , text="Algo falló, revisar consola")
            logger.error("", exc_info=1)

    # BALANCE YER
    elif query.data == "Balance YER":
        try:
            balanceYER()
            query.bot.send_photo(update.effective_chat.id
                , open(find("balanceYER_Actual.png", ubic)
                    , "rb"
                )
            )
        except Exception as e:  
            query.bot.send_message(update.effective_chat.id
                , text="Algo falló, revisar consola")
            logger.error("", exc_info=1)
       
    # INFO Penetración Lubricantes
    elif query.data == "Info Penetración Lubricantes":
        try:
            run_path(filePath_YPF+"Azcuenaga.py")
            run_path(filePath_YPF+"Lamadrid.py")
            run_path(filePath_YPF+"Perdriel1.py")
            run_path(filePath_YPF+"Perdriel2.py")
            run_path(filePath_YPF+"Puente_Olive.py")
            run_path(filePath_YPF+"San_Jose.py")
            query.bot.send_document(update.effective_chat.id
                , open(filePath_YPF+"+YPFAzcuenaga.png"
                    , "rb"
                )
            )
            query.bot.send_document(update.effective_chat.id
                , open(filePath_YPF+"+YPFLamadrid.png"
                    , "rb"
                )
            )
            query.bot.send_document(update.effective_chat.id
                , open(filePath_YPF+"+YPFPerdriel.png"
                    , "rb"
                )
            )
            query.bot.send_document(update.effective_chat.id
                , open(filePath_YPF+"+YPFPerdriel2.png"
                    , "rb"
                )
            )
            query.bot.send_document(update.effective_chat.id
                , open(filePath_YPF+"+YPFPuente_Olive.png"
                    , "rb"
                )
            )
            query.bot.send_document(update.effective_chat.id
                , open(filePath_YPF+"+YPFSan_Jose.png"
                    , "rb"
                )
            )

        except Exception as e:  
            query.bot.send_message(update.effective_chat.id
                , text="Algo falló, revisar consola")
            logger.error("", exc_info=1)
    # volumenes proyectados
    elif query.data == "Info Volumenes Proyectados":
        try:
            run_path(filePath_InfoSemanal+"InfoVtaLiqProy.py")
            query.bot.send_photo(update.effective_chat.id
                , open(filePath_InfoSemanal+"Info_VtaLiquido_Semanal.png"
                    , "rb"
                )
            )
            query.bot.send_photo(update.effective_chat.id
                , open(filePath_InfoSemanal+"Info_VtaGNC_Semanal.png"
                    , "rb"
                )
            )
            query.bot.send_photo(update.effective_chat.id
                , open(filePath_InfoSemanal+"VtaGASOLEOS_Semanal.png"
                    , "rb"
                )
            )
            query.bot.send_photo(update.effective_chat.id
                , open(filePath_InfoSemanal+"VtaNAFTAS_Semanal.png"
                    , "rb"
                )
            )
            query.bot.send_photo(update.effective_chat.id
                , open(filePath_InfoSemanal+"VtaGOs_Semanal.png"
                    , "rb"
                )
            )
            query.bot.send_photo(update.effective_chat.id
                , open(filePath_InfoSemanal+"VtaEUs_Semanal.png"
                    , "rb"
                )
            )
            query.bot.send_photo(update.effective_chat.id
                , open(filePath_InfoSemanal+"VtaNSs_Semanal.png"
                    , "rb"
                )
            )
            query.bot.send_photo(update.effective_chat.id
                , open(filePath_InfoSemanal+"VtaNUs_Semanal.png"
                    , "rb"
                )
            )
        except Exception as e:  
            query.bot.send_message(update.effective_chat.id
                , text="Algo falló, revisar consola")
            logger.error("", exc_info=1)

# INFO MBC Liquidos Dolarizada
    elif query.data == 'Info MBC Liquidos Dolarizada':
        try:
            run_path(filePath_Info_MargenesGasoleos+"/MBCDolares/"+"MBCNaftasDolar.py")
            run_path(filePath_Info_MargenesGasoleos+"/MBCDolares/"+"MBCGasoleosDolar.py")
            query.bot.send_photo(update.effective_chat.id
                , open(filePath_Info_MargenesGasoleos+"MBCDolarGO.png"
                    , "rb"
                )
            )
            query.bot.send_photo(update.effective_chat.id
                , open(filePath_Info_MargenesGasoleos+"MBCDolarEU.png"
                    , "rb"
                )
            )
            query.bot.send_photo(update.effective_chat.id
                , open(filePath_Info_MargenesGasoleos+"MBCDolarNS.png"
                    , "rb"
                )
            )
            query.bot.send_photo(update.effective_chat.id
                , open(filePath_Info_MargenesGasoleos+"MBCDolarNU.png"
                    , "rb"
                )
            )

        except Exception as e:  
            query.bot.send_message(update.effective_chat.id
                , text="Algo falló, revisar consola")
            logger.error("", exc_info=1)

# INFO Balance Economico Descargas
    elif query.data == 'Balance Economico Descargas':
        try:
            run_path(filePath_Info_Descargas+"Balance_Ventas_Descargas.py")
            query.bot.send_photo(update.effective_chat.id
                , open(filePath_Info_Descargas+"Balance_V_D_GO.png"
                    , "rb"
                )
            )
            query.bot.send_photo(update.effective_chat.id
                , open(filePath_Info_Descargas+"Balance_V_D_EU.png"
                    , "rb"
                )
            )
            query.bot.send_photo(update.effective_chat.id
                , open(filePath_Info_Descargas+"Balance_V_D_NS.png"
                    , "rb"
                )
            )
            query.bot.send_photo(update.effective_chat.id
                , open(filePath_Info_Descargas+"Balance_V_D_NU.png"
                    , "rb"
                )
            )
            query.bot.send_photo(update.effective_chat.id
                , open(filePath_Info_Descargas+"Balance_V_D_TOTAL.png"
                    , "rb"
                )
            )
        except Exception as e:  
            query.bot.send_message(update.effective_chat.id
                , text="Algo falló, revisar consola")
            logger.error("", exc_info=1)

# INFO MAS YPF
    elif query.data == '+ YPF':
        try:
            run_path(filePath_YPF+"Azcuenaga.py")
            run_path(filePath_YPF+"Lamadrid.py")
            run_path(filePath_YPF+"Perdriel1.py")
            run_path(filePath_YPF+"Perdriel2.py")
            run_path(filePath_YPF+"Puente_Olive.py")
            run_path(filePath_YPF+"San_Jose.py")
            query.bot.send_photo(update.effective_chat.id
                , open(filePath_YPF+"+YPFPerdriel.png"
                    , "rb"
                )
            )
            query.bot.send_photo(update.effective_chat.id
                , open(filePath_YPF+"+YPFPerdriel2.png"
                    , "rb"
                )
            )
            query.bot.send_photo(update.effective_chat.id
                , open(filePath_YPF+"+YPFAzcuenaga.png"
                    , "rb"
                )
            )
            query.bot.send_photo(update.effective_chat.id
                , open(filePath_YPF+"+YPFLamadrid.png"
                    , "rb"
                )
            )
            query.bot.send_photo(update.effective_chat.id
                , open(filePath_YPF+"+YPFSan_Jose.png"
                    , "rb"
                )
            )
            query.bot.send_photo(update.effective_chat.id
                , open(filePath_YPF+"+YPFPuente_Olive.png"
                    , "rb"
                )
            )

        except Exception as e:  
            query.bot.send_message(update.effective_chat.id
                , text="Algo falló, revisar consola")
            logger.error("", exc_info=1)


    ####################################################
    # CallbackQuery responses for /resend /reenviar
    ####################################################

    # INFO GLOBALES
    
    elif query.data == "Info Globales":
        try:
            query.bot.send_message(update.effective_chat.id
                , text="Activando Reporte Info Globales en 10 segundos")

            context.job_queue.run_once(reportes_ivo_diario_InfoGlobal, 10)

        except Exception as e:
            query.bot.send_message(update.effective_chat.id
                , text="Algo falló, revisar consola")
            logger.error("", exc_info=1)
    
    # INFO PRESUPUESTOS
    elif query.data == "Info Presupuestos":
        try:
            query.bot.send_message(update.effective_chat.id
                , text= "Activando Reporte Diario Info Presupuestos, tiempo estimado: 1 minuto")
            context.job_queue.run_once(reportes_ivo_Presupuestos,10)

        except Exception as e:
            query.bot.send_message(update.effective_chat.id
                , text="Algo fallo, revisar consola")
            logger.error("",exc_info=1)
    # INFO METRICAS
    elif query.data == "Metricas":
        try:
            query.bot.send_message(update.effective_chat.id
                , text= "Activando Reporte Diario Metricas, tiempo estimado: 1 minuto")
            context.job_queue.run_once(reportes_ivo_diario_Metricas,10)

        except Exception as e:
            query.bot.send_message(update.effective_chat.id
                , text="Algo fallo, revisar consola")
            logger.error("",exc_info=1)

    # INFO FINANCIERA
    elif query.data == "Informes Financieros":
        try:
            query.bot.send_message(update.effective_chat.id
                , text="Activando Reporte Informes Financieros en 10 segundos")

            context.job_queue.run_once(reportes_ivo_diario_Financieros, 10)

        except Exception as e:  
            query.bot.send_message(update.effective_chat.id
                , text="Algo falló, revisar consola")
            logger.error("", exc_info=1)

    # INFO SEMANAL
    elif query.data == "Info Semanal":
        try:
            query.bot.send_message(update.effective_chat.id
                , text="Activando Reporte Info Semanal en 10 segundos")

            context.job_queue.run_once(reportes_ivo_semanal_InfoGlobal, 10)
            context.job_queue.run_once(reportes_ivo_semanal_Metricas, 10)
            context.job_queue.run_once(envio_reporte_MBCOperativo, 10)
            context.job_queue.run_once(envio_reporte_MBCGerencial, 10)
            context.job_queue.run_once(envio_reporte_Grandes_Clientes_semanal, 10)

        except Exception as e:  
            query.bot.send_message(update.effective_chat.id
                , text="Algo falló, revisar consola")
            logger.error("", exc_info=1)


    # INFO MBC
    elif query.data == "Info MBC":
        try:
            query.bot.send_message(update.effective_chat.id
                , text="Activando Reporte MBC Semanal en 10seg y Reporte MBC Diario en 1min")

            context.job_queue.run_once(envio_reporte_MBCGerencial_Diario, 10)

        except Exception as e:  
            query.bot.send_message(update.effective_chat.id
                , text="Algo falló, revisar consola")
            logger.error("", exc_info=1)

    # INFO ABASTECIMIENTO
    elif query.data == "Info Abastecimiento":
        try:
            query.bot.send_message(update.effective_chat.id
                , text="Activando Reporte Abastecimiento en 10 segundos")

            context.job_queue.run_once(envio_reporte_Abastecimiento, 10)

        except Exception as e:  
            query.bot.send_message(update.effective_chat.id
                , text="Algo falló, revisar consola")
            logger.error("", exc_info=1)
    
    # INFO COMERCIAL RETAIL
    elif query.data == "Info Comercial Retail":
        try:
            query.bot.send_message(update.effective_chat.id
                , text="Activando Reporte Comercial Retail en minutos")

            context.job_queue.run_once(envio_reporte_ComercialRetail, 10)

        except Exception as e:  
            query.bot.send_message(update.effective_chat.id
                , text="Algo falló, revisar consola")
            logger.error("", exc_info=1)
    
    # INFO GRANDES CLIENTES
    elif query.data == "Info Grandes Clientes":
        try:
            query.bot.send_message(update.effective_chat.id
                , text="Activando Reporte Grandes Clientes en 10 segundos")

            context.job_queue.run_once(envio_reporte_Grandes_Clientes, 10)

        except Exception as e:  
            query.bot.send_message(update.effective_chat.id
                , text="Algo falló, revisar consola")
            logger.error("", exc_info=1)

    

    ##################################################
    # Generic CallbacksQuery Responses
    ##################################################

    # EXIT OPTION
    elif query.data == "Salir":
        query.bot.send_message(update.effective_chat.id
            , text="Consulta Finalizada")

    else:
        query.bot.send_message(update.effective_chat.id
            , text="Algo no salió bien...")



##################################################
# HELP COMMAND ANSWER
##################################################

def help_command(update, context) -> None:
    update.message.reply_text(
        "->Comandos Públicos:\n"
        +"/help o /ayuda -> Muestra esta información.\n"
        +"->Comandos Restringidos:\n"
        +"/start o /informes -> Inicia consulta.\n"
        +"/resend o /reenviar -> Listado de informes para reenviar.\n"
    )


##################################################
# HELP_DEV COMMAND ANSWER
##################################################

def help_Dev_command(update, context) -> None:
    update.message.reply_text(
        "->Comandos Desarrollador:\n"
        +"/set (HH:MM) -> Define horario del informe diario.\n"
        +"/unset info_diario -> Detiene envío del informe diario.\n"
        +"/forzar_envio -> Activa el informe diario en 10seg"
    )



##################################################
# UNKNOWN COMMAND ANSWER
##################################################

# Show a message when the bot receive an unknown command
def unknown(update, context):
    context.bot.send_message(chat_id=update.effective_chat.id
    , text="Disculpa, no entendí ese comando\n"
        +"¿Necesitas /ayuda?")



##################################################
# "SET DAILY REPORT" COMMAND ANSWER
##################################################

@developerOnly
def set_envioDiario(update, context) -> None:
    try:
        # Parse time of what is written in chat command
        horario = dt.datetime.strptime(context.args[0], "%H:%M")
        horario_tz = argTime.localize(horario).timetz() # Apply ARG timezone
        
        # Remove old job to update time
        job_removed = remove_job_if_exists("info_diario", context)

        # Setting daily task       
        context.job_queue.run_daily(envio_reporte_ivo
            , horario_tz
            , name="info_diario"
        )
        text = "Informe Diario seteado!"
        if job_removed:
            text = text + " Horario de envío actualizado."

        update.message.reply_text(text)

    except:
        update.message.reply_text("Escribir hora y minutos: /set (HH:MM)")


# def tareas(update,context) -> None:
#     """ Send a list of scheduled jobs to the console 
#  ----------> NEED IMPROVEMENT to be send to chat <----------
#     """
#     context.job_queue.print_PTB_jobs()


def remove_job_if_exists(name, context) -> bool:
    """Remove job with given name. Returns whether job was removed."""
    current_jobs = context.job_queue.get_jobs_by_name(name)
    if not current_jobs:
        return False
    for job in current_jobs:
        job.schedule_removal()
    return True


#########################
# "REMOVE SCHEDULED REPORTS" COMMAND ANSWER
#########################

@developerOnly
def unset(update, context) -> None:
    """Remove the job if the user changed their mind."""
    try:
        task = context.args[0]
        job_removed = remove_job_if_exists(task, context)
        if job_removed:
            text = "Tarea " + task + " ha sido cancelada!"
        else:
            text= "No hay tarea programada con ese nombre."
        update.message.reply_text(text)
    except:
        update.message.reply_text(
            "Escribir nombre de la tarea: /unset (nombre_Tarea)")


##################################################
# "FORCE DAILY REPORT" COMMAND ANSWER
##################################################

@developerOnly
def forzar_envio(update, context) -> None:
    update.message.reply_text("Enviando informes al canal en 10 seg")
    context.job_queue.run_once(envio_reporte_ivo, 10, name="envio_forzado")




##################################################
# DAILY REPORT IVO (Info_Diario)
##################################################

# Reset all reports and send them to the designated channel
def envio_reporte_ivo(context):
    logger.info("\n->Comenzando generación de informes<-")

    try:
        condicionDeudores()
        logger.info("Info condicionDeudores reseteado")
    except Exception as e:
        context.bot.send_message(id_Autorizados[0]
            , text="Error al resetear Info condicionDeudores"
        )
        logger.error("Error al resetear Info condicionDeudores", exc_info=1)

    try:
        run_path(filePath_InfoVtaComb+"TotalesPorCombustible.py")
        logger.info("Info TotalesPorCombustible reseteado")
    except Exception as e:
        context.bot.send_message(id_Autorizados[0]
            , text="Error al resetear Info TotalesPorCombustible"
        )
        logger.error("Error al resetear Info TotalesPorCombustible", exc_info=1)

    try:
        run_path(filePath_InfoGrandesDeudas+"GrandesDeudas.py")
        logger.info("Info GrandesDeudas reseteado")
    except Exception as e:
        context.bot.send_message(id_Autorizados[0]
            , text="Error al resetear Info GrandesDeudas"
        )
        logger.error("Error al resetear Info GrandesDeudas", exc_info=1)

    try:
        run_path(filePath_Info_Despachos_Camioneros+"DespachosCamion.py")
        logger.info("Info Despachos_Camioneros reseteado")
    except Exception as e:
        context.bot.send_message(id_Autorizados[0]
            , text="Error al resetear Info Despachos_Camioneros"
        )
        logger.error("Error al resetear Despachos_Camioneros", exc_info=1)

    try:
        penetracionRedMas()
        logger.info("Info Penetracion_RedMas reseteado")
    except Exception as e:
        context.bot.send_message(id_Autorizados[0]
            , text="Error al resetear Info Penetracion_RedMas"
        )
        logger.error("Error al resetear Penetracion_RedMas", exc_info=1)
    
    try:
        ventaLubri()
        logger.info("Info Venta_Lubricante reseteado")
    except Exception as e:
        context.bot.send_message(id_Autorizados[0]
            , text="Error al resetear Info Venta_Lubricante"
        )
        logger.error("Error al resetear Venta_Lubricante", exc_info=1)

    fechahoy = dt.datetime.now().strftime("%d/%m/%y")

    for ids in destinatarios:

        context.bot.send_message(
            ids
            , text="INFORMES AUTOMÁTICOS "
            + fechahoy
            + "\n Datos relevados hasta ayer"
        )

        context.bot.send_photo(
            ids
            , open(filePath_InfoVtaComb+"Info_VolumenVentas.png", "rb")
            , "Venta Total por Combustible de Ayer"
        )

        context.bot.send_photo(
            ids
            , open(find("Info_PenetracionRedMas.png", ubic), "rb")
            , "Penetración RedMas"
        )

        context.bot.send_photo(
            ids
            , open(find("Info_VentaLubri.png", ubic), "rb")
            , "Venta Lubricantes"
        )

        context.bot.send_photo(
            ids
            , open(filePath_InfoGrandesDeudas+"Info_GrandesDeudores.png", "rb")
            , "Grandes Deudas"
        )

        context.bot.send_photo(
            ids
            , open(filePath_InfoGrandesDeudas+"Info_GrandesDeudasPorVend.png"
                , "rb")
            , "Grandes Deudas por Vendedor"
        )

        context.bot.send_photo(
            ids
            , open(find("DeudaComercial.png", ubic), "rb")
            , "Deuda Comercial"
        )

        context.bot.send_document(
            ids
            , open(find("ClientesDeudores.xlsx", ubic), "rb")
            , "ClientesDeudores.xlsx"
        )

        context.bot.send_photo(
            ids
            , open(filePath_Info_Despachos_Camioneros+
                "Info_Despachos_Camioneros.png", "rb")
            , "Despachos Camioneros"
        )


###############################################################
######################### NUEVO REPORTES IVO ##################
###############################################################

#### INFO GLOBAL DIARIO
def reportes_ivo_diario_InfoGlobal(context):
    logger.info("\n->Comenzando generación de informes<-")

    try:
        condicionDeudores()
        logger.info("Info condicionDeudores reseteado")
    except Exception as e:
        context.bot.send_message(id_Autorizados[0]
            , text="Error al resetear Info condicionDeudores"
        )
        logger.error("Error al resetear Info condicionDeudores", exc_info=1)
    try:
        run_path(filePath_Info_Pen_Salon+"PromedioVtasSalonIntermensual.py")
        logger.info("Info Ventas Salon reseteado")
        ventaLubri()
        logger.info("Info Venta_Lubricante reseteado")
    except Exception as e:
        context.bot.send_message(id_Autorizados[0]
            , text="Error al resetear Info Venta_Lubricante"
        )
        logger.error("Error al resetear Venta_Lubricante", exc_info=1)
    try:
        run_path(filePath_InfoVtaComb+"TotalesPorCombustible.py")
        logger.info("Info TotalesPorCombustible reseteado")
    except Exception as e:
        context.bot.send_message(id_Autorizados[0]
            , text="Error al resetear Info TotalesPorCombustible"
        )
        logger.error("Error al resetear Info TotalesPorCombustible", exc_info=1)

    try:
        run_path(filePath_Info_Despachos_Camioneros+"DespachosCamion.py")
        logger.info("Info Despachos_Camioneros reseteado")
    except Exception as e:
        context.bot.send_message(id_Autorizados[0]
            , text="Error al resetear Info Despachos_Camioneros"
        )
        logger.error("Error al resetear Despachos_Camioneros", exc_info=1)

    try:
        redControlSemanal()
        logger.info("Info redControlSemanal reseteado")
    except Exception as e:
        context.bot.send_message(id_Autorizados[0]
            , text="Error al resetear Info redControlSemanal"
        )
        logger.error("Error al resetear redControlSemanal", exc_info=1)

    fechahoy = dt.datetime.now().strftime("%d/%m/%y")

    InfoGlobales = rumaos_Informes_Globales

    context.bot.send_message(
        InfoGlobales
        , text="INFORMES AUTOMÁTICOS "
        + fechahoy
        + "\n Datos relevados hasta ayer"
    )
    """
    context.bot.send_photo(
        InfoGlobales
        , open(find("DeudaComercial.png", ubic), "rb")
        , "Deuda Comercial"
    )
    context.bot.send_photo(
        InfoGlobales
        , open(find("DeudaExcedida.png", ubic), "rb")
        , "Clientes Deudores Excedidos"
    )
    context.bot.send_photo(
        InfoGlobales
        , open(find("DeudaMorosa.png", ubic), "rb")
        , "Clientes Deudores Morosos"
    )
    context.bot.send_photo(
        InfoGlobales
        , open(find("DeudaMorosaGrave.png", ubic), "rb")
        , "Clientes Deudores Morosos Graves"
    )
    """
    context.bot.send_photo(
        InfoGlobales
        , open(filePath_InfoVtaComb+"Info_VolumenVentas.png", "rb")
        , "Venta Total por Combustible de Ayer"
    )

    context.bot.send_photo(
        InfoGlobales
        , open(filePath_Info_Despachos_Camioneros+
            "Info_Despachos_Camioneros.png", "rb")
        , "Despachos Camioneros"
    )
    context.bot.send_photo(
        InfoGlobales
        , open(find("Info_VentaLubri.png", ubic), "rb")
        , "Venta Lubricantes"
    )

    context.bot.send_photo(
        InfoGlobales
        , open(find("Info_Promedio_VtasIntermensual.png", ubic), "rb")
        , "Ventas de Salon"
    )
    context.bot.send_photo(
        InfoGlobales
        , open(find("Info_RedControlLiq_Semanal.png", ubic), "rb")
        , "Red Control Líquido"
    )

#### INFO GLOBAL SEMANAL
def reportes_ivo_semanal_InfoGlobal(context):

    logger.info("\n->Comenzando generación de informe semanal<-")

    try:
        run_path(filePath_InfoSemanal+"InfoVtaLiqProy.py")
        logger.info("Info vtaSemanalProy_Liq_GNC reseteado")
        run_path(filePath_InfoVtaComb+"Mix_Ventas_por_Vendedor.py")
        logger.info("Info Mix Ventas por Vendedor reseteado")
    except Exception as e:
        context.bot.send_message(id_Autorizados[0]
            , text="Error al resetear Info vtaSemanalProy_Liq_GNC"
        )
        logger.error("Error al resetear vtaSemanalProy_Liq_GNC y mix por vendedor", exc_info=1)
    
    fechahoy = dt.datetime.now().strftime("%d/%m/%y")
    weekStart = (dt.date.today()-dt.timedelta(days=7)).strftime("%d/%m/%y")
    weekEnd = (dt.date.today()-dt.timedelta(days=1)).strftime("%d/%m/%y")


    InfoGlobalesSemanal = rumaos_Informes_Globales

    context.bot.send_message(
        InfoGlobalesSemanal
        , text="INFORMES AUTOMÁTICOS SEMANALES\n" 
            + "PERÍODO "
            + weekStart
            + " AL "
            + weekEnd
    )

    context.bot.send_photo(
        InfoGlobalesSemanal
        , open(find("VtaNAFTAS_Semanal.png", ubic), "rb")
        , "Venta Naftas Proyectado"
    )
    context.bot.send_photo(
        InfoGlobalesSemanal
        , open(find("VtaGASOLEOS_Semanal.png", ubic), "rb")
        , "Venta Gasoleos Proyectado"
    )
    context.bot.send_photo(
        InfoGlobalesSemanal
        , open(find("Info_VtaGNC_Semanal.png", ubic), "rb")
        , "Venta de GNC Proyectado"
    )
    context.bot.send_photo(
        InfoGlobalesSemanal
        , open(find("Info_VtaLiquido_Semanal.png", ubic), "rb")
        , "Venta de Liquidos Proyectado"
    )
    context.bot.send_photo(
        InfoGlobalesSemanal
        , open(find("Mix_Ventas_Vendedor_Gasoleos.png", ubic), "rb")
        , "Volumen Vendido por Vendedor Gasoleos"
    )
    context.bot.send_photo(
        InfoGlobalesSemanal
        , open(find("Mix_Ventas_Vendedor_Naftas.png", ubic), "rb")
        , "Volumen Vendido por Vendedor Naftas"
    )
#### INFO PRESUPUESTOS DIARIO
def reportes_ivo_Presupuestos(context):
    logger.info("\n->Comenzando generación de informe Presupuestos<-")
    
    try:
        run_path(filePath_Info_Presu_Gas+"Presupuesto_GNC.py")
        run_path(filePath_Info_Presu_Gas+"Presupuesto_Naftas.py")
        run_path(filePath_Info_Presu_Gas+"Presupuesto_Gasoleos.py")
        run_path(filePath_Info_Presu_Gas+"Presupuesto_Lubricantes.py")
        logger.info("Info Ejecucion Presupuestaria reseteado")
    except Exception as e:
        context.bot.send_message(id_Autorizados
            , text="Error al resetear Info Ejecucion Presupuestaria"
        )
        logger.error("Error al resetear Info Ejecucion Presupuestaria", exc_info=1)

    try:
        run_path(filePath_Info_Presu_Gas+"PenetracionLubri_Acumulado.py")
        logger.info("Info Presupuesto diario GNC reseteado")
        run_path(filePath_Info_Presu_Gas+"Mix.py")
        logger.info("Mix reseteado")
    except Exception as e:
        context.bot.send_message(id_Autorizados
            , text="Error al resetear Info Mix"
        )
        logger.error("Error al resetear Info Mix", exc_info=1)
    try:
        run_path(filePath_Info_Presu_Gas+"PromedioVtasSalon_Acumulado.py")
        logger.info("Info Ticket Promedio Ventas Salon")
    except Exception as e:
        context.bot.send_message(id_Autorizados[0]
            , text="Error al resetear Info Ticket Promedio Ventas Salon"
        )
        logger.error("Error al resetear Info Ticket Promedio Ventas Salon", exc_info=1)

    try:
        run_path(filePath_Info_Presu_Gas +"VtasSalonPresupuesto_Acumulado.py")
        logger.info("Info Ejecucion Presupuestaria ventas salon")
    except Exception as e:
        context.bot.send_message(id_Autorizados
            , text="Error al resetear Info Ejecucion Presupuestaria ventas salon"
        )
        logger.error("Error al resetear Info Ejecucion Presupuestaria ventas salon", exc_info=1)

    fechahoy = dt.datetime.now().strftime("%d/%m/%y")
    chat_id = rumaos_Presupuestos
   
    context.bot.send_message(
        chat_id
        , text="INFORMES AUTOMÁTICOS Ejecucion Presupuestos "
        + fechahoy 
        + "\n Datos relevados hasta ayer"
    )
    context.bot.send_photo(
        chat_id
        , open(filePath_Info_Presu_Gas+"Info_Presupuesto_GO_Acumulado.png", "rb")
        , "Info Ejecucion Presupuestaria Ultra Diesel"
    )
    context.bot.send_photo(
        chat_id
        , open(filePath_Info_Presu_Gas+"Info_Presupuesto_EU_Acumulado.png", "rb")
        , "Info Ejecucion Presupuestaria Infinia Diesel"
    )
    context.bot.send_photo(
        chat_id
        , open(filePath_Info_Presu_Gas+"Info_Presupuesto_NS_Acumulado.png", "rb")
        , "Info Ejecucion Presupuestaria Nafta Super"
    )
    context.bot.send_photo(
        chat_id
        , open(filePath_Info_Presu_Gas+"Info_Presupuesto_NU_Acumulado.png", "rb")
        , "Info Ejecucion Presupuestaria Infinia Nafta"
    )
    context.bot.send_photo(
        chat_id
        , open(filePath_Info_Presu_Gas+"Info_Presupuesto_GNC_Acumulado.png", "rb")
        , "Info Ejecucion Presupuestaria GNC"
    )
    context.bot.send_photo(
        chat_id
        , open(filePath_Info_Presu_Gas+"Presupuesto_Lubricantes.png", "rb")
        , "Info Ejecucion Presupuestaria Lubricantes"
    )

    context.bot.send_photo(
        chat_id
        , open(filePath_Info_Presu_Gas+"Info_Penetracion_Lubri_Acumulado.png", "rb")
        , "Info Ejecucion Lubricantes Presupuestado Diario"
    )
    context.bot.send_photo(
        chat_id
        , open(filePath_Info_Presu_Gas +"PresupuestadoSalon_Acumulado.png", "rb")
        , "Info Desvio Presupuestario Ventas Salon"
    )
    context.bot.send_photo(
        chat_id
        , open(filePath_Info_Presu_Gas+"Info_Promedio_Vtas_Acumulado.png", "rb")
        , "Info Ticket Salon Promedio"
    )

### INFO METRICAS DIARIO
def reportes_ivo_diario_Metricas(context):
    logger.info("\n->Comenzando generación de informe Metricas Diario<-")

    try:
        penetracionRedMas()
        logger.info("Info Penetracion_RedMas reseteado")
    except Exception as e:
        context.bot.send_message(id_Autorizados[0]
            , text="Error al resetear Info Penetracion_RedMas"
        )
        logger.error("Error al resetear Penetracion_RedMas", exc_info=1)

    try:
        run_path(filePath_Info_Control_Info+"Calibracion_tanques.py")
        logger.info("Info calibracion tanques reseteado")
        run_path(filePath_Info_Control_Info+"CalibracionGNC_Alerta.py")
        logger.info("Info calibracion GNC reseteado")
    except Exception as e:
        context.bot.send_message(id_Autorizados[0]
            , text="Error al resetear Info Penetracion_RedMas"
        )
        logger.error("Error al resetear Calibracion Tanques/GNC", exc_info=1)



    try:
        run_path(filePath_Info_Pen_Salon+"PromedioVtasSalon.py")
        logger.info("Info Ticket Promedio Ventas Salon")
        run_path(filePath_Info_Presu_Gas+"Mix.py")
        logger.info("Mix reseteado")
    except Exception as e:
        context.bot.send_message(id_Autorizados[0]
            , text="Error al resetear Info Ticket Promedio Ventas Salon"
        )
        logger.error("Error al resetear Info Ticket Promedio Ventas Salon", exc_info=1)


    try:
        run_path(filePath_Info_Descargas+"VolumenPromedio.py")
        logger.info("Evolucion Volumenes Promedio reseteado")
    except Exception as e:
        context.bot.send_message(id_Autorizados[0]
            , text="Error al resetear Evolucion Volumenes Promedio"
        )
        logger.error("Error al resetear Evolucion Volumenes Promedio", exc_info=1)



    fechahoy = dt.datetime.now().strftime("%d/%m/%y")
    chat_id = rumaos_Metricas
    calibracion = rumaosCalibracion_Control
    context.bot.send_message(
        chat_id
        , text="INFORMES AUTOMÁTICOS Periferia "
        + fechahoy 
        + "\n Datos relevados hasta ayer"
    )

    context.bot.send_photo(
        chat_id
        , open(find("Info_PenetracionRedMas.png", ubic), "rb")
        , "Penetración RedMas"
    )
    context.bot.send_photo(
        chat_id
        , open(find("VolumenPromGO.png", ubic), "rb")
        , "Evolucion Volumen Promedio por Despacho Ultra Diesel"
    )
    context.bot.send_photo(
        chat_id
        , open(find("VolumenPromEU.png", ubic), "rb")
        , "Evolucion Volumen Promedio por Despacho Infinia Diesel"
    )
    context.bot.send_photo(
        chat_id
        , open(find("VolumenPromNS.png", ubic), "rb")
        , "Evolucion Volumen Promedio por Despacho Nafta Super"
    )
    context.bot.send_photo(
        chat_id
        , open(find("VolumenPromNU.png", ubic), "rb")
        , "Evolucion Volumen Promedio por Despacho Infinia Nafta"
    )
    context.bot.send_photo(
        chat_id
        , open(find("VolumenPromGNC.png", ubic), "rb")
        , "Evolucion Volumen Promedio por Despacho GNC"
    )
    context.bot.send_photo(
        chat_id
        , open(filePath_Info_Presu_Gas+"Info_MIX.png", "rb")
        , "MIX G3/G2 Ejecucion Presupuestaria"
    )
    context.bot.send_photo(
        chat_id
        , open(filePath_Info_Control_Info+"Calibracion_tanques_semanal.png", "rb")
        , "Calibracion Semanal Tanques"
    )
    context.bot.send_photo(
        chat_id
        , open(filePath_Info_Control_Info+"Calibracion_GNC_semanal.png", "rb")
        , "Calibracion Semanal GNC"
    )
    context.bot.send_photo(
        calibracion
        , open(filePath_Info_Control_Info+"Calibracion_tanques_semanal.png", "rb")
        , "Calibracion Semanal Tanques"
    )
    context.bot.send_photo(
        calibracion
        , open(filePath_Info_Control_Info+"Calibracion_GNC_semanal.png", "rb")
        , "Calibracion Semanal GNC"
    )
### INFO METRICAS SEMANAL
def reportes_ivo_semanal_Metricas(context):
    logger.info("\n->Comenzando generación de informe Metricas Semanal<-")

    try:
        redControlSemanal()
        logger.info("Info redControlSemanal reseteado")
    except Exception as e:
        context.bot.send_message(id_Autorizados[0]
            , text="Error al resetear Info redControlSemanal"
        )
        logger.error("Error al resetear redControlSemanal", exc_info=1)


    try:
        run_path(filePath_YPF+"Azcuenaga.py")
        logger.info("Info +YPF Reseteado")
        run_path(filePath_YPF+"Lamadrid.py")
        logger.info("Info +YPF Reseteado")
        run_path(filePath_YPF+"Perdriel1.py")
        logger.info("Info +YPF Reseteado")
        run_path(filePath_YPF+"Perdriel2.py")
        logger.info("Info +YPF Reseteado")
        run_path(filePath_YPF+"Puente_Olive.py")
        logger.info("Info +YPF Reseteado")
        run_path(filePath_YPF+"San_Jose.py")
        logger.info("Info +YPF Reseteado")
    except Exception as e:
        context.bot.send_message(id_Autorizados[0]
            , text="Error al resetear Info +YPF"
        )
        logger.error("Error al resetear Info +YPF", exc_info=1)

    try:
        run_path(filePath_Pen_APP_YPF+"PenetracionUENAppYPF.py")
        logger.info("Info Penetracion App YPF por Estacion")

    except Exception as e:
        context.bot.send_message(id_Autorizados[0]
            , text="Error al resetear Info Penetracion App YPF"
        )
        logger.error("Error al resetear Info Penetracion App YPF", exc_info=1)
    try:
        run_path(filePath_RedMas+"RedMasClientesActivos.py")
        logger.info("CRM Gasoleos y GNC")
    except Exception as e:
        context.bot.send_message(id_Autorizados[0]
            , text="Error al resetear CRM gasoleos y GNC"
        )
        logger.error("Error al resetear CRM gasoleos y GNC", exc_info=1)


    try:
        run_path(filePath_Pen_RED_PAGO+"PenRedPagoCantidad.py")
        logger.info("Info Penetracion Red Pago Cantidad")
        run_path(filePath_Pen_RED_PAGO+"PenRedPagoPesos.py")
        logger.info("Info Penetracion Red Pago Pesos")
        run_path(filePath_Pen_RED_PAGO+"PenRedPagoServiC.py")
        logger.info("Info Penetracion Red Pago Servi Cantidad")
        run_path(filePath_Pen_RED_PAGO+"PenRedPagoServiP.py")
        logger.info("Info Penetracion Red Pago Servi Pesos")
    except Exception as e:
        context.bot.send_message(id_Autorizados[0]
            , text="Error al resetear Info Penetracion Red Pago"
        )
        logger.error("Error al resetear Info Penetracion Red Pago", exc_info=1)

    fechahoy = dt.datetime.now().strftime("%d/%m/%y")
    weekStart = (dt.date.today()-dt.timedelta(days=7)).strftime("%d/%m/%y")
    weekEnd = (dt.date.today()-dt.timedelta(days=1)).strftime("%d/%m/%y")


    chat_id = rumaos_Metricas
    joaquinBriffe= 1879091694
    masYPF=rumaosMasYPF #Grupo +YPF
    context.bot.send_message(
        chat_id
        , text="INFORMES AUTOMÁTICOS SEMANALES\n" 
            + "PERÍODO "
            + weekStart
            + " AL "
            + weekEnd
    )

    context.bot.send_photo(
        chat_id
        , open(find("Info_Penetración_Semanal.png", ubic), "rb")
        , "Penetración RedMás Semanal"
    )
    context.bot.send_photo(
        chat_id
        , open(filePath_Pen_APP_YPF+"Info_Penetracion_UEN.png", "rb")
        , "Info Penetracion App YPF por Estacion"
    )
    context.bot.send_photo(
        chat_id
        , open(filePath_Pen_RED_PAGO+"PenRedPagoCantidad.png", "rb")
        , "Info Penetracion RedPago Playa Cantidad"
    )
    context.bot.send_photo(
        chat_id
        , open(filePath_Pen_RED_PAGO+"PenRedPagoPesos.png", "rb")
        , "Info Penetracion RedPago Playa Pesos"
    )
    context.bot.send_photo(
        chat_id
        , open(filePath_Pen_RED_PAGO+"PenRedPagoServiC.png", "rb")
        , "Info Penetracion RedPago ServiCompras Cantidad"
    )
    context.bot.send_photo(
        chat_id
        , open(filePath_Pen_RED_PAGO+"PenRedPagoServiP.png", "rb")
        , "Info Penetracion RedPago ServiCompras Pesos"
    )
    context.bot.send_photo(
        joaquinBriffe
        , open(filePath_YPF +
            "+YPFPerdriel.png", "rb")
        , "+YPF Perdriel 1"
    )
    context.bot.send_photo(
        joaquinBriffe
        , open(filePath_YPF +
            "+YPFPerdriel2.png", "rb")
        , "+YPF Perdriel 2"
    )
    context.bot.send_photo(
        joaquinBriffe
        , open(filePath_YPF +
            "+YPFAzcuenaga.png", "rb")
        , "+YPF Azcuenaga"
    )
    context.bot.send_photo(
        joaquinBriffe
        , open(filePath_YPF +
            "+YPFLamadrid.png", "rb")
        , "+YPF Lamadrid"
    )
    context.bot.send_photo(
        joaquinBriffe
        , open(filePath_YPF +
            "+YPFSan_Jose.png", "rb")
        , "+YPF San Jose"
    )
    context.bot.send_photo(
        joaquinBriffe
        , open(filePath_YPF +
            "+YPFPuente_Olive.png", "rb")
        , "+YPF Puente Olive"
    )
    context.bot.send_photo(
        joaquinBriffe
        , open(filePath_RedMas +
            "CRM.png", "rb")
        , "CRM Gasoleos y GNC"
    )


#### INFO FINANCIEROS DIARIOS
def reportes_ivo_diario_Financieros(context):
    logger.info("\n->Comenzando generación de informe Financiero<-")

    try:
        arqueos()
        logger.info("Info Arqueos reseteado")
    except Exception as e:
        context.bot.send_message(id_Autorizados[0]
            , text="Error al resetear Info Arqueos"
        )
        logger.error("Error al resetear Arqueos", exc_info=1)

    try:
        chequesSaldos()
        logger.info("Info chequesSaldos reseteado")
    except Exception as e:
        context.bot.send_message(id_Autorizados[0]
            , text="Error al resetear Info chequesSaldos"
        )
        logger.error("Error al resetear chequesSaldos", exc_info=1)

    try:
        bancosSaldos()
        logger.info("Info BancosSaldos reseteado")
    except Exception as e:
        context.bot.send_message(id_Autorizados[0]
            , text="Error al resetear Info BancosSaldos"
        )
        logger.error("Error al resetear BancosSaldos", exc_info=1)

    try:
        run_path(filePath_InfoGrandesDeudas+"DashboardDeudas.py")
        logger.info("Info Deuda Clientes reseteado")
    except Exception as e:
        context.bot.send_message(id_Autorizados[0]
            , text="Error al resetear Info Deuda Clientes"
        )
        logger.error("Error al resetear Deuda Clientes", exc_info=1)

    try:
        condicionDeudores()
        logger.info("Info condicionDeudores reseteado")
    except Exception as e:
        context.bot.send_message(id_Autorizados[0]
            , text="Error al resetear Info condicionDeudores"
        )
        logger.error("Error al resetear condicionDeudores", exc_info=1)


    try:
        usos_SGFin()
        logger.info("Info Usos_SGFin reseteado")
    except Exception as e:
        context.bot.send_message(id_Autorizados[0]
            , text="Error al resetear Info Usos_SGFin"
        )
        logger.error("Error al resetear Usos_SGFin", exc_info=1)


    try:
        activosCorrientes()
        logger.info("Info activosCorrientes reseteado")
    except Exception as e:
        context.bot.send_message(id_Autorizados[0]
            , text="Error al resetear Info activosCorrientes"
        )
        logger.error("Error al resetear activosCorrientes", exc_info=1)

    try:
        pasivosCorrientes()
        logger.info("Info pasivosCorrientes reseteado")
    except Exception as e:
        context.bot.send_message(id_Autorizados[0]
            , text="Error al resetear Info pasivosCorrientes"
        )
        logger.error("Error al resetear pasivosCorrientes", exc_info=1)

    try:
        run_path(filePath_InfoGrandesDeudas+"DeudaClientes.py")
        logger.info("Info Deuda Clientes reseteado")
    except Exception as e:
        context.bot.send_message(id_Autorizados[0]
            , text="Error al resetear Info Deuda"
        )
        logger.error("Error al resetear Info Deuda", exc_info=1)


    fechahoy = dt.datetime.now().strftime("%d/%m/%y")

    # Where to send the things
    chat_id = rumaos_Informes_Financieros

    context.bot.send_message(
        chat_id
        , text="INFORMES AUTOMÁTICOS "
            + fechahoy
            + "\n Datos actualizados al momento de emisión"
    )

    context.bot.send_photo(
        chat_id
        , open(find("Arqueos.png", ubic), "rb")
        , "Arqueos"
    )

    context.bot.send_photo(
        chat_id
        , open(find("ArqueosUSD.png", ubic), "rb")
        , "Arqueo en Dolares"
    )

    context.bot.send_photo(
        chat_id
        , open(find("ChequesSaldos.png", ubic), "rb")
        , "Cheques Saldos"
    )

    context.bot.send_photo(
        chat_id
        , open(find("BancosSaldos.png", ubic), "rb")
        , "Bancos Saldos"
    )

    context.bot.send_photo(
        chat_id
        , open(find("Usos_SGFin.png", ubic), "rb")
        , "Egresos Por Uso"
    )

    context.bot.send_photo(
        chat_id
        , open(find("activosCorrientes.png", ubic), "rb")
        , "Activos Corrientes"
    )

    context.bot.send_photo(
        chat_id
        , open(find("PasivosCorrientes.png", ubic), "rb")
        , "Pasivos Corrientes"
    )
    context.bot.send_photo(
        chat_id
        , open(find("Deuda_Comercial.png", ubic), "rb")
        , "Deuda Comercial"
    )
    context.bot.send_photo(
        chat_id
        , open(find("Deudores_Morosos.png", ubic), "rb")
        , "Clientes Deudores Morosos"
    )
    context.bot.send_photo(
        chat_id
        , open(find("Deudores_MorososG.png", ubic), "rb")
        , "Clientes Deudores Morosos Graves"
    )
    context.bot.send_photo(
        chat_id
        , open(find("Deudores_GestionJ.png", ubic), "rb")
        , "Clientes Deudores Gestion Judicial"
    )
    context.bot.send_photo(
        chat_id
        , open(find("TablaDeuda.png", ubic), "rb")
        , "Estado de Deuda Comercial"
    )
    context.bot.send_photo(
        chat_id
        , open(find("TablaDeudaVolumen.png", ubic), "rb")
        , "Estado de Deuda Comercial Volumenes"
    )

#### GRUPO MBC 

def envio_reporte_MBC_IVO(context):
    logger.info("\n->Comenzando generación de informe MBC Gerenial<-")

    try:
        run_path(filePath_Info_MargenesGasoleos+"MargenGNCN.py")
        logger.info("Info Margen GNC")
        run_path(filePath_Info_MargenesGasoleos+"MargenNaftasN.py")
        logger.info("Info Margen Naftas")
        run_path(filePath_Info_MargenesGasoleos+"MargenGasoleosN.py")
        logger.info("Info Margen Gasoleos por Estacion")
        run_path(filePath_Info_MargenesGasoleos+"ConsolidadoSalon.py")
        logger.info("Info Margen Gasoleos por Estacion")
        run_path(filePath_Info_MargenesGasoleos+"MargenEmpresa.py")
        logger.info("Info Margen Consolidado")
        run_path(filePath_Info_MargenesGasoleos+"MargenLubricantes.py")
        logger.info("Info Margen Lubricantes por Estacion")
        run_path(filePath_Info_Presu_Gas+"Mix.py")
        logger.info("Mix reseteado")
        run_path(filePath_Info_MargenesGasoleos+"MBCDolares/"+"MBCGasoleosDolar.py")
        logger.info("Info Margen Gasoleos Dolares")
        run_path(filePath_Info_MargenesGasoleos+"MBCDolares/"+"MBCNaftasDolar.py")
        logger.info("Info Margen Naftas Dolares")

    except Exception as e:
        context.bot.send_message(id_Autorizados[0]
            , text="Error al resetear Info Margenes"
        )
        logger.error("Error al resetear Info Margenes", exc_info=1)
    
    try:
        run_path(filePath_InfoSemanal+"InfoVtaLiqProy.py")
        logger.info("Info vtaSemanalProy_Liq_GNC reseteado")
    except Exception as e:
        context.bot.send_message(id_Autorizados[0]
            , text="Error al resetear Info vtaSemanalProy_Liq_GNC"
        )
        logger.error("Error al resetear vtaSemanalProy_Liq_GNC", exc_info=1)



    try:
        run_path(filePath_Info_Margenes+"MargenTotales.py")
        logger.info("Info Margen Salon Totales Reseteado")
    except Exception as e:
        context.bot.send_message(id_Autorizados[0]
            , text="Error al resetear Info Margenes Salon"
        )
        logger.error("Error al resetear Info Margenes Salon", exc_info=1)


    fechahoy = dt.datetime.now().strftime("%d/%m/%y")

    chat_id = rumaos_Informes_MBC
   
    context.bot.send_message(
        chat_id
        , text="INFORMES AUTOMÁTICOS MBC Gerencial Semanal"
        + fechahoy 
        + "\n Datos relevados hasta ayer"
    )
    context.bot.send_photo(
        chat_id
        , open(filePath_Info_MargenesGasoleos+"MBCGNC.png", "rb")
        , "Info Margenes GNC"
    )
    context.bot.send_photo(
        chat_id
        , open(filePath_Info_MargenesGasoleos+"MBCNaftasNS.png", "rb")
        , "Info Margenes Nafta Super"
    )
    context.bot.send_photo(
        chat_id
        , open(filePath_Info_MargenesGasoleos+"MBCNaftasNU.png", "rb")
        , "Info Margenes Infinia Nafta"
    )
    context.bot.send_photo(
        chat_id
        , open(filePath_Info_MargenesGasoleos+"MBCGasoleosGO.png", "rb")
        , "Info Margenes Gasoleos Ultra Diesel"
    )
    context.bot.send_photo(
        chat_id
        , open(filePath_Info_MargenesGasoleos+"MBCGasoleosEU.png", "rb")
        , "Info Margenes Gasoleos Infinia Diesel"
    )
    context.bot.send_photo(
        chat_id
        , open(filePath_Info_MargenesGasoleos+"MBCSalon.png", "rb")
        , "Info Margenes Salon"
    )
    context.bot.send_photo(
        chat_id
        , open(filePath_Info_MargenesGasoleos+"MargenLubri.png", "rb")
        , "Info Margenes Lubricantes"
    )
    context.bot.send_photo(
        chat_id
        , open(filePath_Info_Margenes+"MARGENESTOTALES.png", "rb")
        , "Info Margenes Salon Totales por Familias"
    )
    context.bot.send_photo(
        chat_id
        , open(filePath_Info_MargenesGasoleos+"ConsolidadoEmpresa.png", "rb")
        , "Info Margenes Consolidado Empresa"
    )
    context.bot.send_photo(
        chat_id
        , open(filePath_Info_MargenesGasoleos+"MBCDolarGO.png", "rb")
        , "MBC Dolares Ultra Diesel"
    )
    context.bot.send_photo(
        chat_id
        , open(filePath_Info_MargenesGasoleos+"MBCDolarEU.png", "rb")
        , "MBC Dolares Infinia Diesel"
    )
    context.bot.send_photo(
        chat_id
        , open(filePath_Info_MargenesGasoleos+"MBCDolarNS.png", "rb")
        , "MBC Dolares Nafta Super"
    )
    context.bot.send_photo(
        chat_id
        , open(filePath_Info_MargenesGasoleos+"MBCDolarNU.png", "rb")
        , "MBC Dolares Infinia Nafta"
    )

#### Check Reporte Finanzas
def envio_Check_Finanzas(context):
    logger.info("\n->Comenzando generación de informe Check Finanzas<-")
    try:
        run_path(filePath_Info_Control_Info+"checkFinanzas.py")
        logger.info("Check Finanzas")
    except Exception as e:
        context.bot.send_message(id_Autorizados[0]
            , text="Error al resetear Info Deuda"
        )
        logger.error("Error al Checkear Datos", exc_info=1)


    fechahoy = dt.datetime.now().strftime("%d/%m/%y")

    # Where to send the things
    chat_id = rumaos_Informes_Financieros

    context.bot.send_message(
        chat_id
        , text="INFORMES AUTOMÁTICOS "
            + fechahoy
            + "\n Datos actualizados al momento de emisión"
    )
    context.bot.send_photo(
        chat_id
        , open(filePath_Info_Control_Info+"checkeo_Eduardo.png", "rb")
        , "Check Informacion Eduardo"
    )
    context.bot.send_photo(
        chat_id
        , open(filePath_Info_Control_Info+"checkeo_Kamel.png", "rb")
        , "Check Informacion Kamel"
    )

#### Dashboard Eduardo
def envio_DB_Eduardo(context):
    logger.info("\n->Comenzando generación de informe Dashboard Eduardo <-")
    try:
        run_path(filePath_Eduardo + "Dahsboard_Eduardo.py")
        logger.info("Dashboard Eduardo")
    except Exception as e:
        context.bot.send_message(id_Autorizados[0]
            , text="Error al resetear Info Deuda"
        )
        logger.error("Error al emitir el reporte Dashboard Eduardo", exc_info=1)


    nombre_mes_Actual = "JUNIO"

    nombre_mes_Anterior = " - MAYO - "

    fechahoy = dt.datetime.now().strftime("%d/%m/%y")

    # Where to send the things
    chat_id = rumaos_Informes_Globales

    context.bot.send_message(
        chat_id
        , text="INFORME GASOLEOS"
            + nombre_mes_Anterior + nombre_mes_Actual
            + "\n Datos actualizados al momento de emisión"
    )

    context.bot.send_photo(
        chat_id
        , open(filePath_Eduardo+"DE_Comercial_Final.png", "rb")
        , "INDICADORES COMERCIALES" + nombre_mes_Anterior + nombre_mes_Actual
    )

    context.bot.send_photo(
        chat_id
        , open(filePath_Eduardo+"DE_Economico_Final.png", "rb")
        , "INDICADORES ECONOMICOS" + nombre_mes_Anterior + nombre_mes_Actual
    )

    context.bot.send_photo(
        chat_id
        , open(filePath_Eduardo+"DE_Financieros_Final.png", "rb")
        , "INDICADORES FINANCIEROS" + nombre_mes_Anterior + nombre_mes_Actual
    )

    context.bot.send_photo(
        chat_id
        , open(filePath_Eduardo+"Rdo_Renta_Presupuestada.png", "rb")
        , "RESULTADOS PRESUPUESTADOS - JUNIO"
    )
##################################################################
####################### CANALES OPERATIVOS #######################
##################################################################

#####################################
#### Actualizacion Info DASHBOARD SHEET ####
#####################################

def actualizacion_Info_Sheet(context):
    logger.info("\n->Comenzando generación de Actualizacion de Sheets<-")

    try:
        run_path(filePath_SHEET+"Dolar_sheet.py")
        logger.info("Dolar Sheet Actualizado")
        run_path(filePath_SHEET+"IPC_sheet.py")
        logger.info("IPC Sheet Actualizado")
        run_path(filePath_SHEET+"MBC_sheet.py")
        logger.info("MBC Sheet Actualizado")
        run_path(filePath_SHEET+"DeudaSheet.py")
        logger.info("Deuda Sheet Actualizado")

    except Exception as e:
        context.bot.send_message(id_Autorizados[0]
            , text="Error al Actualizar Sheets"
        )
        logger.error("Error al Actualizar Sheets", exc_info=1)






#####################################
#### GRUPO MBC Operativo Semanal ####
#####################################

def envio_reporte_MBCOperativo(context):
    logger.info("\n->Comenzando generación de informe MBC Operativo<-")

    try:
        run_path(filePath_Info_Margenes+"MargenSalonAZ.py")
        logger.info("Info Margen Salon Azcuenaga Reseteado")
        run_path(filePath_Info_Margenes+"MargenSalonLM.py")
        logger.info("Info Margen Salon LAMADRID Reseteado")
        run_path(filePath_Info_Margenes+"MargenSalonM2.py")
        logger.info("Info Margen Salon Mercado 2 Reseteado")
        run_path(filePath_Info_Margenes+"MargenSalonP1.py")
        logger.info("Info Margen Salon Perdriel 1 Reseteado")
        run_path(filePath_Info_Margenes+"MargenSalonP2.py")
        logger.info("Info Margen Salon Perdriel 2 Reseteado")
        run_path(filePath_Info_Margenes+"MargenSalonPO.py")
        logger.info("Info Margen Salon Puente Olive Reseteado")
        run_path(filePath_Info_Margenes+"MargenSalonSJ.py")
        logger.info("Info Margen Salon San Jose Reseteado")
        run_path(filePath_Info_Margenes+"MargenSalonXS.py")
        logger.info("Info Margen Salon Xpress Reseteado")
    except Exception as e:
        context.bot.send_message(id_Autorizados[0]
            , text="Error al resetear Info Margenes Salon"
        )
        logger.error("Error al resetear Info Margenes Salon", exc_info=1)

    fechahoy = dt.datetime.now().strftime("%d/%m/%y")
    chat_id = rumaos_MBC_operativo
   
    context.bot.send_message(
        chat_id
        , text="INFORMES AUTOMÁTICOS MBC Operativo Semanal"
        + fechahoy 
        + "\n Datos relevados hasta ayer"
    )
    context.bot.send_photo(
        chat_id
        , open(filePath_Info_Margenes+"ControlVtasAZ.png", "rb")
        , "Info Margenes Salon Azcuenaga"
    )
    context.bot.send_photo(
        chat_id
        , open(filePath_Info_Margenes+"ControlVtasLM.png", "rb")
        , "Info Margenes Salon Lamadrid"
    )

    context.bot.send_photo(
        chat_id
        , open(filePath_Info_Margenes+"ControlVtasP1.png", "rb")
        , "Info Margenes Salon Perdriel"
    )
    context.bot.send_photo(
        chat_id
        , open(filePath_Info_Margenes+"ControlVtasP2.png", "rb")
        , "Info Margenes Salon Perdriel 2"
    )
    context.bot.send_photo(
        chat_id
        , open(filePath_Info_Margenes+"ControlVtasSJ.png", "rb")
        , "Info Margenes Salon San Jose"
    )
    context.bot.send_photo(
        chat_id
        , open(filePath_Info_Margenes+"ControlVtasXS.png", "rb")
        , "Info Margenes Salon Xpress"
    )
    context.bot.send_photo(
        chat_id
        , open(filePath_Info_Margenes+"ControlVtasPO.png", "rb")
        , "Info Margenes Salon Puente Olive"
    )
#####################################
#### GRUPO MBC Gerencial Semanal ####
#####################################

def envio_reporte_MBCGerencial(context):
    logger.info("\n->Comenzando generación de informe MBC Gerenial<-")

    try:
        run_path(filePath_Info_MargenesGasoleos+"MargenGNCN.py")
        logger.info("Info Margen GNC")
        run_path(filePath_Info_MargenesGasoleos+"MargenNaftasN.py")
        logger.info("Info Margen Naftas")
        run_path(filePath_Info_MargenesGasoleos+"MargenGasoleosN.py")
        logger.info("Info Margen Gasoleos por Estacion")
        run_path(filePath_Info_MargenesGasoleos+"ConsolidadoSalon.py")
        logger.info("Info Margen Gasoleos por Estacion")
        run_path(filePath_Info_Presu_Gas+"Mix.py")
        logger.info("Mix reseteado")

    except Exception as e:
        context.bot.send_message(id_Autorizados[0]
            , text="Error al resetear Info Margenes"
        )
        logger.error("Error al resetear Info Margenes", exc_info=1)
    

    
    try:
        run_path(filePath_InfoSemanal+"InfoVtaLiqProy.py")
        logger.info("Info vtaSemanalProy_Liq_GNC reseteado")
    except Exception as e:
        context.bot.send_message(id_Autorizados[0]
            , text="Error al resetear Info vtaSemanalProy_Liq_GNC"
        )
        logger.error("Error al resetear vtaSemanalProy_Liq_GNC", exc_info=1)
    
    try:
        run_path(filePath_Info_Presu_Gas+"Mix.py")
        logger.info("Mix reseteado")

    except Exception as e:
        context.bot.send_message(id_Autorizados[0]
            , text="Error al resetear Info Margenes Gasoleos"
        )
        logger.error("Error al resetear Info Margenes Gasoleos", exc_info=1)

    try:
        run_path(filePath_Info_Margenes+"MargenTotales.py")
        logger.info("Info Margen Salon Totales Reseteado")
    except Exception as e:
        context.bot.send_message(id_Autorizados[0]
            , text="Error al resetear Info Margenes Salon"
        )
        logger.error("Error al resetear Info Margenes Salon", exc_info=1)

    try:
        run_path(filePath_Pen_APP_YPF+"PenetracionEmpAppYPF.py")
        logger.info("Info Penetracion App YPF por Empleado")
        run_path(filePath_Pen_APP_YPF+"PenetracionUENAppYPF.py")
        logger.info("Info Penetracion App YPF por Estacion")
    except Exception as e:
        context.bot.send_message(id_Autorizados[0]
            , text="Error al resetear Info Penetracion App YPF"
        )
        logger.error("Error al resetear Info Penetracion App YPF", exc_info=1)


    fechahoy = dt.datetime.now().strftime("%d/%m/%y")
    chat_id = rumaos_MBC_gerencial
   
    context.bot.send_message(
        chat_id
        , text="INFORMES AUTOMÁTICOS MBC Gerencial Semanal"
        + fechahoy 
        + "\n Datos relevados hasta ayer"
    )
    context.bot.send_photo(
        chat_id
        , open(filePath_Info_MargenesGasoleos+"MBCGNC.png", "rb")
        , "Info Margenes GNC"
    )
    context.bot.send_photo(
        chat_id
        , open(filePath_Info_MargenesGasoleos+"MBCNaftasNS.png", "rb")
        , "Info Margenes Nafta Super"
    )
    context.bot.send_photo(
        chat_id
        , open(filePath_Info_MargenesGasoleos+"MBCNaftasNU.png", "rb")
        , "Info Margenes Infinia Nafta"
    )
    context.bot.send_photo(
        chat_id
        , open(filePath_Info_MargenesGasoleos+"MBCGasoleosGO.png", "rb")
        , "Info Margenes Gasoleos Ultra Diesel"
    )
    context.bot.send_photo(
        chat_id
        , open(filePath_Info_MargenesGasoleos+"MBCGasoleosEU.png", "rb")
        , "Info Margenes Gasoleos Infinia Diesel"
    )
    context.bot.send_photo(
        chat_id
        , open(filePath_Info_MargenesGasoleos+"MBCSalon.png", "rb")
        , "Info Margenes Salon"
    )
    context.bot.send_photo(
        chat_id
        , open(find("VtaGASOLEOS_Semanal.png", ubic), "rb")
        , "Venta Gasoleos Proyectado"
    )
    context.bot.send_photo(
        chat_id
        , open(find("VtaNAFTAS_Semanal.png", ubic), "rb")
        , "Venta Naftas Proyectado"
    )
    context.bot.send_photo(
        chat_id
        , open(find("Info_VtaGNC_Semanal.png", ubic), "rb")
        , "Venta de GNC Proyectado"
    )
    context.bot.send_photo(
        chat_id
        , open(filePath_Info_Margenes+"MARGENESTOTALES.png", "rb")
        , "Info Margenes Salon Totales"
    )
    context.bot.send_photo(
        chat_id
        , open(filePath_Pen_APP_YPF+"Info_Penetracion_UEN.png", "rb")
        , "Info Penetracion App YPF por Estacion"
    )
    context.bot.send_document(
            chat_id
        , open(filePath_Pen_APP_YPF+"Penetracion_AppYPF_Emp.xlsx", "rb")
        , "Penetracion_AppYPF_Emp.xlsx"
    )
    context.bot.send_photo(
        chat_id
        , open(filePath_Info_Presu_Gas+"Info_MIX.png", "rb")
        , "MIX G3/G2 Ejecucion Presupuestaria"
    )
    context.bot.send_photo(
        chat_id
        , open(filePath_InfoVtaComb+"Info_VolumenVentas.png", "rb")
        , "Venta Total por Combustible de Ayer"
    )
    context.bot.send_photo(
        chat_id
        , open(find("Info_VentaLubri.png", ubic), "rb")
        , "Venta Lubricantes"
    )
    context.bot.send_photo(
        chat_id
        , open(find("Info_Promedio_VtasIntermensual.png", ubic), "rb")
        , "Ventas de Salon"
    )
######################################################
#### GRUPO MBC Gerencial Diario/OPERATIVO DIARIO #####
######################################################

def envio_reporte_MBCGerencial_Diario(context):
    logger.info("\n->Comenzando generación de informe MBC Gerenial<-")

    try:
        run_path(filePath_InfoVtaComb+"TotalesPorCombustible.py")
        logger.info("Info TotalesPorCombustible reseteado")
    except Exception as e:
        context.bot.send_message(id_Autorizados[0]
            , text="Error al resetear Info TotalesPorCombustible"
        )
        logger.error("Error al resetear Info TotalesPorCombustible", exc_info=1)
    try:
        run_path(filePath_Info_RH+"Ausentes.py")
        logger.info("Info Ausentes reseteado")
    except Exception as e:
        context.bot.send_message(id_Autorizados[0]
            , text="Error al resetear Info Ausentes reseteado"
        )
        logger.error("Error al resetear Info Ausentes", exc_info=1)
    try:
        ventaLubri()
        logger.info("Info Venta_Lubricante reseteado")
    except Exception as e:
        context.bot.send_message(id_Autorizados[0]
            , text="Error al resetear Info Venta_Lubricante"
        )
        logger.error("Error al resetear Venta_Lubricante", exc_info=1)
    try:
        run_path(filePath_InfoGrandesDeudas+"DashboardDeudas.py")
        logger.info("Info Deuda Clientes reseteado")
    except Exception as e:
        context.bot.send_message(id_Autorizados[0]
            , text="Error al resetear Info Deuda Clientes"
        )
        logger.error("Error al resetear Deuda Clientes", exc_info=1)

    
    try:
        run_path(filePath_Info_Pen_Salon+"PromedioVtasSalonIntermensual.py")
        logger.info("Info Ventas Salon reseteado")
    except Exception as e:
        context.bot.send_message(id_Autorizados[0]
            , text="Error al resetear Info Ventas Salon"
        )
        logger.error("Error al resetear Ventas Salon", exc_info=1)

    fechahoy = dt.datetime.now().strftime("%d/%m/%y")
    operativo = rumaos_MBC_operativo
    gerencial = rumaos_MBC_gerencial
    chat_id= [operativo,gerencial]
    sebavillar= 740873634

    #### ENVIO DIARIO TAMBIEN A MBC OPERATIVO   

    chat_idOperativo= rumaos_MBC_operativo
    context.bot.send_photo(
        operativo
        , open(filePath_InfoVtaComb+"Info_VolumenVentas.png", "rb")
        , "Venta Total por Combustible de Ayer"
    )
    context.bot.send_photo(
        operativo
        , open(find("Info_VentaLubri.png", ubic), "rb")
        , "Venta Lubricantes"
    )
    context.bot.send_photo(
        operativo
        , open(find("Info_Promedio_VtasIntermensual.png", ubic), "rb")
        , "Ventas de Salon"
    )
    context.bot.send_photo(
        sebavillar
        , open(find("Ausentes.png", ubic), "rb")
        , "Ausentes Ayer"
    )
    context.bot.send_message(
        gerencial 
        , text="INFORMES AUTOMÁTICOS "
        + fechahoy
        + "\n Datos relevados hasta ayer"
    )

    context.bot.send_photo(
        gerencial 
        , open(find("TablaDeuda.png", ubic), "rb")
        , "Deuda Comercial"
    )
    context.bot.send_photo(
        gerencial 
        , open(find("TablaDeudaVolumen.png", ubic), "rb")
        , "Deuda Comercial Volumenes"
    )


######################################
#### GRUPO ABASTECIMIENTO DIARIO #####
######################################

def envio_reporte_Abastecimiento(context):
    logger.info("\n->Comenzando generación de informe Abastecimiento<-")

    try:
        run_path(filePath_Info_Descargas+"InformeVolumenStock.py")
        logger.info("Info Margen Gasoleos")
        run_path(filePath_Info_Descargas+"Descargas.py")
        logger.info("Info Margen Gasoleos")
        run_path(filePath_KAMEL +"EstimadoRecaudacion.py")
        logger.info("Info Margen Gasoleos")
    except Exception as e:
        context.bot.send_message(id_Autorizados[0]
            , text="Error al resetear Info Volumen"
        )
        logger.error("Error al resetear Info Volumen", exc_info=1)


    
    fechahoy = dt.datetime.now().strftime("%d/%m/%y")
    chat_id = rumaos_Abastecimiento
    kamel = 778494913
    tiendas_Elaboracion = -521451668
    context.bot.send_message(
        chat_id
        , text="INFORMES AUTOMÁTICOS Abastecimiento"
        + fechahoy 
        + "\n Datos relevados hasta ayer"
    )
    context.bot.send_document(
            chat_id
        , open(filePath_Info_Descargas+"volumen.xlsx", "rb")
        , "Info Volumen.xlsx"
    )
    context.bot.send_photo(
        chat_id
        , open(filePath_Info_Descargas+"VolumEuDapsa.png", "rb")
        , "Info Volumen EU Dapsa"
    )
    context.bot.send_photo(
        chat_id
        , open(filePath_Info_Descargas+"VolumGoDapsa.png", "rb")
        , "Info Volumen GO Dapsa"
    )
    context.bot.send_photo(
        chat_id
        , open(filePath_Info_Descargas+"VolumNsDapsa.png", "rb")
        , "Info Volumen NS Dapsa"
    )
    context.bot.send_photo(
        chat_id
        , open(filePath_Info_Descargas+"VolumEuYPF.png", "rb")
        , "Info Volumen EU YPF"
    )
    context.bot.send_photo(
        chat_id
        , open(filePath_Info_Descargas+"VolumGoYPF.png", "rb")
        , "Info Volumen GO YPF"
    )
    context.bot.send_photo(
        chat_id
        , open(filePath_Info_Descargas+"VolumNsYPF.png", "rb")
        , "Info Volumen NS YPF"
    )
    context.bot.send_photo(
        chat_id
        , open(filePath_Info_Descargas+"VolumNuYPF.png", "rb")
        , "Info Volumen NU YPF"
    )
    context.bot.send_photo(
        chat_id
        , open(filePath_Info_Descargas+"PresupuestoDescargasGO.png", "rb")
        , "Ejecucion Cupo de Descargas YPF GO"
    )
    context.bot.send_photo(
        kamel
        , open(filePath_KAMEL +"Estimado_Recaudacion.png", "rb")
        , "Estimado de Recaudacion"
    )
#################################
#### GRUPO Comercial Retail #####
#################################

def envio_reporte_ComercialRetail(context):
    logger.info("\n->Comenzando generación de informe comercial retail<-")
 
    try:
        run_path(filePath_Info_Presu_Gas+"InfoPresupuestoGas.py")
        logger.info("Info Ejecucion Presupuestaria reseteado")
    except Exception as e:
        context.bot.send_message(id_Autorizados
            , text="Error al resetear Info Ejecucion Presupuestaria"
        )
        logger.error("Error al resetear Info Ejecucion Presupuestaria", exc_info=1)
 
    try:
        penetracionRedMas()
        logger.info("Info Penetracion_RedMas reseteado")
    except Exception as e:
        context.bot.send_message(id_Autorizados[0]
            , text="Error al resetear Info Penetracion_RedMas"
        )
        logger.error("Error al resetear Penetracion_RedMas", exc_info=1)

    try:
        run_path(filePath_Info_Pen_Salon +"VtasSalonPresupuesto.py")
        logger.info("Info Ejecucion Presupuestaria ventas salon")
    except Exception as e:
        context.bot.send_message(id_Autorizados
            , text="Error al resetear Info Ejecucion Presupuestaria ventas salon"
        )
        logger.error("Error al resetear Info Ejecucion Presupuestaria ventas salon", exc_info=1)

    try:
        run_path(filePath_Pen_APP_YPF+"PenetracionUENAppYPF.py")
        logger.info("Info Penetracion App YPF por Estacion")
    except Exception as e:
        context.bot.send_message(id_Autorizados[0]
            , text="Error al resetear Info Penetracion App YPF"
        )
        logger.error("Error al resetear Info Penetracion App YPF", exc_info=1)


    try:
        run_path(filePath_Info_Pen_Lubri+"PenetracionLubri.py")
        logger.info("Info Penetracion Lubricantes Reseteado")
    except Exception as e:
        context.bot.send_message(id_Autorizados[0]
            , text="Error al resetear Info Penetracion Lubricantes"
        )
        logger.error("Error al resetear Info Penetracion Lubricantes", exc_info=1)

    try:
        run_path(filePath_Info_Pen_Salon+"PromedioVtasSalon.py")
        logger.info("Info Ticket Promedio Ventas Salon")
        run_path(filePath_Info_Pen_Salon+"PromedioVtasSalonIntermensual.py")
        logger.info("Info Ventas Salon")
    except Exception as e:
        context.bot.send_message(id_Autorizados[0]
            , text="Error al resetear Info Ticket Promedio Ventas Salon"
        )
        logger.error("Error al resetear Info Ticket Promedio Ventas Salon", exc_info=1)


    fechahoy = dt.datetime.now().strftime("%d/%m/%y")
    chat_id = rumaos_Comer_retail
   
    context.bot.send_message(
        chat_id
        , text="INFORMES AUTOMÁTICOS Comercial Retail "
        + fechahoy 
        + "\n Datos relevados hasta ayer"
    )
  
    context.bot.send_photo(
        chat_id
        , open(filePath_Info_Presu_Gas+"Info_Presupuesto_GNC.png", "rb")
        , "Info Ejecucion Presupuestaria GNC"
    )

    '''context.bot.send_photo(
        chat_id
        , open(filePath_Info_Pen_Lubri+"Info_PenetracionLubri_Diario.png", "rb")
        , "Info Ejecucion Lubricantes Presupuestado Diario"
    )'''

    context.bot.send_photo(
        chat_id
        , open(filePath_Info_Pen_Salon +"PresupuestadoSalon.png", "rb")
        , "Info Desvio Presupuestario Ventas Salon"
    )

    context.bot.send_photo(
        chat_id
        , open(filePath_Info_Pen_Lubri+"Info_Penetracion_Lubri.png", "rb")
        , "Info Penetracion Lubricantes"
    )
    context.bot.send_photo(
        chat_id
        , open(filePath_Info_Pen_Salon+"Info_Promedio_Vtas.png", "rb")
        , "Info Ticket Salon Promedio"
    )

    context.bot.send_photo(
        chat_id
        , open(find("Info_PenetracionRedMas.png", ubic), "rb")
        , "Penetración RedMas"
    )
    context.bot.send_photo(
        chat_id
        , open(filePath_Pen_APP_YPF+"Info_Penetracion_UEN.png", "rb")
        , "Info Penetracion App YPF por Estacion"
    )
#########################################
#### GRUPO Comercial Retail Semanal #####
#########################################

def envio_reporte_ComercialRetail_Semanal(context):
    logger.info("\n->Comenzando generación de informe comercial retail<-")
    try:
        redControlSemanal()
        logger.info("Info redControlSemanal reseteado")
    except Exception as e:
        context.bot.send_message(id_Autorizados[0]
            , text="Error al resetear Info redControlSemanal"
        )
        logger.error("Error al resetear redControlSemanal", exc_info=1)
    try:
        run_path(filePath_YPF+"Azcuenaga.py")
        logger.info("Info +YPF Reseteado")
        run_path(filePath_YPF+"Lamadrid.py")
        logger.info("Info +YPF Reseteado")
        run_path(filePath_YPF+"Perdriel1.py")
        logger.info("Info +YPF Reseteado")
        run_path(filePath_YPF+"Perdriel2.py")
        logger.info("Info +YPF Reseteado")
        run_path(filePath_YPF+"Puente_Olive.py")
        logger.info("Info +YPF Reseteado")
        run_path(filePath_YPF+"San_Jose.py")
        logger.info("Info +YPF Reseteado")
    except Exception as e:
        context.bot.send_message(id_Autorizados[0]
            , text="Error al resetear Info +YPF"
        )
        logger.error("Error al resetear Info +YPF", exc_info=1)
    try:
        run_path(filePath_RedMas+"RedMasClientesActivos.py")
        logger.info("CRM Gasoleos y GNC")
        run_path(filePath_RedMas+"graficoCRM.py")
        logger.info("Grafico CRM GNC")
        run_path(filePath_RedMas+"graficoRemis.py")
        logger.info("Grafico CRM GNC Remis")
        run_path(filePath_RedMas+"graficoTaxis.py")
        logger.info("Grafico CRM GNC Taxis")
        run_path(filePath_RedMas+"graficoFlete.py")
        logger.info("Grafico CRM GNC Fletes")
    except Exception as e:
        context.bot.send_message(id_Autorizados[0]
            , text="Error al resetear CRM gasoleos y GNC"
        )
        logger.error("Error al resetear CRM gasoleos y GNC", exc_info=1)


    fechahoy = dt.datetime.now().strftime("%d/%m/%y")
    weekStart = (dt.date.today()-dt.timedelta(days=7)).strftime("%d/%m/%y")
    weekEnd = (dt.date.today()-dt.timedelta(days=1)).strftime("%d/%m/%y")


    chat_id = rumaos_Comer_retail

    context.bot.send_message(
        chat_id
        , text="INFORMES AUTOMÁTICOS SEMANALES\n" 
            + "PERÍODO "
            + weekStart
            + " AL "
            + weekEnd
    )
    context.bot.send_photo(
        chat_id
        , open(find("Info_RedControlLiq_Semanal.png", ubic), "rb")
        , "Red Control Líquido"
    )
    context.bot.send_photo(
        chat_id
        , open(find("Info_Penetración_Semanal.png", ubic), "rb")
        , "Penetración RedMás Semanal"
    )
    context.bot.send_photo(
        chat_id
        , open(filePath_YPF +
            "+YPFPerdriel.png", "rb")
        , "+YPF Perdriel 1"
    )
    context.bot.send_photo(
        chat_id
        , open(filePath_YPF +
            "+YPFPerdriel2.png", "rb")
        , "+YPF Perdriel 2"
    )
    context.bot.send_photo(
        chat_id
        , open(filePath_YPF +
            "+YPFAzcuenaga.png", "rb")
        , "+YPF Azcuenaga"
    )
    context.bot.send_photo(
        chat_id
        , open(filePath_YPF +
            "+YPFLamadrid.png", "rb")
        , "+YPF Lamadrid"
    )
    context.bot.send_photo(
        chat_id
        , open(filePath_YPF +
            "+YPFSan_Jose.png", "rb")
        , "+YPF San Jose"
    )
    context.bot.send_photo(
        chat_id
        , open(filePath_YPF +
            "+YPFPuente_Olive.png", "rb")
        , "+YPF Puente Olive"
    )
    context.bot.send_photo(
        chat_id
        , open(filePath_RedMas +
            "CRM.png", "rb")
        , "CRM Gasoleos y GNC"
    )
    context.bot.send_photo(
        chat_id
        , open(filePath_RedMas +
            "grafico.png", "rb")
        , "CRM GNC"
    )
    context.bot.send_photo(
        chat_id
        , open(filePath_RedMas +
            "graficoTaxis.png", "rb")
        , "CRM GNC Taxis"
    )
    context.bot.send_photo(
        chat_id
        , open(filePath_RedMas +
            "graficoRemis.png", "rb")
        , "CRM GNC Remis"
    )
    context.bot.send_photo(
        chat_id
        , open(filePath_RedMas +
            "graficoFlete.png", "rb")
        , "CRM GNC Fletes"
    )

#############################################
#### GRUPO COMBUSTIBLES GRANDES CLIENTES ####
#############################################

def envio_reporte_Grandes_Clientes(context):
    logger.info("\n->Comenzando generación de informes Combustibles Grandes Clientes<-")
    try:
        run_path(filePath_InfoGrandesDeudas+"DeudaClientes.py")
        logger.info("Info Deuda Clientes reseteado")
    except Exception as e:
        context.bot.send_message(id_Autorizados[0]
            , text="Error al resetear Info Deuda Clientes"
        )
        logger.error("Error al resetear Deuda Clientes", exc_info=1)

    try:
        run_path(filePath_Info_Despachos_Camioneros+"DespachosCamion.py")
        logger.info("Info Despachos_Camioneros reseteado")
    except Exception as e:
        context.bot.send_message(id_Autorizados[0]
            , text="Error al resetear Info Despachos_Camioneros"
        )
        logger.error("Error al resetear Despachos_Camioneros", exc_info=1)

    try:
        run_path(filePath_InfoVtaComb+"TotalesPorCombustible.py")
        logger.info("Info TotalesPorCombustible reseteado")
    except Exception as e:
        context.bot.send_message(id_Autorizados[0]
            , text="Error al resetear Info TotalesPorCombustible"
        )
        logger.error("Error al resetear Info TotalesPorCombustible", exc_info=1)
  
    try:
        ventaLubri()
        logger.info("Info Venta_Lubricante reseteado")
    except Exception as e:
        context.bot.send_message(id_Autorizados[0]
            , text="Error al resetear Info Venta_Lubricante"
        )
        logger.error("Error al resetear Venta_Lubricante", exc_info=1)
    
    try:
        run_path(filePath_Info_Pen_Salon+"PromedioVtasSalonIntermensual.py")
        logger.info("Info Ventas Salon reseteado")
    except Exception as e:
        context.bot.send_message(id_Autorizados[0]
            , text="Error al resetear Info Ventas Salon"
        )
        logger.error("Error al resetear Ventas Salon", exc_info=1)


    fechahoy = dt.datetime.now().strftime("%d/%m/%y")

    chat_id = rumaos_Grandes_clientes

    context.bot.send_message(
        chat_id
        , text="INFORMES AUTOMÁTICOS "
        + fechahoy
        + "\n Datos relevados hasta ayer"
    )
    context.bot.send_photo(
        chat_id 
        , open(find("Deuda_Comercial.png", ubic), "rb")
        , "Deuda Comercial"
    )
    context.bot.send_photo(
        chat_id 
        , open(find("Deudores_Morosos.png", ubic), "rb")
        , "Clientes Deudores Morosos"
    )
    context.bot.send_photo(
        chat_id 
        , open(find("Deudores_MorososG.png", ubic), "rb")
        , "Clientes Deudores Morosos Graves"
    )
    context.bot.send_photo(
        chat_id 
        , open(find("Deudores_GestionJ.png", ubic), "rb")
        , "Clientes Deudores Gestion Judicial"
    )
    context.bot.send_document(
        chat_id
        , open(find("Deudores_Comerciales.pdf", ubic), "rb")
        , "Deudores_Comerciales.pdf"
    )
    context.bot.send_photo(
        chat_id
        , open(filePath_Info_Despachos_Camioneros+
            "Info_Despachos_Camioneros.png", "rb")
        , "Despachos Camioneros"
    )
#####################################################
#### GRUPO COMBUSTIBLES GRANDES CLIENTES SEMANAL ####
#####################################################

def envio_reporte_Grandes_Clientes_semanal(context):
    logger.info("\n->Comenzando generación de informes Combustibles Grandes Clientes<-")
    try:
        vtaProyGranClient()
        logger.info("Info vtaProyGranClient reseteado")
        run_path(filePath_InfoVtaComb+"Mix_Ventas_por_Vendedor.py")
        logger.info("Info Mix Ventas por Vendedor reseteado")
    except Exception as e:
        context.bot.send_message(id_Autorizados[0]
            , text="Error al resetear Info vtaProyGranClient"
        )
        logger.error("Error al resetear vtaProyGranClient", exc_info=1)



    weekStart = (dt.date.today()-dt.timedelta(days=7)).strftime("%d/%m/%y")
    weekEnd = (dt.date.today()-dt.timedelta(days=1)).strftime("%d/%m/%y")

    weekStart = (dt.date.today()-dt.timedelta(days=7)).strftime("%d/%m/%y")
    weekEnd = (dt.date.today()-dt.timedelta(days=1)).strftime("%d/%m/%y")


    chat_id = rumaos_Grandes_clientes

    context.bot.send_message(
        chat_id=chat_id
        , text="INFORMES AUTOMÁTICOS SEMANALES\n" 
            + "PERÍODO "
            + weekStart
            + " AL "
            + weekEnd
    )
    context.bot.send_document(
            chat_id
            , open(find("Grandes_Clientes_Baja_Consumo.pdf", ubic), "rb")
            , "Grandes_Clientes_Baja_Consumo.pdf"
        )
    context.bot.send_photo(
        InfoGlobalesSemanal
        , open(find("Mix_Ventas_Vendedor_Gasoleos.png", ubic), "rb")
        , "Volumen Vendido por Vendedor Gasoleos"
    )
    context.bot.send_photo(
        InfoGlobalesSemanal
        , open(find("Mix_Ventas_Vendedor_Naftas.png", ubic), "rb")
        , "Volumen Vendido por Vendedor Naftas"
    )

##################################################
# DAILY REPORT Comercial
##################################################

def envio_reporte_comercial(context):

    logger.info("\n->Comenzando generación de informe comercial<-")

    fechahoy = dt.datetime.now().strftime("%d/%m/%y")

    chat_id = rumaos_info_com

    context.bot.send_message(
        chat_id
        , text="INFORMES AUTOMÁTICOS " 
        + fechahoy 
        + "\n Datos relevados hasta ayer"
    )

    context.bot.send_photo(
        chat_id
        , open(filePath_InfoGrandesDeudas+"Info_GrandesDeudores.png", "rb")
        , "Grandes Deudas"
    )

    context.bot.send_photo(
        chat_id
        , open(filePath_InfoGrandesDeudas+"Info_GrandesDeudasPorVend.png"
            , "rb")
        , "Grandes Deudas por Vendedor"
    )

    context.bot.send_photo(
        chat_id
        , open(find("DeudaComercial.png", ubic), "rb")
        , "Deuda Comercial"
    )

    context.bot.send_photo(
        chat_id
        , open(find("DeudaExcedida.png", ubic), "rb")
        , "Deudas Excedidas"
    )

    context.bot.send_photo(
        chat_id
        , open(find("DeudaMorosa.png", ubic), "rb")
        , "Deudas Morosas"
    )
    
    context.bot.send_photo(
        chat_id
        , open(find("DeudaMorosaGrave.png", ubic), "rb")
        , "Deudas Morosas Graves"
    )

    context.bot.send_document(
        chat_id
        , open(find("ClientesDeudores.xlsx", ubic), "rb")
        , "ClientesDeudores.xlsx"
    )

    context.bot.send_photo(
        chat_id
        , open(filePath_Info_Despachos_Camioneros+
            "Info_Despachos_Camioneros.png", "rb")
        , "Despachos Camioneros"
    ) 
    


##################################################
# DAILY REPORT Lapuchesky
##################################################

def envio_reporte_lapuchesky(context):

    logger.info("\n->Comenzando generación de informe Lapuchesky<-")

    fechahoy = dt.datetime.now().strftime("%d/%m/%y")

    context.bot.send_message(
        chat_id=lapuchesky
        , text="INFORMES AUTOMÁTICOS " + fechahoy
    )

    context.bot.send_photo(
        lapuchesky
        , open(find("Info_PenetracionRedMas.png", ubic), "rb")
        , "Penetración RedMas"
    )
    
    

##################################################
# DAILY REPORT of Checks
##################################################

def envio_reporte_cheques(context):

    logger.info("\n->Comenzando generación de informe cheques<-")

    try:
        cheques_ayer()
        logger.info("Info Cheques_Ayer reseteado")
    except Exception as e:
        context.bot.send_message(id_Autorizados[0]
            , text="Error al resetear Info Cheques_Ayer"
        )
        logger.error("Error al resetear Cheques_Ayer", exc_info=1)

    fechahoy = dt.datetime.now().strftime("%d/%m/%y")

    context.bot.send_message(
        chat_id=rumaos_cheques
        , text="INFORMES AUTOMÁTICOS " + fechahoy
    )

    context.bot.send_document(
        rumaos_cheques
        , open(find("Cheques_UENs.xlsx", ubic), "rb")
        , "Cheques_UENs.xlsx"
    )



##################################################
# DAILY REPORT of conciliations
##################################################

def envio_reporte_conciliaciones(context):

    logger.info("\n->Comenzando generación de informe conciliaciones<-")

    try:
        balanceYER()
        logger.info("Info balanceYER reseteado")
    except Exception as e:
        context.bot.send_message(id_Autorizados[0]
            , text="Error al resetear Info balanceYER"
        )
        logger.error("Error al resetear balanceYER", exc_info=1)

    fechahoy = dt.datetime.now().strftime("%d/%m/%y")

    context.bot.send_message(
        chat_id=rumaos_conciliaciones
        , text="INFORMES AUTOMÁTICOS " + fechahoy
    )

    context.bot.send_photo(
        rumaos_conciliaciones
        , open(find("balanceYER_Acumulado.png", ubic), "rb")
        , "Balance YER Acumulado"
    )

    context.bot.send_photo(
        rumaos_conciliaciones
        , open(find("balanceYER_Actual.png", ubic), "rb")
        , "Balance YER Mes en Curso"
    )



##################################################
# DAILY REPORT "CFO"
##################################################

# arqueos
# chequesSaldos
# bancosSaldos
# condicionDeudores
# usos_SGFin
# activosCorrientes
# pasivosCorrientes


def envio_reporte_CFO(context):

    logger.info("\n->Comenzando generación de informe CFO<-")

    try:
        arqueos()
        logger.info("Info Arqueos reseteado")
    except Exception as e:
        context.bot.send_message(id_Autorizados[0]
            , text="Error al resetear Info Arqueos"
        )
        logger.error("Error al resetear Arqueos", exc_info=1)

    try:
        chequesSaldos()
        logger.info("Info chequesSaldos reseteado")
    except Exception as e:
        context.bot.send_message(id_Autorizados[0]
            , text="Error al resetear Info chequesSaldos"
        )
        logger.error("Error al resetear chequesSaldos", exc_info=1)

    try:
        bancosSaldos()
        logger.info("Info BancosSaldos reseteado")
    except Exception as e:
        context.bot.send_message(id_Autorizados[0]
            , text="Error al resetear Info BancosSaldos"
        )
        logger.error("Error al resetear BancosSaldos", exc_info=1)

    try:
        condicionDeudores()
        logger.info("Info condicionDeudores reseteado")
    except Exception as e:
        context.bot.send_message(id_Autorizados[0]
            , text="Error al resetear Info condicionDeudores"
        )
        logger.error("Error al resetear condicionDeudores", exc_info=1)

    try:
        usos_SGFin()
        logger.info("Info Usos_SGFin reseteado")
    except Exception as e:
        context.bot.send_message(id_Autorizados[0]
            , text="Error al resetear Info Usos_SGFin"
        )
        logger.error("Error al resetear Usos_SGFin", exc_info=1)

    try:
        activosCorrientes()
        logger.info("Info activosCorrientes reseteado")
    except Exception as e:
        context.bot.send_message(id_Autorizados[0]
            , text="Error al resetear Info activosCorrientes"
        )
        logger.error("Error al resetear activosCorrientes", exc_info=1)

    try:
        pasivosCorrientes()
        logger.info("Info pasivosCorrientes reseteado")
    except Exception as e:
        context.bot.send_message(id_Autorizados[0]
            , text="Error al resetear Info pasivosCorrientes"
        )
        logger.error("Error al resetear pasivosCorrientes", exc_info=1)


    fechahoy = dt.datetime.now().strftime("%d/%m/%y")

    # Where to send the things
    chat_id = rumaos_info_CFO

    context.bot.send_message(
        chat_id=chat_id
        , text="INFORMES AUTOMÁTICOS "
            + fechahoy
            + "\n Datos actualizados al momento de emisión"
    )

    context.bot.send_photo(
        chat_id
        , open(find("Arqueos.png", ubic), "rb")
        , "Arqueos.png"
    )

    context.bot.send_photo(
        chat_id
        , open(find("ArqueosUSD.png", ubic), "rb")
        , "ArqueosUSD.png"
    )

    context.bot.send_photo(
        chat_id
        , open(find("ChequesSaldos.png", ubic), "rb")
        , "ChequesSaldos.png"
    )

    context.bot.send_photo(
        chat_id
        , open(find("BancosSaldos.png", ubic), "rb")
        , "BancosSaldos.png"
    )

    context.bot.send_photo(
        chat_id
        , open(find("Usos_SGFin.png", ubic), "rb")
        , "Usos_SGFin.png"
    )

    context.bot.send_photo(
        chat_id
        , open(find("activosCorrientes.png", ubic), "rb")
        , "activosCorrientes.png"
    )

    context.bot.send_photo(
        chat_id
        , open(find("PasivosCorrientes.png", ubic), "rb")
        , "PasivosCorrientes.png"
    )

    context.bot.send_photo(
        chat_id
        , open(find("DeudaComercial.png", ubic), "rb")
        , "DeudaComercial.png"
    )
    context.bot.send_message(
        chat_id=chat_id
        , text=
        """
        Se consideran "4-Morosos Graves" a aquellos deudores que no han 
        realizado compras en los últimos 60 días o si sus "Días Adeudados
        de Venta" superan los 60 días.
        """
    )
    context.bot.send_document(
        chat_id
        , open(find("ClientesDeudores.xlsx", ubic), "rb")
        , "ClientesDeudores.xlsx"
    )



##################################################
# WEEKLY REPORT "periferia"
# Channel Rumaos_Info_CFO and Rumaos_Info_Comercial
##################################################

# CFO
# perifericos (con margen)

# Comercial
# perifericos (sin margen)

def envio_reporte_periferia_semanal(context):

    chat_id = [rumaos_info_CFO, rumaos_info_com]

    logger.info("\n->Comenzando generación de informe semanal<-")

    try:
        perifericoSemanal()
        logger.info("Info Periferico Semanal reseteado")
    except Exception as e:
        context.bot.send_message(id_Autorizados[0]
            , text="Error al resetear Info Periferico Semanal"
        )
        logger.error("Error al resetear Periferico Semanal", exc_info=1)


    weekStart = (dt.date.today()-dt.timedelta(days=7)).strftime("%d/%m/%y")
    weekEnd = (dt.date.today()-dt.timedelta(days=1)).strftime("%d/%m/%y")

    # Send to both channels
    for ids in chat_id:
        context.bot.send_message(
            chat_id=ids
            , text="INFORMES AUTOMÁTICOS SEMANALES\n" 
                + "PERÍODO "
                + weekStart
                + " AL "
                + weekEnd
        )


    # Send to channel Rumaos_Info_CFO
    context.bot.send_photo(
            chat_id[0]
            , open(find("periferia_salonpan.png", ubic), "rb")
            , "Periferia Salón y Panadería"
        )
    
    context.bot.send_photo(
            chat_id[0]
            , open(find("periferia_cafesandwich.png", ubic), "rb")
            , "Periferia Cafetería y Sandwiches"
        )

    context.bot.send_photo(
            chat_id[0]
            , open(find("periferico_lubri.png", ubic), "rb")
            , "Periferia Lubriplaya"
        )
    
    try:
        context.bot.send_photo(
            chat_id[0]
            , open(find("periferico_grill.png", ubic), "rb")
            , "Periferia Grill"
        )
    except:
        context.bot.send_message(
            chat_id=chat_id[0]
            , text="Reporte Grill no generado. Ventas Grill aún no se \
                registran a través de módulo Servicompra"
        )


    # Send to channel Rumaos_Info_Comercial
    context.bot.send_photo(
            chat_id[1]
            , open(find("periferia_salonpan_comer.png", ubic), "rb")
            , "Periferia Salón y Panadería"
        )
    
    context.bot.send_photo(
            chat_id[1]
            , open(find("periferia_cafesandwich_comer.png", ubic), "rb")
            , "Periferia Cafetería y Sandwiches"
        )

    context.bot.send_photo(
            chat_id[1]
            , open(find("periferico_lubri_comer.png", ubic), "rb")
            , "Periferia Lubriplaya"
        )
    
    try:
        context.bot.send_photo(
            chat_id[1]
            , open(find("periferico_grill_comer.png", ubic), "rb")
            , "Periferia Grill"
        )
    except:
        context.bot.send_message(
            chat_id=chat_id[1]
            , text="Reporte Grill no generado. Ventas Grill aún no se \
                registran a través de módulo Servicompra"
        )



###########################################
####### RREPORTES CONTROL DE INFORMACION
############################################



def envio_reporte_ControlInfo(context):
    logger.info("\n->Comenzando generación de Control de Informacion<-")

    try:
        run_path(filePath_Info_Control_Info+"ControlDatosVentas.py")
        logger.info("Info Control Ventas resetado")
        run_path(filePath_Info_Control_Info+"ControlDespaPro.py")
        logger.info("Info Control Despachos reseteado")
    except Exception as e:
        context.bot.send_message(id_Autorizados[0]
            , text="Error al resetear Control de Informacion"
        )
        logger.error("Error al resetear Control de Informacion", exc_info=1)

    fechahoy = dt.datetime.now().strftime("%d/%m/%y")
    chat_id = rumaos_Control_info
   
    context.bot.send_message(
        chat_id
        , text="INFORMES Control De Informacion"
        + fechahoy 
        + "\n Datos relevados hasta ayer"
    )

    context.bot.send_photo(
        chat_id
        , open(filePath_Info_Control_Info+"ControlVtas.png", "rb")
        , "Control Informacion Ventas"
    )
    context.bot.send_photo(
        chat_id
        , open(filePath_Info_Control_Info+"ControlDespa.png", "rb")
        , "Control Informacion Despachos"
    )


#############################
##  Envio Reportes Ejecucion Presupuestaria         #############################
##############################



def envio_reporte_Presupuesto(context):
    logger.info("\n->Comenzando generación de informe comercial<-")

    try:
        run_path(filePath_Info_Presu_Gas+"InfoPresupuestoGas.py")
        logger.info("Info Ejecucion Presupuestaria reseteado")
        run_path(filePath_Info_Presu_Gas+"InfoPresupuestoGasTURNOS.py")
        logger.info("Info Desvio Turnos Reseteado")
        run_path(filePath_Info_Presu_Gas+"Mix.py")
        logger.info("Mix reseteado")
    except Exception as e:
        context.bot.send_message(id_Autorizados
            , text="Error al resetear Info Ejecucion Presupuestaria"
        )
        logger.error("Error al resetear Info Ejecucion Presupuestaria", exc_info=1)
    try:
        run_path(filePath_Info_Pen_Salon +"VtasSalonPresupuesto.py")
        logger.info("Info Ejecucion Presupuestaria ventas salon")
    except Exception as e:
        context.bot.send_message(id_Autorizados
            , text="Error al resetear Info Ejecucion Presupuestaria ventas salon"
        )
        logger.error("Error al resetear Info Ejecucion Presupuestaria ventas salon", exc_info=1)

    fechahoy = dt.datetime.now().strftime("%d/%m/%y")
    chat_id = rumaos_Info_EPresu
   
    context.bot.send_message(
        chat_id
        , text="INFORMES AUTOMÁTICOS Ejecucion Presupuestos "
        + fechahoy 
        + "\n Datos relevados hasta ayer"
    )

    context.bot.send_photo(
        chat_id
        , open(filePath_Info_Presu_Gas+"Info_Presupuesto_GNC.png", "rb")
        , "Info Ejecucion Presupuestaria GNC"
    )
    context.bot.send_photo(
        chat_id
        , open(filePath_Info_Presu_Gas+"Info_Desvio_Turnos.png", "rb")
        , "Info Desvio Presupuestario Penetracion GNC Turnos"
    )
    context.bot.send_photo(
        chat_id
        , open(filePath_Info_Pen_Pan+"Info_PenetracionPan.png", "rb")
        , "Info Ejecucion Panaderia Presupuestado Diario"
    )
    context.bot.send_photo(
        chat_id
        , open(filePath_Info_Pen_Lubri+"Info_PenetracionLubri_Diario.png", "rb")
        , "Info Ejecucion Lubricantes Presupuestado Diario"
    )
    context.bot.send_photo(
        chat_id
        , open(filePath_Info_Presu_Gas+"Info_MIX.png", "rb")
        , "MIX G3/G2 Ejecucion Presupuestaria"
    )
    context.bot.send_photo(
        chat_id
        , open(filePath_Info_Pen_Salon +"PresupuestadoSalon.png", "rb")
        , "Info Desvio Presupuestario Ventas Salon"
    )

#############################################
############## REPORTE PERIFERIA ############
#############################################

def envio_reporte_Periferia(context):
    logger.info("\n->Comenzando generación de informe Periferia<-")

    try:
        run_path(filePath_Info_Pen_Pan+"Info_Penetracion_Pan.py")
        logger.info("Info Penetracion Panaderia Reseteado")
        run_path(filePath_Info_Pen_Pan+"MermasPanaderia.py")
        logger.info("Mermas Panaderia Reseteado")

    except Exception as e:
        context.bot.send_message(id_Autorizados[0]
            , text="Error al resetear Info Penetracion Panaderia"
        )
        logger.error("Error al resetear Info Penetracion Panaderia", exc_info=1)

    try:
        run_path(filePath_Info_Pen_Lubri+"PenetracionLubri.py")
        logger.info("Info Penetracion Lubricantes Reseteado")
    except Exception as e:
        context.bot.send_message(id_Autorizados[0]
            , text="Error al resetear Info Penetracion Lubricantes"
        )
        logger.error("Error al resetear Info Penetracion Lubricantes", exc_info=1)

    try:
        run_path(filePath_Info_Pen_Salon+"PromedioVtasSalon.py")
        logger.info("Info Ticket Promedio Ventas Salon")
        run_path(filePath_Info_Pen_Salon+"PromedioVtasSalonIntermensual.py")
        logger.info("Info Ventas Salon")
    except Exception as e:
        context.bot.send_message(id_Autorizados[0]
            , text="Error al resetear Info Ticket Promedio Ventas Salon"
        )
        logger.error("Error al resetear Info Ticket Promedio Ventas Salon", exc_info=1)

    fechahoy = dt.datetime.now().strftime("%d/%m/%y")
    chat_id = rumaos_Info_Periferia
   
    context.bot.send_message(
        chat_id
        , text="INFORMES AUTOMÁTICOS Periferia "
        + fechahoy 
        + "\n Datos relevados hasta ayer"
    )

    context.bot.send_photo(
        chat_id
        , open(filePath_Info_Pen_Pan+"Info_Penetracion_Pan.png", "rb")
        , "Info Penetracion Panaderia"
    )
    context.bot.send_photo(
        chat_id
        , open(filePath_Info_Pen_Lubri+"Info_Penetracion_Lubri.png", "rb")
        , "Info Penetracion Lubricantes"
    )
    context.bot.send_photo(
        chat_id
        , open(filePath_Info_Pen_Salon+"Info_Promedio_Vtas.png", "rb")
        , "Info Ticket Salon Promedio"
    )
    context.bot.send_photo(
        chat_id
        , open(filePath_Info_Pen_Pan+"MermasPanaderia.png", "rb")
        , "Mermas Panaderia"
    )
    context.bot.send_photo(
        chat_id
        , open(filePath_Info_Pen_Salon+"Info_Promedio_VtasIntermensual.png", "rb")
        , "Info Ventas de Salon"
    )


####################################################
######### REPORTE  MARGENES  SALON #################
####################################################

def envio_reporte_MargenesSalon(context):
    logger.info("\n->Comenzando generación de informe Margenes Salon<-")

    try:
        run_path(filePath_Info_Margenes+"MargenSalonAZ.py")
        logger.info("Info Margen Salon Azcuenaga Reseteado")
        run_path(filePath_Info_Margenes+"MargenSalonLM.py")
        logger.info("Info Margen Salon LAMADRID Reseteado")
        run_path(filePath_Info_Margenes+"MargenSalonM2.py")
        logger.info("Info Margen Salon Mercado 2 Reseteado")
        run_path(filePath_Info_Margenes+"MargenSalonP1.py")
        logger.info("Info Margen Salon Perdriel 1 Reseteado")
        run_path(filePath_Info_Margenes+"MargenSalonP2.py")
        logger.info("Info Margen Salon Perdriel 2 Reseteado")
        run_path(filePath_Info_Margenes+"MargenSalonPO.py")
        logger.info("Info Margen Salon Puente Olive Reseteado")
        run_path(filePath_Info_Margenes+"MargenSalonSJ.py")
        logger.info("Info Margen Salon San Jose Reseteado")
        run_path(filePath_Info_Margenes+"MargenSalonXS.py")
        logger.info("Info Margen Salon Xpress Reseteado")
        run_path(filePath_Info_Margenes+"MargenTotales.py")
        logger.info("Info Margen Salon Totales Reseteado")
    except Exception as e:
        context.bot.send_message(id_Autorizados[0]
            , text="Error al resetear Info Margenes Salon"
        )
        logger.error("Error al resetear Info Margenes Salon", exc_info=1)

    fechahoy = dt.datetime.now().strftime("%d/%m/%y")
    chat_id = rumaos_Margenes
   
    context.bot.send_message(
        chat_id
        , text="INFORMES AUTOMÁTICOS Margenes Salon"
        + fechahoy 
        + "\n Datos relevados hasta ayer"
    )
    context.bot.send_photo(
        chat_id
        , open(filePath_Info_Margenes+"ControlVtasAZ.png", "rb")
        , "Info Margenes Salon Azcuenaga"
    )
    context.bot.send_photo(
        chat_id
        , open(filePath_Info_Margenes+"ControlVtasLM.png", "rb")
        , "Info Margenes Salon Lamadrid"
    )

    context.bot.send_photo(
        chat_id
        , open(filePath_Info_Margenes+"ControlVtasP1.png", "rb")
        , "Info Margenes Salon Perdriel"
    )
    context.bot.send_photo(
        chat_id
        , open(filePath_Info_Margenes+"ControlVtasP2.png", "rb")
        , "Info Margenes Salon Perdriel 2"
    )
    context.bot.send_photo(
        chat_id
        , open(filePath_Info_Margenes+"ControlVtasSJ.png", "rb")
        , "Info Margenes Salon San Jose"
    )
    context.bot.send_photo(
        chat_id
        , open(filePath_Info_Margenes+"ControlVtasXS.png", "rb")
        , "Info Margenes Salon Xpress"
    )
    context.bot.send_photo(
        chat_id
        , open(filePath_Info_Margenes+"ControlVtasPO.png", "rb")
        , "Info Margenes Salon Puente Olive"
    )
    context.bot.send_photo(
        chat_id
        , open(filePath_Info_Margenes+"MARGENESTOTALES.png", "rb")
        , "Info Margenes Salon Totales"
    )

####################################################
######### REPORTE  MARGENES  PLAYA #################
####################################################


def envio_reporte_MargenesPlaya(context):
    logger.info("\n->Comenzando generación de informe Margenes Playa<-")

    try:
        run_path(filePath_Info_MargenesGasoleos+"MargenGasoleos.py")
        logger.info("Info Margen Gasoleos")
        run_path(filePath_Info_MargenesGasoleos+"MargenGNC.py")
        logger.info("Info Margen GNC")
    except Exception as e:
        context.bot.send_message(id_Autorizados[0]
            , text="Error al resetear Info Margenes Playa"
        )
        logger.error("Error al resetear Info Margenes Playa", exc_info=1)
    
    fechahoy = dt.datetime.now().strftime("%d/%m/%y")
    chat_id = rumaos_Margenes
   
    context.bot.send_message(
        chat_id
        , text="INFORMES AUTOMÁTICOS Margenes Playa "
        + fechahoy 
        + "\n Datos relevados hasta ayer"
    )
    context.bot.send_photo(
        chat_id
        , open(filePath_Info_MargenesGasoleos+"Margenes_Gasoleos.png", "rb")
        , "Info Margenes Gasoleos"
    )
    context.bot.send_photo(
        chat_id
        , open(filePath_Info_MargenesGasoleos+"Margenes_GNC.png", "rb")
        , "Info Margenes GNC"
    )


####################################################
######### REPORTE  VOLUMEN #################
####################################################


def envio_reporte_Descargas(context):
    logger.info("\n->Comenzando generación de informe Volumenes<-")

    try:
        run_path(filePath_Info_Descargas+"InformeVolumenStock.py")
        logger.info("Info Margen Gasoleos")
    except Exception as e:
        context.bot.send_message(id_Autorizados[0]
            , text="Error al resetear Info Volumen"
        )
        logger.error("Error al resetear Info Volumen", exc_info=1)
    
    fechahoy = dt.datetime.now().strftime("%d/%m/%y")
    chat_id = rumaos_Descargas
   
    context.bot.send_message(
        chat_id
        , text="INFORMES AUTOMÁTICOS Margenes Gasoleos"
        + fechahoy 
        + "\n Datos relevados hasta ayer"
    )
    context.bot.send_document(
            chat_id
        , open(filePath_Info_Descargas+"volumen.xlsx", "rb")
        , "Info Volumen.xlsx"
    )
    context.bot.send_photo(
        chat_id
        , open(filePath_Info_Descargas+"VolumEuDapsa.png", "rb")
        , "Info Volumen EU Dapsa"
    )
    context.bot.send_photo(
        chat_id
        , open(filePath_Info_Descargas+"VolumGoDapsa.png", "rb")
        , "Info Volumen GO Dapsa"
    )
    context.bot.send_photo(
        chat_id
        , open(filePath_Info_Descargas+"VolumNsDapsa.png", "rb")
        , "Info Volumen NS Dapsa"
    )
    context.bot.send_photo(
        chat_id
        , open(filePath_Info_Descargas+"VolumEuYPF.png", "rb")
        , "Info Volumen EU YPF"
    )
    context.bot.send_photo(
        chat_id
        , open(filePath_Info_Descargas+"VolumGoYPF.png", "rb")
        , "Info Volumen GO YPF"
    )
    context.bot.send_photo(
        chat_id
        , open(filePath_Info_Descargas+"VolumNsYPF.png", "rb")
        , "Info Volumen NS YPF"
    )
    context.bot.send_photo(
        chat_id
        , open(filePath_Info_Descargas+"VolumNuYPF.png", "rb")
        , "Info Volumen NU YPF"
    )
##################################################
# WEEKLY REPORT channel Rumaos_Info
##################################################

def envio_reporte_semanal(context):

    logger.info("\n->Comenzando generación de informe semanal<-")

    try:
        penetracionRMSemanal()
        logger.info("Info penetracionRMSemanal reseteado")
    except Exception as e:
        context.bot.send_message(id_Autorizados[0]
            , text="Error al resetear Info penetracionRMSemanal"
        )
        logger.error("Error al resetear penetracionRMSemanal", exc_info=1)

    try:
        redControlSemanal()
        logger.info("Info redControlSemanal reseteado")
    except Exception as e:
        context.bot.send_message(id_Autorizados[0]
            , text="Error al resetear Info redControlSemanal"
        )
        logger.error("Error al resetear redControlSemanal", exc_info=1)

    try:
        run_path(filePath_InfoSemanal+"InfoVtaLiqProy.py")
        logger.info("Info vtaSemanalProy_Liq_GNC reseteado")
    except Exception as e:
        context.bot.send_message(id_Autorizados[0]
            , text="Error al resetear Info vtaSemanalProy_Liq_GNC"
        )
        logger.error("Error al resetear vtaSemanalProy_Liq_GNC", exc_info=1)

    try:
        vtaProyGranClient()
        logger.info("Info vtaProyGranClient reseteado")
    except Exception as e:
        context.bot.send_message(id_Autorizados[0]
            , text="Error al resetear Info vtaProyGranClient"
        )
        logger.error("Error al resetear vtaProyGranClient", exc_info=1)



    weekStart = (dt.date.today()-dt.timedelta(days=7)).strftime("%d/%m/%y")
    weekEnd = (dt.date.today()-dt.timedelta(days=1)).strftime("%d/%m/%y")


    chat_id = rumaos_info

    context.bot.send_message(
        chat_id=chat_id
        , text="INFORMES AUTOMÁTICOS SEMANALES\n" 
            + "PERÍODO "
            + weekStart
            + " AL "
            + weekEnd
    )

    context.bot.send_photo(
            chat_id
            , open(find("Info_RedControlLiq_Semanal.png", ubic), "rb")
            , "Red Control Líquido"
        )

    context.bot.send_document(
            chat_id
            , open(find("Grandes_Clientes_Baja_Consumo.pdf", ubic), "rb")
            , "Grandes_Clientes_Baja_Consumo.pdf"
        )

    context.bot.send_photo(
            chat_id
            , open(find("VtaGASOLEOS_Semanal.png", ubic), "rb")
            , "Venta Gasóleos Proyectado"
        )
    context.bot.send_photo(
            chat_id
            , open(find("VtaNAFTAS_Semanal.png", ubic), "rb")
            , "Venta Naftas Proyectado"
        )

    context.bot.send_photo(
            chat_id
            , open(find("Info_VtaGNC_Semanal.png", ubic), "rb")
            , "Venta de GNC Proyectado"
        )

    context.bot.send_photo(
            chat_id
            , open(find("Info_Penetración_Semanal.png", ubic), "rb")
            , "Penetración RedMás Semanal"
        )
    



##################################################
# CONNECTION KEEP ALIVE
##################################################

def keepAlive(context):
    '''
    Will send a message to an invalid Telegram ID to 
    keep alive the connection
    '''
    try:
        context.bot.send_message(0, text="Keep Alive")
    except:
        pass
    


##################################################
# MAIN FUNCTION
##################################################

def main() -> None:
    """Run the bot."""
    defaults = Defaults(tzinfo=argTime)

    # Create the Updater and pass your bot token.
    updater = Updater(token, defaults=defaults)
    dispatcher = updater.dispatcher
    dispatcher.add_handler(CommandHandler(["start", "informes"], start
        , run_async=True))
    dispatcher.add_handler(CallbackQueryHandler(button
        , run_async=True))
    dispatcher.add_handler(CommandHandler(["resend", "reenviar"], resend
        , run_async=True))
    dispatcher.add_handler(CommandHandler(["help","ayuda"], help_command
        , run_async=True))
    dispatcher.add_handler(CommandHandler(["help_dev","ayuda_dev"]
        , help_Dev_command
        , run_async=True))
    dispatcher.add_handler(CommandHandler(["forzar_envio"], forzar_envio))
    dispatcher.add_handler(CommandHandler(["set"], set_envioDiario))
    dispatcher.add_handler(CommandHandler(["unset"], unset))
    # dispatcher.add_handler(CommandHandler(["tareas"], tareas))

    # Listening for wrong or unknown commands
    # MUST BE AT THE END OF THE HANDLERS!!!!!
    unknown_handler = MessageHandler(Filters.command, unknown
        , run_async=True)
    dispatcher.add_handler(unknown_handler)


    ############# TASKs ############

    # updater.job_queue.run_repeating(callback_minute, interval=60, first=10)
    # updater.job_queue.run_once(envio_reporte_cheques, 15)

    # Daily IVO

    updater.job_queue.run_daily(reportes_ivo_diario_Financieros
        , dt.time(17,50,0,tzinfo=argTime) 
        , days=(0, 1, 2, 3, 4)
        , name="info_diario"
    )
    updater.job_queue.run_daily(envio_Check_Finanzas
        , dt.time(18,0,0,tzinfo=argTime) 
        , days=(0, 1, 2, 3, 4)
        , name="check_Info"
    )
    updater.job_queue.run_daily(reportes_ivo_diario_InfoGlobal
        , dt.time(11,10,0,tzinfo=argTime) 
        , name="info_diario"
    )
    # Daily Empleados
    updater.job_queue.run_daily(envio_reporte_MBCGerencial_Diario
        , dt.time(12,30,0,tzinfo=argTime)
    )
    updater.job_queue.run_daily(envio_reporte_Abastecimiento
        , dt.time(6,45,0,tzinfo=argTime)
    )
    '''
    updater.job_queue.run_daily(actualizacion_Info_Sheet
        , dt.time(8,30,0,tzinfo=argTime)
    )
    '''
    updater.job_queue.run_daily(envio_reporte_ControlInfo
        , dt.time(9,0,0,tzinfo=argTime)
    )
    updater.job_queue.run_daily(envio_reporte_ComercialRetail
        , dt.time(10,35,0,tzinfo=argTime)
    )
    updater.job_queue.run_daily(envio_reporte_Grandes_Clientes
        , dt.time(10,40,0,tzinfo=argTime)
    )
    '''
    updater.job_queue.run_daily(actualizacion_Info_Sheet
        , dt.time(11,0,0,tzinfo=argTime)
    )
    '''
    # Monday to Friday
    updater.job_queue.run_daily(envio_reporte_cheques
        , dt.time(8,0,0,tzinfo=argTime)
        , days=(0, 1, 2, 3, 4)
        , name="info_cheques"
    )
    updater.job_queue.run_daily(envio_reporte_conciliaciones
        , dt.time(16,30,0,tzinfo=argTime)
        , days=(0, 1, 2, 3, 4)
        , name="info_conciliaciones"
    )

    # Weekly IVO
    updater.job_queue.run_daily(reportes_ivo_semanal_InfoGlobal
        , dt.time(13,59,0,tzinfo=argTime)
        , days=[4]
        , name="info_Global"
    )
    updater.job_queue.run_daily(reportes_ivo_semanal_Metricas
        , dt.time(14,15,tzinfo=argTime)
        , days=[4]
        , name="info_Metricas"
    )
    updater.job_queue.run_daily(reportes_ivo_diario_Metricas
        , dt.time(14,25,0,tzinfo=argTime)
        , days=[4]
        , name="info_comercial"
    )
    updater.job_queue.run_daily(reportes_ivo_Presupuestos
        , dt.time(13,40,0,tzinfo=argTime)
        , days=[4] 
        , name="info_Presupuestos"
    )
    updater.job_queue.run_daily(envio_reporte_MBC_IVO
        , dt.time(14,0,0,tzinfo=argTime)
        , days=[0]
        , name="info_MBC"
    )
    ''' updater.job_queue.run_daily(envio_DB_Eduardo
        , dt.time(14,50,0,tzinfo=argTime)
        , days=[0]
        , name="Dashboard_Eduardo"
    ) '''
    # Weekly Empleados
    updater.job_queue.run_daily(envio_reporte_Grandes_Clientes_semanal
        , dt.time(12,45,0,tzinfo=argTime)
        , days=[4]
        , name="info_semanal"
    )

    updater.job_queue.run_daily(envio_reporte_ComercialRetail_Semanal
        , dt.time(12,56,0,tzinfo=argTime)
        , days=[4]
        , name="info_semanal"
    )
    updater.job_queue.run_daily(envio_reporte_MBCGerencial
        , dt.time(13,9,0,tzinfo=argTime)
        , days=[4]
        , name="info_semanal"
    )
    updater.job_queue.run_daily(envio_reporte_MBCOperativo
        , dt.time(12,12,0,tzinfo=argTime)
        , days=[4]
        , name="info_semanal"
    )
    #updater.job_queue.run_daily(envio_reporte_periferia_semanal
    #    , dt.time(13,0,0,tzinfo=argTime)
    #    , days=[4]
    #    , name="info_periferia_semanal"
    #)

    # Keep Alive Task
    updater.job_queue.run_repeating(keepAlive, interval=210, first=10)

    ############# /TASKs ############

    # Start the Bot
    updater.start_polling()

    # Run the bot until the user presses Ctrl-C or the process receives SIGINT,
    # SIGTERM or SIGABRT
    updater.idle()





if __name__ == "__main__":
    main()