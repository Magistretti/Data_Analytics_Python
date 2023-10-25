##########################
#
#    RUMAOS_INFO_BOT
#
##########################

import os
import pathlib
ubic = str(pathlib.Path(__file__).parent)+"\\"

from DatosTelegram import id_Autorizados, bot_token, testbot_token
from DatosTelegram import testrumaos, rumaos_info, rumaos_info_com
from DatosTelegram import rumaos_cheques, rumaos_info_CFO
from DatosTelegram import lapuchesky, rumaos_conciliaciones
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
from InfoSemanal.InfoVtaLiqProy import vtaSemanalProy_Liq_GNC
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
            InlineKeyboardButton("Info Diario"
                , callback_data="Info Diario")
            , InlineKeyboardButton("Info Comercial"
                , callback_data="Info Comercial")
        ]
        , [
            InlineKeyboardButton("Info CFO"
                , callback_data="Info CFO")
            , InlineKeyboardButton("Info Semanal"
                , callback_data="Info Semanal")
        ]
        , [
            InlineKeyboardButton("Info Cheques"
                , callback_data="Info Cheques")
            , InlineKeyboardButton("Info Lapuchesky"
                , callback_data="Info Lapuchesky")
        ]
        , [
            InlineKeyboardButton("Info Conciliaciones"
                , callback_data="Info Conciliaciones")
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
            query.bot.send_photo(update.effective_chat.id
                , open(find("DeudaComercial.png", ubic), "rb")
            )
            query.bot.send_photo(update.effective_chat.id
                , open(find("DeudaExcedida.png", ubic), "rb")
            )
            query.bot.send_photo(update.effective_chat.id
                , open(find("DeudaMorosa.png", ubic), "rb")
            )
            query.bot.send_photo(update.effective_chat.id
                , open(find("DeudaMorosaGrave.png", ubic), "rb")
            )
            query.bot.send_document(update.effective_chat.id
                , open(find("ClientesDeudores.xlsx", ubic), "rb")
                , "ClientesDeudores.xlsx"
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


    ####################################################
    # CallbackQuery responses for /resend /reenviar
    ####################################################

    # INFO DIARIO
    elif query.data == "Info Diario":
        try:
            query.bot.send_message(update.effective_chat.id
                , text="Activando Reporte Diario en 10 segundos")

            context.job_queue.run_once(envio_reporte_ivo, 10)

        except Exception as e:
            query.bot.send_message(update.effective_chat.id
                , text="Algo falló, revisar consola")
            logger.error("", exc_info=1)

    # INFO COMERCIAL
    elif query.data == "Info Comercial":
        try:
            query.bot.send_message(update.effective_chat.id
                , text="Activando Reporte Comercial en 10 segundos")

            context.job_queue.run_once(envio_reporte_comercial, 10)

        except Exception as e:  
            query.bot.send_message(update.effective_chat.id
                , text="Algo falló, revisar consola")
            logger.error("", exc_info=1)

    # INFO CFO
    elif query.data == "Info CFO":
        try:
            query.bot.send_message(update.effective_chat.id
                , text="Activando Reporte CFO en 10 segundos")

            context.job_queue.run_once(envio_reporte_CFO, 10)

        except Exception as e:  
            query.bot.send_message(update.effective_chat.id
                , text="Algo falló, revisar consola")
            logger.error("", exc_info=1)
    
    # INFO SEMANAL
    elif query.data == "Info Semanal":
        try:
            query.bot.send_message(update.effective_chat.id
                , text="Activando Reporte Periferia Semanal en 10seg y Reporte Semanal en 1min")

            context.job_queue.run_once(envio_reporte_semanal, 60)
            context.job_queue.run_once(envio_reporte_periferia_semanal, 10)

        except Exception as e:  
            query.bot.send_message(update.effective_chat.id
                , text="Algo falló, revisar consola")
            logger.error("", exc_info=1)

    # INFO CHEQUES
    elif query.data == "Info cheques":
        try:
            query.bot.send_message(update.effective_chat.id
                , text="Activando Reporte Cheques en 10 segundos")

            context.job_queue.run_once(envio_reporte_cheques, 10)

        except Exception as e:  
            query.bot.send_message(update.effective_chat.id
                , text="Algo falló, revisar consola")
            logger.error("", exc_info=1)
    
    # INFO LAPUCHESKY
    elif query.data == "Info Lapuchesky":
        try:
            query.bot.send_message(update.effective_chat.id
                , text="Activando Reporte Lapuchesky en 10 segundos")

            context.job_queue.run_once(envio_reporte_lapuchesky, 10)

        except Exception as e:  
            query.bot.send_message(update.effective_chat.id
                , text="Algo falló, revisar consola")
            logger.error("", exc_info=1)
    
    # INFO CONCILIACIONES
    elif query.data == "Info Conciliaciones":
        try:
            query.bot.send_message(update.effective_chat.id
                , text="Activando Reporte Conciliaciones en 10 segundos")

            context.job_queue.run_once(envio_reporte_conciliaciones, 10)

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
        vtaSemanalProy_Liq_GNC()
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
            , open(find("Info_VtaLiquido_Semanal.png", ubic), "rb")
            , "Venta de Líquidos Proyectado"
        )

    context.bot.send_photo(
            chat_id
            , open(find("Info_GrupoLiq_Semanal.png", ubic), "rb")
            , "Venta Gasóleos/Naftas Proyectado"
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

    # Daily
    updater.job_queue.run_daily(envio_reporte_ivo
        , dt.time(11,0,0,tzinfo=argTime) 
        , name="info_diario"
    )
    updater.job_queue.run_daily(envio_reporte_comercial
        , dt.time(11,5,0,tzinfo=argTime) 
        , name="info_comercial"
    )
    updater.job_queue.run_daily(envio_reporte_lapuchesky
        , dt.time(11,15,0,tzinfo=argTime) 
        , name="info_lapuchesky"
    )

    # Monday to Friday
    updater.job_queue.run_daily(envio_reporte_cheques
        , dt.time(8,0,0,tzinfo=argTime)
        , days=(0, 1, 2, 3, 4)
        , name="info_cheques"
    )
    updater.job_queue.run_daily(envio_reporte_conciliaciones
        , dt.time(17,15,0,tzinfo=argTime)
        , days=(0, 1, 2, 3, 4)
        , name="info_conciliaciones"
    )
    updater.job_queue.run_daily(envio_reporte_CFO
        , dt.time(17,30,0,tzinfo=argTime)
        , days=(0, 1, 2, 3, 4)
        , name="info_CFO"
    )

    # Weekly
    updater.job_queue.run_daily(envio_reporte_semanal
        , dt.time(12,0,0,tzinfo=argTime)
        , days=[4]
        , name="info_semanal"
    )
    updater.job_queue.run_daily(envio_reporte_periferia_semanal
        , dt.time(13,0,0,tzinfo=argTime)
        , days=[4]
        , name="info_periferia_semanal"
    )

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