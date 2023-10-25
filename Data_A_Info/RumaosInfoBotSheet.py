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
            InlineKeyboardButton("Actualizar Sheet"
                , callback_data="Actualizar Sheet")
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
    if query.data == "Actualizar Sheet":
        try:
            actualizacion_Info_Sheet()
            query.bot.send_message(update.effective_chat.id
                                   , text="Actualizacion Terminada !!")

        except Exception as e:
            query.bot.send_message(update.effective_chat.id
                                   , text="Algo falló, revisar consola")
            logger.error("", exc_info=1)


    ####################################################
    # CallbackQuery responses for /start /informes
    ####################################################


    ##################################################
    # Generic CallbacksQuery Responses
    ##################################################


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
    dispatcher.add_handler(CommandHandler(["help","ayuda"], help_command
        , run_async=True))
    dispatcher.add_handler(CommandHandler(["help_dev","ayuda_dev"]
        , help_Dev_command
        , run_async=True))

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



    updater.job_queue.run_daily(actualizacion_Info_Sheet
        , dt.time(8,30,0,tzinfo=argTime)
    )



    updater.job_queue.run_daily(actualizacion_Info_Sheet
        , dt.time(11,0,0,tzinfo=argTime)
    )

    # Monday to Friday

    ''' updater.job_queue.run_daily(envio_DB_Eduardo
        , dt.time(14,50,0,tzinfo=argTime)
        , days=[0]
        , name="Dashboard_Eduardo"
    ) '''
    # Weekly Empleados

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