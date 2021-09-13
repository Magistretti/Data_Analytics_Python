##########################
#
#    RUMAOS_INFO_BOT
#
##########################

from DatosTelegram import id_Autorizados, bot_token, testbot_token
from runpy import run_path
import logging

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram import ChatAction
from telegram.ext import Updater, CommandHandler
from telegram.ext import MessageHandler, Filters 
from telegram.ext import CallbackQueryHandler, CallbackContext

from functools import wraps


#####//////////////######
# Where to find the report image files of:
# "Info_Morosos.png"
# "Info_VolumenVentas.png"
filePath_Info_Morosos = "C:\Informes\InfoMorosos\\"
filePath_InfoVtaComb = "C:\Informes\InfoVtaCombustibles\\"
######//////////////######


logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        , level=logging.INFO
)
logger = logging.getLogger(__name__)


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


def send_action(action):
    """
        Create decorator for handlers that sends "action" while processing
        func command.
            action: examples are ChatAction.TYPING or ChatAction.UPLOAD_PHOTO
    """
    def decorator(func):
        @wraps(func)
        def command_func(update, context, *args, **kwargs):
            context.bot.send_chat_action(chat_id=update.effective_message.chat_id, action=action)
            return func(update, context,  *args, **kwargs)
        return command_func
    
    return decorator


@restricted # NOTE: Access restricted to "start" function!
def start(update, context) -> None:
    # This will create inline buttons
    keyboard = [
        [
            InlineKeyboardButton("Info Morosos"
                , callback_data="Info Morosos")
            , InlineKeyboardButton("Volumen Ventas Ayer"
                , callback_data="Volumen Ventas Ayer")
        ]
        , [
            InlineKeyboardButton("Reset Info Morosos"
                , callback_data="Reset Info Morosos")
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


@send_action(ChatAction.UPLOAD_PHOTO)
def button(update, context) -> None:
    """Parses the CallbackQuery and updates the message text."""
    query = update.callback_query

    # CallbackQueries need to be answered to avoid errors, even with empty
    query.answer()

    query.edit_message_text(text=f"Opción Seleccionada: {query.data}")

    if query.data == "Info Morosos":
        query.bot.send_photo(update.effective_chat.id
            , open(filePath_Info_Morosos+"Info_Morosos.png"
                , "rb"))

    elif query.data == "Volumen Ventas Ayer":
        try:
            run_path(filePath_InfoVtaComb+"TotalesPorCombustible.py")
            query.bot.send_photo(update.effective_chat.id
                , open(filePath_InfoVtaComb+"Info_VolumenVentas.png"
                , "rb"
                )
            )
        except:  
            # The module use an exit exception, this will catch it and every 
            # other exception
            query.bot.send_message(update.effective_chat.id
                , text="Algo falló, revisar consola")

    elif query.data == "Salir":
        query.bot.send_message(update.effective_chat.id
            , text="Consulta Finalizada")

    elif query.data == "Reset Info Morosos":
        try:
            run_path(filePath_Info_Morosos+"DeudaClientes.py")
            query.bot.send_message(update.effective_chat.id
                , text="Informe reseteado")
        except:
            query.bot.send_message(update.effective_chat.id
                , text="Algo falló, revisar consola")

    else:
        query.bot.send_message(update.effective_chat.id
            , text="Algo no salió bien...")


# Show info on how to use the bot
def help_command(update, context) -> None:
    update.message.reply_text("Comandos:\n"
        +"/start o /informes para iniciar consulta.\n"
        +"/help o /ayuda para mostrar esta información."
    )


# Show a message when the bot receive an unknown command
def unknown(update, context):
    context.bot.send_message(chat_id=update.effective_chat.id
    , text="Disculpa, no entendí ese comando\n"
        +"¿Necesitas /ayuda?")


def main() -> None:
    """Run the bot."""
    # Create the Updater and pass your bot token.
    updater = Updater(bot_token)
    dispatcher = updater.dispatcher
    dispatcher.add_handler(CommandHandler(["start", "informes"], start))
    dispatcher.add_handler(CallbackQueryHandler(button))
    dispatcher.add_handler(CommandHandler(["help","ayuda"], help_command))

    # Listening for wrong or unknown commands
    # MUST BE AT THE END OF THE HANDLERS!!!!!
    unknown_handler = MessageHandler(Filters.command, unknown)
    dispatcher.add_handler(unknown_handler)

    # Start the Bot
    updater.start_polling()

    # Run the bot until the user presses Ctrl-C or the process receives SIGINT,
    # SIGTERM or SIGABRT
    updater.idle()


if __name__ == "__main__":
    main()