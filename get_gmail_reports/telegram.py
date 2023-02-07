import requests
from datetime import datetime


def telegram_bot_sendtext(bot_message):
    bot_token = '*********'
    bot_chatID = '********'
    date_time = datetime.now()
    date_time = date_time.strftime('%d.%m.%y %H:%M:%S')
    send_text = 'https://api.telegram.org/bot' + bot_token + '/sendMessage?chat_id=' + bot_chatID + '&text=' \
                + bot_message + ' at ' + date_time
    requests.get(send_text)
    print('telegram text', bot_message)
