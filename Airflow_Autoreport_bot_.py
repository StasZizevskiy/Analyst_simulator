#получаем доступ к боту
import telegram
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import io
import pandas as pd
import pandahouse as ph
from datetime import datetime, timedelta
import os

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 's_zizevskij',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 3, 24),
}

# Интервал запуска DAG
schedule_interval = '0 11 * * *' #здесь стоит каждый день в 11 часов

my_token = '7680117939:AAFmoIXFfxF8ruzwtiZxg37OIH9jTpBZCHw' # тут нужно заменить на токен вашего бота
chat_id = -938659451 #сюда пойдут сообщения бота
bot = telegram.Bot(token=my_token) # получаем доступ

connection = {'host': 'https://clickhouse.lab.karpov.courses',
                      'database':'simulator_20250220',
                      'user':'student', 
                      'password':'dpo_python_2020'
                     }

def get_dates(): #функция для обновления даты
    date_format = '%d-%m-%Y'
    today = datetime.now()
    yesterday = today - timedelta(days=1)
    return yesterday.strftime(date_format)

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def KC_auto_report_zas():
    
    @task()
    def extract_df():
        q = """
        select
            toDate(time) as date,
            count(distinct user_id) as DAU,
            countIf(user_id, action = 'view') as views,
            countIf(user_id, action = 'like') as likes,
            round(likes/views, 3) as CTR
        from {db}.feed_actions
        where toDate(time) between yesterday() - 6 and yesterday()
        group by date
        """
        df = ph.read_clickhouse(q, connection=connection)

        return df
        
    @task()
    def send_message(df):
        df_yesterday = df[6:].reset_index(drop=True)

        msg = 'Ключевые метрики за ' + get_dates() + ':\n\n'
        msg += 'DAU: ' + str(df_yesterday.DAU[0]) + '\n'
        msg += 'Просмотры: ' + str(df_yesterday.views[0]) + '\n'
        msg += 'Лайки: ' + str(df_yesterday.likes[0]) + '\n'
        msg += 'CTR: ' + str(df_yesterday.CTR[0])

        bot.sendMessage(chat_id=chat_id, text=msg)
        
    @task()
    def send_plot(df):
        plt.figure(figsize=[25, 17])

        plt.suptitle('Значения метрик за предыдущие 7 дней', fontsize=20, fontweight='bold')

        plt.subplot(2, 2, 1)
        plt.plot(df.date, df.DAU, marker='o', color='g', linewidth=3)
        plt.title('DAU', fontsize=15)
        plt.xticks(rotation=45)
        plt.grid()

        plt.subplot(2, 2, 2)
        plt.plot(df.date, df.views, marker='o', color='b', linewidth=3)
        plt.title('Просмотры', fontsize=15)
        plt.xticks(rotation=45)
        plt.grid()

        plt.subplot(2, 2, 3)
        plt.plot(df.date, df.likes, marker='o', color='y', linewidth=3)
        plt.title('Лайки', fontsize=15)
        plt.xticks(rotation=45)
        plt.grid()

        plt.subplot(2, 2, 4)
        plt.plot(df.date, df.CTR, marker='o', color='r', linewidth=3)
        plt.title('CTR', fontsize=15)
        plt.xticks(rotation=45)
        plt.grid()

        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'report_plot.png'
        plt.close()

        bot.sendPhoto(chat_id=chat_id, photo=plot_object)

    df = extract_df()
    send_message(df)
    send_plot(df)

KC_auto_report_zas = KC_auto_report_zas()