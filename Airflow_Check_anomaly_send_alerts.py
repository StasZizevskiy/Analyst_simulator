import pandas as pd  # Работа с таблицами
import requests  # HTTP-запросы
import io  # Потоки ввода/вывода
import telegram  # работа с телеграм
import seaborn as sns  # Визуализация данных
import matplotlib.pyplot as plt  # Графики
from datetime import datetime, timedelta  # Работа с датами
from io import StringIO  # Чтение данных из строки
from airflow.decorators import dag, task  # DAG и таски Airflow

#Вводные данные для моего бота
my_token = '7680117939:AAFmoIXFfxF8ruzwtiZxg37OIH9jTpBZCHw'
chat = -969316925
bot = telegram.Bot(token = my_token) 


#Настройки по умолчанию для tasks
default_args = {
    'owner': 's_zizevskij',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 3, 2)
}
    
# Интервал запуска DAG
schedule_interval = '*/15 * * * *'

# Функция для выполнения SQL-запроса в ClickHouse и получения данных в виде DataFrame
def ch_get_df(query, host='https://clickhouse.lab.karpov.courses', user='student', password='dpo_python_2020'):
    r = requests.post(host, data=query.encode("utf-8"), auth=(user, password), verify=False)  # Отправляем запрос в ClickHouse
    result = pd.read_csv(StringIO(r.text), sep='\t')  # Читаем полученные данные в DataFrame
    return result

# поиск аномалий с помощью межквартильного интервала 25 и 75
def check_anomaly(df, metric, a=5, n=5):
    df['q25'] = df[metric].shift(1).rolling(n).quantile(0.25)  # считаем 25 процентиль, пробегая по n датафрейм
    df['q75'] = df[metric].shift(1).rolling(n).quantile(0.75)  # считаем 75 процентиль, пробегая по n датафрейм
    df['iqr'] = df['q75'] - df['q25']  # Вычисляем межквартильный интервал 
    df['up'] = df['q75'] + a * df['iqr']  # Верхняя граница аномалии (Q3 + a*IQR)
    df['low'] = df['q25'] - a * df['iqr']  # Нижняя граница аномалии (Q1 - a*IQR)
    
# Сглаживаем волатильность границ
    df['up'] = df['up'].rolling(n, center=True, min_periods=1).mean()  
    df['low'] = df['low'].rolling(n, center=True, min_periods=1).mean()

    # Проверяем, выходит ли последнее значение метрики за границы допустимого интервала
    if df[metric].iloc[-1] < df['low'].iloc[-1] or df[metric].iloc[-1] > df['up'].iloc[-1]:
        is_alert = 1  # выявлена аномалия
    else:
        is_alert = 0  # норм

    return is_alert, df  # Возвращаем статус аномалии и DataFrame с новыми полями

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def alerts_zas():
    
    @task
    def get_data():
        # получаем данные из feed_actions
        feed_query = '''
            SELECT toStartOfFifteenMinutes(time) as ts,  -- округляем время до 15 минут 
                   toDate(ts) as date,  -- получаем дату
                   formatDateTime(ts, '%R') as hm,  -- время в формате HH:MM
                   uniqExact(user_id) as dau_lenta,  -- dau
                   countIf(action = 'view') AS views,  -- просмотры
                   countIf(action = 'like') AS likes,  -- лайки
                   if(views > 0, likes / views, 0) AS ctr  -- конверсия, отработали ошибку деления на ноль
            FROM simulator_20250220.feed_actions
            WHERE ts >= today() - 1 AND ts < toStartOfFifteenMinutes(now())  -- данные за последние сутки
            GROUP BY ts, date, hm  -- группировка по времени
            ORDER BY ts
            FORMAT TSVWithNames  -- вывод в формате TSV с заголовками
            '''

        # получаем данные из feed_actions
        message_query = '''
            SELECT 
                toStartOfFifteenMinutes(time) AS ts,
                toDate(ts) AS date,
                formatDateTime(ts, '%R') AS hm,
                uniqExact(user_id) AS dau_message,  -- dau
                count(*) AS messages  -- сообщения
            FROM simulator_20250220.message_actions
            WHERE ts >= today() - 1 AND ts < toStartOfFifteenMinutes(now())  -- данные за последние сутки
            GROUP BY ts, date, hm
            ORDER BY ts
            FORMAT TSVWithNames
            '''
        
        df_feed = ch_get_df(feed_query)  # Запрос данных из ленты
        df_message = ch_get_df(message_query)  # Запрос данных из мессенджера

        # Объединяем два датафрейма по столбцу 'ts'
        data = pd.merge(df_feed, df_message, on=['ts', 'date', 'hm'], how='outer').fillna(0)
        return data
    
    @task
    def send_alert(my_data, chat = None):
        
        chat_id = chat or 908660065 #личный диалог
        
        metrics_list = ['dau_lenta', 'likes', 'views', 'ctr', 'dau_message', 'messages']  # Список метрик для анализа

        for metric in metrics_list:
            df = my_data[['ts', 'date', 'hm', metric]].copy()  # Копируем нужные столбцы
            is_alert, df = check_anomaly(df, metric)  # Проверяем метрику на аномальность

            if is_alert == 1:  # Если обнаружена аномалия
                current_val = df[metric].iloc[-1]
                prev_val = df[metric].iloc[-2]

                if metric == "ctr":
                    diff = abs(current_val - prev_val)  # для CTR берём разницу по модулю
                    current_val = f"{current_val:.4f}"  # 4 знака после запятой
                    diff = f"{diff:.4f}"  # Без процентов
                else:
                    diff = abs(1 - (current_val / prev_val))  # считаем в процениах аномалию
                    current_val = f"{current_val:.2f}"  # 2 знака после запятой
                    diff = f"{diff:.2%}"  # в процентах

                msg = (
                    f"Внимание! Аномалия по {metric}\n"
                    f"Текущее значение: {current_val}\n"
                    f"Отклонение от предыдущего значения: {diff}\n"
                    "Подробнее: http://superset.lab.karpov.courses/r/6329"
                )  # Формируем сообщение без лишних отступов и пробелов

                sns.set(rc={'figure.figsize': (16, 10)})  # размер графика
                sns.set_style("darkgrid")
                plt.tight_layout()  # Улучшаем компоновку

                # Создаём график
                ax = sns.lineplot(x=df['ts'], y=df[metric], label='Метрика', linewidth=3.5, color='blue')  # метрика
                sns.lineplot(x=df['ts'], y=df['up'], label='Верхняя граница', linestyle='dashdot', color='green', ax=ax)  # Верхняя граница
                sns.lineplot(x=df['ts'], y=df['low'], label='Нижняя граница', linestyle='dashdot', color='green', ax=ax)  # Нижняя граница

                # Подсвечиваем аномалии красными точками
                anomaly = df[(df[metric] > df['up']) | (df[metric] < df['low'])]
                sns.scatterplot(x=anomaly['ts'], y=anomaly[metric], color='red', s=100, label='Аномалии', ax=ax, marker='o')

                # Оставляем подписи оси X только для каждого 20-го значения
                for ind, label in enumerate(ax.get_xticklabels()):
                    if ind % 20 == 0:
                        label.set_visible(True)
                    else:
                        label.set_visible(False)

                ax.set(xlabel='Время', ylabel=metric)  # Подписи осей
                ax.set_title(f'Динамика {metric}', fontsize=16)  # Заголовок графика
                ax.set(ylim=(0, None))  # Ограничение для оси Y
                ax.grid(True, linestyle='--', alpha=0.7)  # Добавляем сетку

                # Создаём файловый объект для отправки графика
                plot_object = io.BytesIO()
                ax.figure.savefig(plot_object)  # Сохраняем график в объект
                plot_object.seek(0)  # Перемещаем указатель в начало файла
                plot_object.name = '{0}.png'.format(metric)  # Присваиваем имя файлу
                plt.close()  # Закрываем график

                # Отправляем алерт
                bot.sendMessage(chat_id=chat_id, text=msg)
                bot.sendPhoto(chat_id=chat_id, photo=plot_object)
    data = get_data()
    send_alert(data, chat)
    
alerts_zas = alerts_zas()
        