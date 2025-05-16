from airflow import DAG
from airflow.operators.python_operator import PythonOperator # Так как мы пишем таски в питоне
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from datetime import datetime, timedelta
import pandas as pd
from io import StringIO
import requests
import pandahouse as ph


# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 's_zizevskij',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 3, 21),
}

# создаем подключение к БД, из которой будем загружать данные 
connection = {'host': 'https://clickhouse.lab.karpov.courses',
                      'database':'simulator_20250220',
                      'user':'student', 
                      'password':'dpo_python_2020'
                     }

# Интервал запуска DAG
schedule_interval = '0 12 * * *'


@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def sa_zizevskij():

    @task()
    def extract_actions():
        # подключились к БД и выполнили запрос по извлечению данных
        # из таблицы по действиям в ленте постов
        q1 = """
                SELECT 
                    user_id,
                    age, 
                    os,
                    gender,
                    sum(action = 'like') as likes,
                    sum(action = 'view') as views,
                    toDate(time) as event_date
                FROM simulator_20250220.feed_actions 
                WHERE toDate(time) = yesterday()
                GROUP BY user_id, event_date, age, os, gender
            """
        df_feed = ph.read_clickhouse(q1, connection=connection)
        return df_feed
    
    @task() 
    def extract_message(df_feed):
        # подключились к БД и выполнили запрос по извлечению данных
        # из таблицы по сообщениям
        q2 = """
                SELECT user_id,
                       messages_received, 
                       messages_sent, 
                       users_received, 
                       users_sent
                       
                FROM
                  (SELECT 
                            user_id,
                            count(receiver_id) as messages_sent,
                            count(distinct receiver_id) as users_sent
                        FROM simulator_20250220.message_actions
                        WHERE toDate(time) = yesterday()
                        GROUP BY  user_id) t1
                   JOIN
                     (SELECT 
                            receiver_id,
                            count(user_id) as messages_received,
                            count(distinct user_id) as users_received
                        FROM simulator_20250220.message_actions
                        WHERE toDate(time) = yesterday()
                        GROUP BY  receiver_id) t2
                   ON t1.user_id =t2.receiver_id
            """
        df_mess = ph.read_clickhouse(q2, connection=connection)
        df_merged = df_feed.merge(df_mess, how='outer', on='user_id')
        return df_merged
    
    @task() 
    # Срез по операционной системе
    def df_os(df_merged):
        df_os = df_merged.groupby(['os', 'event_date'])[['views','likes','messages_received','messages_sent','users_received','users_sent']] \
                .sum().reset_index()
        df_os['dimension'] = 'os'
        df_os.rename(columns = {'os' : 'dimension_value'}, inplace = True)
        return df_os    
    
    @task() 
    # Срез по полу
    def df_gender(df_merged):
        df_gender = df_merged.groupby(['gender', 'event_date'])[['views','likes','messages_received','messages_sent','users_received','users_sent']] \
                    .sum().reset_index()
        df_gender['dimension'] = 'gender'
        df_gender.rename(columns = {'gender' : 'dimension_value'}, inplace = True)
        return df_gender
           
    
    @task() 
    # Срез по возрасту
    def df_age(df_merged):
        df_age = df_merged.groupby(['age', 'event_date'])[['views','likes','messages_received','messages_sent','users_received','users_sent']] \
                    .sum().reset_index()
        df_age['dimension'] = 'age'
        df_age.rename(columns = {'age' : 'dimension_value'}, inplace = True)
        return df_age
    
    @task() 
    # загрузка результирующей таблицы с данными по отчету 
    # в собственную дополняемую таблицу в базе данных   
    def load(df_gender, df_os, df_age):
        df_total = pd.concat([df_gender, df_os, df_age])
        df_total = df_total.reindex(columns=['event_date', 
                                     'dimension', 
                                     'dimension_value',
                                     'views', 
                                     'likes', 
                                     'messages_received',
                                     'messages_sent', 
                                     'users_received', 
                                     'users_sent',
                                    ])
        df_total = df_total.astype({
                                    #'dimension' : 'string',
                                    #'dimension_value' : 'string',
                                    #'event_date' : 'datetime64',
                                    'views': 'int32',
                                    'likes': 'int32',
                                    'messages_sent': 'int32',
                                    'users_sent': 'int32',
                                    'messages_received' : 'int32',
                                    'users_received' : 'int32'
                                   })
        
        # соединение с тестовой  БД, в которую будет писаться таблица с отчетом за день 
        connect_test = {
                'host': 'https://clickhouse.lab.karpov.courses',
                'password': '656e2b0c9c',
                'user': 'student-rw',
                'database': 'test'
            }
        create_table = """
                    CREATE TABLE IF NOT EXISTS test.s_zizevskij(
                        dimension String,
                        dimension_value String,
                        event_date String,
                        views Int32,
                        likes Int32,
                        messages_sent Int32,
                        users_sent Int32,
                        messages_received Int32,
                        users_received Int32)
                    ENGINE = MergeTree()
                    ORDER BY event_date
                """
        ph.execute(query=create_table, connection=connect_test)
        ph.to_clickhouse(df = df_total, table = 's_zizevskij', connection=connect_test, index=False)
        
    # собственно шаги DAG, каждое из которых - отдельная task     
    df_feed = extract_actions()
    df_merged = extract_message(df_feed)
    df_os = df_os(df_merged)
    df_gender = df_gender(df_merged)
    df_age = df_age(df_merged)
    load(df_gender, df_os, df_age)  
    
    
sa_zizevskij = sa_zizevskij()