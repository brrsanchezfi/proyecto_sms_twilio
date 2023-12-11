from airflow import DAG
from datetime import datetime, timedelta
from googleapiclient.discovery import build

from airflow.operators.python_operator import PythonOperator
from twilio.rest import Client

import os

import logging

import pendulum
import pprint

API_KEY_YOUTUBE='AIzaSyCgKXm552fgPXteo0grv9GWxtOAR5ll9w0'
CHANNEL_ID='UCPF-oYb2-xN5FbCXy0167Gg'
ACCOUNT_SID='AC62829ecb7cfc9c649d98f53e1999b789'
AUTH_TOKEN='5a150e7cc1320119cf50ef4870e9bdbe'

default_args={
    'owner':'Roberto',
    'depends_on_past':True,
    'start_date':pendulum.datetime(2023,1,1, tz="UTC"),
    'retries':0,
}

dag = DAG(
    'YOUTUBE_ALERT_NEW_VIDEO',
    default_args=default_args,
    description= 'dag para alertar de nuevos videos subidos',
    schedule=timedelta(minutes=30),
    catchup=False,
)


def obtener_ultimo_video():
    
    api_key_youtube=API_KEY_YOUTUBE
    channel_id = CHANNEL_ID

    youtube_request = build('youtube','v3',developerKey=api_key_youtube)

    video_request = youtube_request.search().list(
        part='id,snippet',
        channelId=channel_id,
        type='video',
        order='date',
        maxResults=1
    )

    videos_response= video_request.execute()

    if('items' in videos_response):
        ultimo_video = videos_response['items'][0]
        return {
            'video_id':ultimo_video['id']['videoId'],
            'title':ultimo_video['snippet']['title'],
            'publishedAt':ultimo_video['snippet']['publishedAt'],
            'descrition':ultimo_video['snippet']['description']
        }
    else:
        return None


def verificar_nuevo_video(**kwargs):

    ti = kwargs['ti']
    ultimo_video_almacenado = ti.xcom_pull(task_ids='task_1')

    fecha_dt = datetime.strptime(ultimo_video_almacenado.get('publishedAt'), '%Y-%m-%dT%H:%M:%SZ')
    fecha_actual = datetime.utcnow()
    diferencia_tiempo = fecha_actual - fecha_dt

    if diferencia_tiempo < timedelta(minutes=30):
        return ultimo_video_almacenado
    else:
        None

def enviar_sms(**kwargs):
        
    ti = kwargs['ti']
    ultimo_video = ti.xcom_pull(task_ids='task_2')

    print(
            f"Nuevo video en YouTube:\n"
            f"Título: {ultimo_video.get('title')}\n"
            f"Video ID: {ultimo_video.get('videoId')}\n"
            f"Publicado en: {ultimo_video.get('publishedAt')}\n"
            f"Descripción: {ultimo_video.get('description')}"
    )

    if ultimo_video!=None:
            
        account_sid = ACCOUNT_SID
        auth_token = AUTH_TOKEN
        
        client = Client(account_sid, auth_token)

        message = client.messages.create(
        from_='+19162587884',
        body=(
            f"Nuevo video en YouTube:\n"
            f"Título: {ultimo_video.get('title')}\n"
            f"Video ID: {ultimo_video.get('videoId')}\n"
            f"Publicado en: {ultimo_video.get('publishedAt')}\n"
            f"Descripción: {ultimo_video.get('description')}"
        ),
        to='+573123276044'
        )

        print(message.sid)

    else:
        
        print('Video antiguo')


task_1 = PythonOperator(
    task_id="task_1",
    python_callable=obtener_ultimo_video,
    provide_context=True,
    dag=dag,
)

task_2 = PythonOperator(
    task_id="task_2",
    python_callable=verificar_nuevo_video,
    provide_context=True,
    dag=dag,
)

task_3 = PythonOperator(
    task_id="task_3",
    python_callable=enviar_sms,
    provide_context=True,
    dag=dag,
)

task_1>>task_2>>task_3
