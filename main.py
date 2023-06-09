import paho.mqtt.client as mqtt
import sqlite3
from time import time
from secrets import secrets

MQTT_HOST = secrets.get("MQTT_HOST")
MQTT_PORT = secrets.get("MQTT_PORT")
MQTT_CLIENT_ID = secrets.get("MQTT_CLIENT_ID")
MQTT_USER = secrets.get("MQTT_USER")
MQTT_PASSWORD = secrets.get("MQTT_PASSWORD")
TOPIC_SENSOR = secrets.get("TOPIC_SENSOR")
TOPIC_LOCK = secrets.get("TOPIC_LOCK")

DATABASE_FILE = 'SmartDoorLock.db'


def on_connect(mqtt_client, user_data, flags, conn_result):
    mqtt_client.subscribe(TOPIC_SENSOR)
    mqtt_client.subscribe(TOPIC_LOCK)


def on_message(mqtt_client, user_data, message):
    payload = message.payload.decode('utf-8')

    db_conn = user_data['db_conn']
    sql_sensor = 'INSERT INTO DoorSensor (topic, payload, created_at) VALUES (?, ?, ?)'
    sql_lock = 'INSERT INTO DoorLock (topic, payload, created_at) VALUES (?, ?, ?)'
    cursor = db_conn.cursor()
    if message.topic == TOPIC_LOCK:
        cursor.execute(sql_lock, (TOPIC_LOCK, payload, int(time())))
    elif message.topic == TOPIC_SENSOR:
            cursor.execute(sql_sensor, (TOPIC_SENSOR, payload, int(time())))

    db_conn.commit()
    cursor.close()


def main():
    db_conn = sqlite3.connect(DATABASE_FILE)
    sql_sensor = """
    CREATE TABLE IF NOT EXISTS DoorSensor (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        topic TEXT NOT NULL,
        payload TEXT NOT NULL,
        created_at INTEGER NOT NULL
    )
    """

    sql_lock = """
        CREATE TABLE IF NOT EXISTS DoorLock (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            topic TEXT NOT NULL,
            payload TEXT NOT NULL,
            created_at INTEGER NOT NULL
        )
        """

    cursor = db_conn.cursor()
    cursor.execute(sql_sensor)
    cursor.execute(sql_lock)
    cursor.close()

    mqtt_client = mqtt.Client(MQTT_CLIENT_ID)
    mqtt_client.username_pw_set(MQTT_USER, MQTT_PASSWORD)
    mqtt_client.user_data_set({'db_conn': db_conn})

    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message

    mqtt_client.connect(MQTT_HOST, MQTT_PORT)
    mqtt_client.loop_forever()


main()