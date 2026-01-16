import paho.mqtt.client as mqtt
from pymongo import MongoClient
from py2neo import Graph
import mysql.connector
from mysql.connector import errorcode
import json

# Подключение к MongoDB
mongo_client = MongoClient('mongodb://localhost:27017/')
db = mongo_client['iotdb']
collection = db['iot']

# Подключение к Neo4j
neo4j_graph = Graph("bolt://localhost:7687", auth=("neo4j", "password"))

# Подключение к MySQL
def connect_to_mysql():
    mysql_conn = mysql.connector.connect(
        host="localhost",
        user="root",
        password="",
        database="iot"
    )
    return mysql_conn

# Callback функция при подключении к MQTT серверу
def on_connect(mqtt_client, userdata, flags, rc):
    print(f"Connected with result code {rc}")
    mqtt_client.subscribe("sensors/devices")  # Подписка на топик "sensors/restaurants"

# Callback функция при получении сообщения
def on_message(mqtt_client, userdata, msg):
    print(f"Received message: {msg.topic} {msg.payload.decode()}")
    try:
        batch = json.loads(msg.payload.decode())  # Декодируем JSON-полезную нагрузку
        for document in batch:
            process_and_store_data(document)
    except json.JSONDecodeError:
        print("Failed to decode JSON")

# Функция для обработки и хранения данных
def process_and_store_data(document):
    store_in_mongodb(document)
    store_in_neo4j(document)
    store_in_mysql(document)

# Функция для сохранения данных в MongoDB
def store_in_mongodb(document):
    mongo_document = {
        "device_id": document["device_id"],
        "timestamp": document["timestamp"],
        "motion" : document["motion"],
        "latitude" :document["location"]["latitude"],
        "longitude" :document["location"]["longitude"]
    }
    collection.insert_one(mongo_document)
    print(f"Data stored in MongoDB: {mongo_document}")

# Функция для сохранения данных в Neo4j
def store_in_neo4j(document):
    device_id = document['device_id']

    # Создание узла устройства
    neo4j_graph.run(
        """
        MERGE (d:Device {id: $device_id})
        """, 
        device_id=device_id
    )

    # Проверяем наличие поля data и data_type в документе
    if 'data' in document and 'data_type' in document['data']:
        data_type = document['data']['data_type']['type']
    else:
        data_type = 'Unknown'

    # Создание узла типа данных
    neo4j_graph.run(
        """
        MERGE (t:DataType {type: $data_type})
        """, 
        data_type=data_type
    )

    # Создание отношения HAS_DATA_TYPE между устройством и типом данных
    neo4j_graph.run(
        """
        MATCH (d:Device {id: $device_id})
        MATCH (t:DataType {type: $data_type})
        MERGE (d)-[:HAS_DATA_TYPE]->(t)
        """, 
        device_id=device_id, data_type=data_type
    )

    print(f"Data stored in Neo4j: Device '{device_id}' with data type '{data_type}'")



# Функция для сохранения данных в MySQL
def store_in_mysql(document):
    mysql_conn = connect_to_mysql()
    if mysql_conn is None:
        print("Failed to connect to MySQL")
        return
    mysql_cursor = mysql_conn.cursor()

    # Insert data into Devices table
    device_query = """
    INSERT INTO Devices (device_id, timestamp, motion, latitude, longitude)
    VALUES (%s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        timestamp=VALUES(timestamp),
        motion=VALUES(motion),
        latitude=VALUES(latitude),
        longitude=VALUES(longitude)
    """
    device_values = (
        document['device_id'],
        document['timestamp'],
        document['motion'],
        document['location']['latitude'],
        document['location']['longitude']
    )
    mysql_cursor.execute(device_query, device_values)

    # Insert data into DataTypes table
    data_type_query = """
    INSERT INTO DataTypes (type, ssid, signal_strength)
    VALUES (%s, %s, %s)
    ON DUPLICATE KEY UPDATE
        ssid=VALUES(ssid),
        signal_strength=VALUES(signal_strength)
    """
    data_type_values = (
        document['data']['data_type']['type'],
        document['data']['data_type']['ssid'],
        document['data']['data_type']['signal_strength']
    )
    mysql_cursor.execute(data_type_query, data_type_values)

    # Insert data into DeviceDataTypes table
    device_data_type_query = """
    INSERT INTO DeviceDataTypes (device_id, data_type)
    VALUES (%s, %s)
    ON DUPLICATE KEY UPDATE
        device_id=VALUES(device_id),
        data_type=VALUES(data_type)
    """
    device_data_type_values = (
        document['device_id'],
        document['data']['data_type']['type']
    )
    mysql_cursor.execute(device_data_type_query, device_data_type_values)

    mysql_conn.commit()
    print(f"Data stored in MySQL: {document}")

    mysql_cursor.close()
    mysql_conn.close()
    
# Настройка клиента MQTT
mqtt_client = mqtt.Client()
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message

# Подключение к серверу MQTT
mqtt_client.connect("localhost", 1883, 60)

# Запуск цикла обработки сообщений
mqtt_client.loop_forever()
