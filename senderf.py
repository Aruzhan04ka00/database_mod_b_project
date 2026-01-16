import paho.mqtt.client as mqtt
import json
import time

# Настройки MQTT
mqtt_host = "localhost"
mqtt_port = 1883
mqtt_topic = "sensors/devices"

# Функция для публикации данных о ресторанах через MQTT с использованием batch_size
def publish_data(filename, batch_size=10):
    try:
        # Открытие файла и чтение данных
        with open(filename, 'r', encoding='utf-8') as file:
            data = [json.loads(line) for line in file if line.strip()]  # Чтение каждой строки как JSON-объекта

        # Разделение данных на пакеты
        for i in range(0, len(data), batch_size):
            batch = data[i:i + batch_size]
            mqtt_payload = json.dumps(batch)  # Преобразование данных в JSON строку
            mqtt_client.publish(mqtt_topic, mqtt_payload)
            print(f"Published batch {i // batch_size + 1} to MQTT topic '{mqtt_topic}'")
            time.sleep(1)  # Задержка между публикациями

    except FileNotFoundError:
        print(f"File '{filename}' not found")
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON from file '{filename}': {str(e)}")
    except Exception as e:
        print(f"Error publishing data to MQTT: {str(e)}")

# Настройка клиента MQTT и подключение к брокеру
mqtt_client = mqtt.Client()
try:
    mqtt_client.connect(mqtt_host, mqtt_port)
except ConnectionRefusedError:
    print(f"Connection to MQTT broker at {mqtt_host}:{mqtt_port} refused")
    exit(1)

# Запуск публикации данных
publish_data('iot_data.json', batch_size=10)

# Отключение от MQTT брокера
mqtt_client.disconnect()
