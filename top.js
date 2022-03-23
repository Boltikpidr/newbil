import random
from threading import Thread
import paho.mqtt.client as mqtt

# Берем константы
topic = "service/weather_logger"
mqtt_host = 'mqtt0.bast-dev.ru'
mqtt_port = 1883
mqtt_username = 'hackathon'
mqtt_password = 'Autumn2021'

# Генерация идентификатора клиента
mqtt_client_id = f'test_client_{random.randint(0, 9999)}'

# Топик для отправки статуса подключения
mqtt_status_topic = f'{mqtt_client_id}/state'


class MQTTApiError(Exception):
    pass


class MqttClient(Thread):
    def __init__(self, timeout, topics):
        super(MqttClient, self).__init__()
        self.sent_message_counter = 0
        self.broker_user = mqtt_username
        self.broker_pass = mqtt_password
        self.broker_host = mqtt_host
        self.broker_port = int(mqtt_port)
        self.keep_alive = timeout
        self.topics = topics

        self.client = mqtt.Client(
            client_id=mqtt_client_id,
            protocol=mqtt.MQTTv311,
            clean_session=False,
            userdata=self,
        )

    def run(self):
        self._connect_to_broker()

    def _connect_to_broker(self):
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message
        self.client.username_pw_set(
            username=self.broker_user,
            password=self.broker_pass,
        )
        # Отправляем сообщение со статусом при разрыве коннекта
        self.client.will_set(
            topic=mqtt_status_topic,
            payload='',
            retain=True,
        )
        self.client.connect(
            self.broker_host,
            self.broker_port,
            self.keep_alive,
        )
        self.client.loop_forever()

    def _on_connect(self, client, userdata, flags, rc):
        """callback при подключении"""

        print('MQTT Client --> connected\n')

        # Отправляем сообщение со статусом
        client.publish(
            topic=mqtt_status_topic,
            payload='active',
            retain=True,
        )

        print('MQTT Client --> status - active\n')

        # Подписываемся на топики
        for topic in self.topics:
            client.subscribe(topic)

    def _on_message(self, client, userdata, msg):
        """callback при получении сообщения"""

        print('MQTT client --> received a message\n')
        api.handle(self.client, msg.topic, msg.payload)


class Request():
    mqtt_client = None
    data = None


class MqttApi:
    prefix = ""
    endpoints = {}

    def __init__(self, prefix):
        self.prefix = prefix

    def handle(self, mqtt_client, topic, payload):
        """Обработчик MQTT сообщения"""

        request = Request()
        request.mqtt_client = mqtt_client

        try:
            request.data = payload
        except Exception as e:
            print(f'ERROR', e, topic, '\n')
            return

        print('topic:', topic)
        print('data:', request.data.decode('ascii'), '\n')


def start_mqtt():
    """Запуск MQTT подключения"""

    print('MQTT Client --> starting ...\n')

    MqttClient(timeout=1, topics=[
        f'{mqtt_host}/service/weather_logger/#',
    ]).start()
    MqttClient(timeout=30,topics=[
        f'{mqtt_host}/service/weather_logger/outdor_humidity/#'
    ])

api = MqttApi(mqtt_client_id)

if __name__ == '__main__':
    start_mqtt()
