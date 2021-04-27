from typing import List


LOG_LEVEL: str = 'INFO'
ROUTING_KEYS_TO_LISTEN_TO: List[str] = []
HOST: str = 'localhost'
USE_TLS: bool = False
PORT: int = 5672
USERNAME: str = 'rabbitmq'
PASSWORD: str = 'rabbitmq'
AMQP_RECONNECT_SECONDS: int = 3
EXCHANGE_NAME: str = 'amq.topic'
EXCHANGE_TYPE: str = 'topic'
RPC_TIMEOUT_SECONDS: int = 3
