import asyncio
from asyncio.events import AbstractEventLoop
from typing import Callable, List
from aio_pika.channel import Channel

from aio_pika.exchange import Exchange
from aio_pika.queue import Queue
from aio_pika.robust_connection import RobustConnection

from .constants import (
    ROUTING_KEYS_TO_LISTEN_TO,
    HOST,
    USERNAME,
    USE_TLS,
    PORT,
    PASSWORD,
    AMQP_RECONNECT_SECONDS,
    EXCHANGE_NAME,
    EXCHANGE_TYPE,
    RPC_TIMEOUT_SECONDS,
)
from .logger import create_logger
from .helpers import (
    pretty_format,
)
from .rpc_request import RpcRequest
from aio_pika import (
    connect_robust,
    Message,
)
log = create_logger(__name__)


class AmqpClient:
    def __init__(
        self,
        routing_key_string: str = ROUTING_KEYS_TO_LISTEN_TO,
        host: str = HOST,
        port: int = PORT,
        username: str = USERNAME,
        password: str = PASSWORD,
        amqp_reconnect_seconds: int = AMQP_RECONNECT_SECONDS,
        rpc_timeout: int = RPC_TIMEOUT_SECONDS,
        rpc_request: bool = None,
        exchange_name: str = EXCHANGE_NAME,
        exchange_type: str = EXCHANGE_TYPE,
    ):
        self.connection: RobustConnection = None
        self.exchange: Exchange = None

        self.host: str = host
        self.port: int = port
        self.username: str = username
        self.password: str = password
        self.amqp_reconnect_seconds: int = amqp_reconnect_seconds
        self.rpc_timeout: int = rpc_timeout
        self.rpc_request: bool = rpc_request
        self.routing_keys: List[str] = []
        self.exchange_name: str = exchange_name
        self.exchange_type: str = exchange_type

    async def connect(self, loop: AbstractEventLoop) -> RobustConnection:
        protocol: str = 'amqps' if USE_TLS is True else 'amqp'

        try:
            self.connection = await connect_robust(
                f'{protocol}://{self.username}:{self.password}@{self.host}:{self.port}', loop=loop
            )

        except:  # pragma: no cover
            log.info(f'Could not connect to amqp broker. Retrying...')
            await asyncio.sleep(self.amqp_reconnect_seconds)
            return await self.connect(loop)

    async def create_channel(self) -> Channel:
        try:
            channel: Channel = await self.connection.channel()
            await channel.set_qos(prefetch_count=10)
            await self.declare_exchange(channel)

            return channel
        except:  # pragma: no cover
            log.error(f'Could not create channel.')

    async def declare_exchange(self, channel: Channel) -> None:
        self.exchange: Exchange = await channel.declare_exchange(
            name=self.exchange_name,
            type=self.exchange_type,
            durable=True,
            auto_delete=False
        )

    async def publish(self, routing_key: str, payload: str, is_rpc: bool = False):
        try:
            async def send_message(correlation_id: str = '', reply_to: str = ''):
                log.info(
                    f'Publishing amqp message: {routing_key}: {pretty_format(payload)}'
                )
                await self.exchange.publish(
                    Message(
                        body=bytes(payload, 'utf-8'),
                        headers={},
                        correlation_id=correlation_id,
                        reply_to=reply_to
                    ),
                    routing_key=routing_key,
                )

            if is_rpc:
                rpc_request: RpcRequest = RpcRequest(
                    channel=self.channel,
                    original_routing_key=routing_key,
                ) if self.rpc_request is None else self.rpc_request

                await rpc_request.declare_queue()
                # message_handler: Callable[[IncomingMessage], None] = rpc_request.create_message_handler(
                message_handler = rpc_request.create_message_handler(
                    self.handle_message)
                await rpc_request.queue.consume(message_handler)
                await send_message(rpc_request.correlation_id, rpc_request.reply_to)
                await rpc_request.timeout_request(
                    # self.osc_client.send
                )
            else:
                await send_message()

        except Exception as err:
            log.error(f'Error publishing AMQP message: {err}')

    async def consume(self, loop: AbstractEventLoop = asyncio.get_event_loop()) -> RobustConnection:
        connection: RobustConnection = await self.connect(loop)

        queue: Queue = await self.channel.declare_queue(
            name=self.queue_name,
            auto_delete=True,
            exclusive=True,
            durable=False,
        )
        [await queue.bind(self.exchange, routing_key) for routing_key in self.routing_keys]
        # await queue.consume(self.handle_message)

        log.info(f'AMQP connection established to: {self.exchange_name}')
        log.info(f'listening to: {self.routing_keys}')

        return connection
