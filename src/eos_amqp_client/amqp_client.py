import asyncio
from typing import Callable, List, Union
from aio_pika.channel import Channel
from aio_pika.exchange import Exchange
from aio_pika.message import IncomingMessage
from aio_pika.queue import Queue
from aio_pika.robust_connection import RobustConnection
from aio_pika import (
    connect_robust,
    Message,
)

from eos_amqp_client.constants import (
    HOST,
    USERNAME,
    USE_TLS,
    PORT,
    PASSWORD,
    AMQP_RECONNECT_SECONDS,
    EXCHANGE_NAME,
    EXCHANGE_TYPE,
    RPC_TIMEOUT_SECONDS,
    PREFETCH_COUNT,
)
from eos_amqp_client.logger import create_logger
from eos_amqp_client.rpc_request import RpcRequest
log = create_logger(__name__)


class AmqpClient:
    def __init__(
        self,
        use_tls: bool = USE_TLS,
        host: str = HOST,
        port: int = PORT,
        username: str = USERNAME,
        password: str = PASSWORD,
        amqp_reconnect_seconds: int = AMQP_RECONNECT_SECONDS,
        rpc_timeout: int = RPC_TIMEOUT_SECONDS,
        exchange_name: str = EXCHANGE_NAME,
        exchange_type: str = EXCHANGE_TYPE,
        prefetch_count: int = PREFETCH_COUNT,
    ):
        self.connection: RobustConnection = None
        self.exchange: Exchange = None

        self.use_tls: bool = use_tls
        self.host: str = host
        self.port: int = port
        self.username: str = username
        self.password: str = password
        self.amqp_reconnect_seconds: int = amqp_reconnect_seconds
        self.rpc_timeout: int = rpc_timeout
        self.routing_keys: List[str] = []
        self.exchange_name: str = exchange_name
        self.exchange_type: str = exchange_type
        self.prefetch_count: str = prefetch_count

    async def connect(self) -> RobustConnection:
        protocol: str = 'amqps' if self.username is True else 'amqp'

        try:
            self.connection = await connect_robust(
                url=f'{protocol}://{self.username}:{self.password}@{self.host}:{self.port}',
                timeout=self.amqp_reconnect_seconds,
            )

        except Exception as err:
            log.error(f'Could not connect to amqp broker: {err}, Retrying...')
            await asyncio.sleep(self.amqp_reconnect_seconds)
            return await self.connect()

    async def create_channel(self) -> Channel:
        try:
            channel: Channel = await self.connection.channel()
            await channel.set_qos(prefetch_count=self.prefetch_count)
            await self.declare_exchange(channel)

            return channel
        except Exception as err:  # pragma no cover
            log.error(f'Could not create channel: {err}')

    async def declare_exchange(self, channel: Channel) -> None:
        self.exchange: Exchange = await channel.declare_exchange(
            name=self.exchange_name,
            type=self.exchange_type,
            durable=True,
            auto_delete=False
        )

    async def publish(
        self,
        routing_key: str,
        payload: str,
        is_rpc: bool = False,
        channel: Channel = None,
        handle_rpc_message: Callable[[IncomingMessage], None] = None,
    ):
        try:
            async def send_message(correlation_id: str = '', reply_to: str = ''):
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
                    channel=channel,
                    original_routing_key=routing_key,
                )

                await rpc_request.declare_queue()
                await rpc_request.queue.consume(rpc_request.create_message_handler(handle_rpc_message))

                await send_message(rpc_request.correlation_id, rpc_request.reply_to)
                asyncio.create_task(
                    rpc_request.timeout_request(
                        rpc_request.correlation_id,
                        rpc_request.reply_to,
                    )
                )
            else:
                await send_message()

        except Exception as err:
            log.error(f'Error publishing AMQP message: {err}')

    async def consume(
        self,
        channel: Channel,
        routing_keys: Union[List[str], str],
        handle_message: Callable[[IncomingMessage], None],
        queue_name: str = '',
        auto_delete: bool = True,
        exclusive: bool = True,
        durable: bool = False,
    ) -> None:

        if isinstance(routing_keys, str):
            routing_keys = [routing_keys]

        queue: Queue = await channel.declare_queue(
            name=queue_name,
            auto_delete=auto_delete,
            exclusive=exclusive,
            durable=durable,
        )

        [await queue.bind(self.exchange, routing_key) for routing_key in routing_keys]

        await queue.consume(handle_message)

        log.info(f'AMQP connection established to: {self.exchange_name}')
        log.info(f'listening to: {routing_keys}')
