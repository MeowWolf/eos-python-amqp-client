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
    payload_to_args,
    pretty_format,
    get_routing_key_to_address_list,
    get_incoming_routing_key_list,
    get_address_by_routing_key,
)
from .rpc_request import RpcRequest
import asyncio
from aio_pika import (
    connect_robust,
    IncomingMessage,
    Message,
)
log = create_logger(__name__)

MESSAGE_FROM_FIELD = 'message_from'


class AmqpClient:
    def __init__(
        self,
        routing_key_string=ROUTING_KEYS_TO_LISTEN_TO,
        host=HOST,
        port=PORT,
        username=USERNAME,
        password=PASSWORD,
        amqp_reconnect_seconds=AMQP_RECONNECT_SECONDS,
        rpc_timeout=RPC_TIMEOUT_SECONDS,
        rpc_request=None,
        exchange_name=EXCHANGE_NAME,
        exchange_type=EXCHANGE_TYPE,
    ):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.amqp_reconnect_seconds = amqp_reconnect_seconds
        self.rpc_timeout = rpc_timeout
        self.rpc_request = rpc_request
        self.routing_key_to_address_list = get_routing_key_to_address_list(
            routing_key_string)
        self.routing_keys = get_incoming_routing_key_list(
            self.routing_key_to_address_list)
        self.exchange = None
        self.channel = None
        self.exchange_name = exchange_name
        self.exchange_type = exchange_type

    async def connect(self, loop):
        protocol = 'amqps' if USE_TLS is True else 'amqp'

        try:
            connection = await connect_robust(
                f'{protocol}://{self.username}:{self.password}@{self.host}:{self.port}', loop=loop
            )
            self.channel = await connection.channel()
            await self.channel.set_qos(prefetch_count=10)

            return connection
        except:  # pragma: no cover
            log.info(f'Could not connect to amqp broker. Retrying...')
            await asyncio.sleep(self.amqp_reconnect_seconds)
            return await self.connect(loop)

    async def consume(self, loop=asyncio.get_event_loop()):
        connection = await self.connect(loop)

        self.exchange = await self.channel.declare_exchange(
            name=self.exchange_name,
            type=self.exchange_type,
            durable=True,
            auto_delete=False
        )

        queue = await self.channel.declare_queue(
            name=self.queue_name,
            auto_delete=True,
            exclusive=True,
            durable=False,
        )
        [await queue.bind(self.exchange, routing_key) for routing_key in self.routing_keys]
        await queue.consume(self.handle_message)

        log.info(f'AMQP connection established to: {self.exchange_name}')
        log.info(f'listening to: {self.routing_keys}')

        return connection

    async def handle_message(self, message: IncomingMessage):
        # context manager handles acking for us
        async with message.process():
            try:
                # we don't want to send a message back to ourselves
                if message.headers.get(MESSAGE_FROM_FIELD) != self.device_name:
                    payload = message.body.decode('utf-8')
                    routing_key = message.routing_key

                    log.info(
                        f'Received amqp message: {routing_key}: {pretty_format(payload)}')
                    log.debug(message)

                    address = get_address_by_routing_key(
                        routing_key, self.routing_key_to_address_list)

                    args = payload_to_args(payload)

            except Exception as err:  # pragma: no cover
                log.error(f'Error handling incoming AMQP message: {err}')

    async def publish(self, routing_key, payload, is_rpc=False):
        try:
            async def send_message(correlation_id='', reply_to=''):
                log.info(
                    f'Publishing amqp message: {routing_key}: {pretty_format(payload)}'
                )
                await self.exchange.publish(
                    Message(
                        body=bytes(payload, 'utf-8'),
                        headers={MESSAGE_FROM_FIELD: self.device_name},
                        correlation_id=correlation_id,
                        reply_to=reply_to
                    ),
                    routing_key=routing_key,
                )

            if is_rpc:
                rpc_request = RpcRequest(
                    channel=self.channel,
                    original_routing_key=routing_key,
                ) if self.rpc_request is None else self.rpc_request

                await rpc_request.declare_queue()
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
