from .constants import RPC_TIMEOUT_SECONDS
from .logger import create_logger
import asyncio
from aio_pika import (
    IncomingMessage,
    Channel,
)
from .helpers import (
    routing_key_to_address,
    uuid_str,
)
log = create_logger(__name__)


class RpcRequest:
    def __init__(
        self,
        channel: Channel,
        original_routing_key,
        rpc_timeout=RPC_TIMEOUT_SECONDS
    ):
        self.channel = channel
        self.queue_has_been_deleted = False
        self.correlation_id = uuid_str()
        self.reply_to = uuid_str()
        self.original_routing_key = original_routing_key
        self.rpc_timeout = rpc_timeout
        self.queue = None

    async def declare_queue(self):
        self.queue = await self.channel.declare_queue(
            name=self.reply_to,
            auto_delete=True,
            exclusive=True,
            durable=False,
        )

    async def delete_queue(self):
        await self.channel.queue_delete(self.reply_to)
        self.queue_has_been_deleted = True

    def create_message_handler(self, handle_message):
        async def handle_rpc_message(message: IncomingMessage):
            if message.correlation_id == self.correlation_id:
                message.routing_key = self.original_routing_key
                await handle_message(message)
                await self.delete_queue()

        return handle_rpc_message

    async def timeout_request(self, send_osc_func):
        await asyncio.sleep(self.rpc_timeout)
        if self.queue_has_been_deleted is False:
            address = routing_key_to_address(self.original_routing_key)
            send_osc_func(
                address, "Timed out waiting for AMQP rpc response.")
            await self.delete_queue()
            self.queue_has_been_deleted = True
