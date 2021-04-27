from typing import Callable
from aio_pika.queue import Queue
from .constants import RPC_TIMEOUT_SECONDS
from .logger import create_logger
import asyncio
from aio_pika import (
    IncomingMessage,
    Channel,
)
from .helpers import (
    uuid_str,
)
log = create_logger(__name__)


class RpcRequest:
    def __init__(
        self,
        channel: Channel,
        original_routing_key: str,
        rpc_timeout: int = RPC_TIMEOUT_SECONDS
    ):
        self.channel: Channel = channel
        self.queue_has_been_deleted: bool = False
        self.correlation_id: str = uuid_str()
        self.reply_to: str = uuid_str()
        self.original_routing_key: str = original_routing_key
        self.rpc_timeout: int = rpc_timeout
        self.queue: Queue = None

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

    def create_message_handler(self, handle_message: Callable[[IncomingMessage], None]) -> Callable[[IncomingMessage], None]:
        async def handle_rpc_message(message: IncomingMessage):
            if message.correlation_id == self.correlation_id:
                message.routing_key = self.original_routing_key
                await handle_message(message)
                await self.delete_queue()

        return handle_rpc_message

    async def timeout_request(self, send_osc_func):
        await asyncio.sleep(self.rpc_timeout)
        if self.queue_has_been_deleted is False:
            # send_osc_func(
            #     address, "Timed out waiting for AMQP rpc response.")
            # await self.delete_queue()
            # self.queue_has_been_deleted = True
            pass
