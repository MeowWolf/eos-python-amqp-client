from callee import (
    String,
)
import pytest
from aio_pika import (
    IncomingMessage,
)
from eos_amqp_client.rpc_request import RpcRequest

channel = "I'm a channel"
original_routing_key = '#.project_code.#'
rpc_timeout = 8


class AmqpChannelStub():
    def __init__(self):
        pass

    async def declare_queue(self, name, auto_delete, exclusive, durable):
        pass

    async def queue_delete(self, name):
        pass


class MessageStub():
    correlation_id = None
    routing_key = None

    def __init__(self):
        pass


def test_init(mocker):
    uuid_str_mock = mocker.patch('eos_amqp_client.rpc_request.uuid_str')
    rpc_request = RpcRequest(
        channel=channel,
        original_routing_key=original_routing_key,
        rpc_timeout=rpc_timeout,
    )

    assert rpc_request.channel is channel
    assert rpc_request.original_routing_key is original_routing_key
    assert rpc_request.rpc_timeout is rpc_timeout
    uuid_str_mock.assert_called()


@pytest.mark.asyncio
async def test_declare_queue(mocker):
    channelStub = AmqpChannelStub()
    declare_queue_spy = mocker.spy(channelStub, 'declare_queue')

    rpc_request = RpcRequest(
        channel=channelStub,
        original_routing_key=original_routing_key,
        rpc_timeout=rpc_timeout,
    )

    await rpc_request.declare_queue()
    declare_queue_spy.assert_called_once_with(
        name=String(),
        auto_delete=True,
        exclusive=True,
        durable=False,
    )


@pytest.mark.asyncio
async def test_delete_queue(mocker):
    channelStub = AmqpChannelStub()
    queue_delete_spy = mocker.spy(channelStub, 'queue_delete')

    rpc_request = RpcRequest(
        channel=channelStub,
        original_routing_key=original_routing_key,
        rpc_timeout=rpc_timeout,
    )

    await rpc_request.delete_queue()

    queue_delete_spy.assert_called_once_with(
        name=String(),
    )
    assert rpc_request.queue_has_been_deleted is True


@pytest.mark.asyncio
async def test_create_message_handler(mocker):
    channelStub = AmqpChannelStub()
    queue_delete_spy = mocker.spy(channelStub, 'queue_delete')

    async def handle_message(message: IncomingMessage):
        pass

    rpc_request = RpcRequest(
        channel=channelStub,
        original_routing_key=original_routing_key,
        rpc_timeout=rpc_timeout,
    )

    message = MessageStub()
    message_handler = rpc_request.create_message_handler(handle_message)

    # message that is not for us
    message.correlation_id = 'no sir'
    await message_handler(message)
    queue_delete_spy.assert_not_called()
    assert rpc_request.queue_has_been_deleted is False

    # message that is for us
    message.correlation_id = rpc_request.correlation_id
    await message_handler(message)
    queue_delete_spy.assert_called_once_with(
        name=String(),
    )
    assert rpc_request.queue_has_been_deleted is True


@pytest.mark.asyncio
async def test_timeout_request(mocker):
    channelStub = AmqpChannelStub()
    queue_delete_spy = mocker.spy(channelStub, 'queue_delete')
    mocker.patch('asyncio.sleep')

    rpc_request = RpcRequest(
        channel=channelStub,
        original_routing_key=original_routing_key,
        rpc_timeout=rpc_timeout,
    )

    # queue has already been deleted
    rpc_request.queue_has_been_deleted = True
    await rpc_request.timeout_request('correlation_id', 'reply_to')
    queue_delete_spy.assert_not_called()

    # queue has not yet been deleted
    rpc_request.queue_has_been_deleted = False
    await rpc_request.timeout_request('correlation_id', 'reply_to')
    queue_delete_spy.assert_called()
    assert rpc_request.queue_has_been_deleted is True
