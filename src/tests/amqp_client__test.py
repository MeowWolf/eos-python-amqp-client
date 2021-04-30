from unittest.mock import (
    AsyncMock,
    DEFAULT,
)
import pytest
import json
from eos_amqp_client.amqp_client import AmqpClient

rabbit_host = 'rabbit_host'
rabbit_port = 5672
rabbit_user_name = 'rabbit_user_name'
rabbit_pass = 'rabbit_pass'
routing_key_string = '#.project_code.#, #.project_code2.#'
exchange_name = 'exchange_name'


def test_init():
    amqp_client = AmqpClient(
        host=rabbit_host,
        port=rabbit_port,
        username=rabbit_user_name,
        password=rabbit_pass,
    )

    assert amqp_client.port is rabbit_port


@pytest.mark.asyncio
async def test_connect(mocker):
    connect_robust_mock = mocker.patch(
        'eos_amqp_client.amqp_client.connect_robust')

    amqp_client = AmqpClient(
        host=rabbit_host,
        port=rabbit_port,
        username=rabbit_user_name,
        password=rabbit_pass,
    )

    await amqp_client.connect()

    assert await connect_robust_mock.called_once()


@pytest.mark.asyncio
async def test_connect_exception(mocker):
    connect_robust_mock = mocker.patch(
        'eos_amqp_client.amqp_client.connect_robust')

    connect_robust_mock.side_effect = [Exception('no!!!'), DEFAULT]

    amqp_client = AmqpClient(
        host=rabbit_host,
        port=rabbit_port,
        username=rabbit_user_name,
        password=rabbit_pass,
    )

    await amqp_client.connect()

    assert await connect_robust_mock.called_twice()


@pytest.mark.asyncio
async def test_create_channel(mocker):
    mocker.patch('eos_amqp_client.amqp_client.connect_robust')

    amqp_client = AmqpClient(
        host=rabbit_host,
        port=rabbit_port,
        username=rabbit_user_name,
        password=rabbit_pass,
    )

    assert amqp_client.exchange is None
    await amqp_client.connect()
    await amqp_client.create_channel()

    assert amqp_client.exchange is not None


@pytest.mark.asyncio
async def test_publish(mocker):
    mocker.patch('eos_amqp_client.amqp_client.connect_robust')

    exchange_mock = AsyncMock()

    amqp_client = AmqpClient(
        host=rabbit_host,
        port=rabbit_port,
        username=rabbit_user_name,
        password=rabbit_pass,
    )

    amqp_client.exchange = exchange_mock
    payload = json.dumps(['ten', 10, False])

    await amqp_client.publish('gsc.routing.key', payload)
    assert await exchange_mock.publish.called_once()


@pytest.mark.asyncio
async def test_publish_with_rpc(mocker):
    RpcRequest_mock = mocker.patch(
        'eos_amqp_client.amqp_client.RpcRequest')
    rpc_request_mock = RpcRequest_mock()
    rpc_request_mock.declare_queue = AsyncMock()
    rpc_request_mock.queue.consume = AsyncMock()
    rpc_request_mock.timeout_request = AsyncMock()
    exchange_mock = AsyncMock()

    amqp_client = AmqpClient(
        host=rabbit_host,
        username=rabbit_user_name,
        password=rabbit_pass,
    )
    amqp_client.exchange = exchange_mock
    payload = json.dumps(['ten', 10, False])

    await amqp_client.publish(
        routing_key='gsc.routing.key',
        payload=payload,
        is_rpc=True
    )
    assert await rpc_request_mock.declare_queue.called_once()
    assert exchange_mock.publish.called is True
    assert rpc_request_mock.create_message_handler.called is True
    assert await rpc_request_mock.queue.consume.called_once()
    assert await rpc_request_mock.timeout_request.called_once()


@pytest.mark.asyncio
async def test_publish_with_rpc_exception(mocker):
    RpcRequest_mock = mocker.patch(
        'eos_amqp_client.amqp_client.RpcRequest')
    rpc_request_mock = RpcRequest_mock()
    rpc_request_mock.declare_queue = Exception('hey!!!')
    rpc_request_mock.queue.consume = AsyncMock()
    rpc_request_mock.timeout_request = AsyncMock()
    exchange_mock = AsyncMock()

    amqp_client = AmqpClient(
        host=rabbit_host,
        username=rabbit_user_name,
        password=rabbit_pass,
    )
    amqp_client.exchange = exchange_mock
    payload = json.dumps(['ten', 10, False])

    await amqp_client.publish(
        routing_key='gsc.routing.key',
        payload=payload,
        is_rpc=True
    )
    assert rpc_request_mock.timeout_request.called is False


@pytest.mark.asyncio
async def test_consume(mocker):
    mocker.patch('eos_amqp_client.amqp_client.connect_robust')

    amqp_client = AmqpClient(
        host=rabbit_host,
        port=rabbit_port,
        username=rabbit_user_name,
        password=rabbit_pass,
    )

    await amqp_client.connect()
    channel = await amqp_client.create_channel()

    def handle_message():
        return None

    await amqp_client.consume(
        channel=channel,
        routing_keys='to.consume',
        handle_message=handle_message,
    )

    assert channel.declare_queue.called is True


@pytest.mark.asyncio
async def test_consume_multi_routing_keys(mocker):
    mocker.patch('eos_amqp_client.amqp_client.connect_robust')

    amqp_client = AmqpClient(
        host=rabbit_host,
        port=rabbit_port,
        username=rabbit_user_name,
        password=rabbit_pass,
    )

    await amqp_client.connect()
    channel = await amqp_client.create_channel()

    def handle_message():
        return None

    await amqp_client.consume(
        channel=channel,
        routing_keys=['to.consume', 'to.also.consume'],
        handle_message=handle_message,
    )

    assert channel.declare_queue.called is True
