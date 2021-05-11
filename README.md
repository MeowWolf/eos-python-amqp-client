# EOS Python AMQP Client

## Installation

```sh
pip install git+https://github.com/MeowWolf/eos-python-amqp-client.git
```

## Usage

### Initialize and connect

```python
from eos_amqp_client import (
    AmqpClient,
)

async def main():
    # Initialize
    amqp = AmqpClient(
        host='localhost',
        port=5672,
        username='rabbitmq',
        password='rabbitmq',
        exchange_name='amq.topic',
        exchange_type='topic',
    )

    # Connect
    await amqp.connect()

    # Create a channel
    chan = await amqp.create_channel()
```

### Publish a message

```python
async def main():
    await amqp.publish(
        routing_key='publish.to',
        payload="{'test': 'message'}",
        channel=chan,
    )
```

### Publish a message with rpc response

```python
from eos_amqp_client import (
    IncomingMessage,
)

async def handle_message(message: IncomingMessage):
    # context manager handles acking for us
    async with message.process():
        try:
            payload = message.body.decode('utf-8')
            routing_key = message.routing_key

            print(
                f'Received amqp message: {routing_key}: {payload}')

        except Exception as err:  # pragma: no cover
            print(f'Error handling incoming AMQP message: {err}')


async def main():
    await amqp.publish(
        routing_key='publish.to',
        payload="{'test': 'message'}",
        channel=chan,
        is_rpc=True,
        handle_rpc_message=handle_message
    )
```

### Set up a consumer

```python
import asyncio
from eos_amqp_client import (
    IncomingMessage,
)

async def handle_message(message: IncomingMessage):
    # handle message
    # don't forget to ack! (see rpc message handler above for example)
    pass

async def main():
    asyncio.get_event_loop().create_task(
        amqp.consume(
            channel=chan,
            routing_keys=['super.test', 'super.wolf'],
            handle_message=handle_message,
            queue_name='python-client-test',
        )
    ),
```

### Set up multiple consumers

```python
import asyncio
from eos_amqp_client import (
    IncomingMessage,
)

async def handle_message(message: IncomingMessage):
    # handle message
    # don't forget to ack! (see rpc message handler above for example)
    pass

async def main():
    await asyncio.gather(
        amqp.consume(
            channel=chan,
            routing_keys=['super.test', 'super.wolf'],
            handle_message=handle_message,
            queue_name='python-client-test',
        ),
        amqp.consume(
            channel=chan,
            routing_keys=['other.test', 'swear.wolf'],
            handle_message=handle_message,
            queue_name='python-client-test2',
        )
    )
```

### Run using the event loop

```python
import asyncio

loop = asyncio.get_event_loop()
try:
    # the main function as used in the above examples
    loop.create_task(main())
except:
    print("Something did not go according to plan.")

loop.run_forever()
```

## Run the tests

Use vscode.
Make sure you've got the `ms-python.python` extension installed and run the tests via the test pane in the editor.
Coverage is generated in an `htmlcov` directory in the root of the project
