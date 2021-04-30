# EOS Python AMQP Client

### Quick & Dirty Example Usaage
(An actual README will be written soon)
```python
import asyncio
from eos_amqp_client import (
    AmqpClient,
    IncomingMessage,
)


async def handle_message(message: IncomingMessage):
    # context manager handles acking for us
    async with message.process():
        try:
            # we don't want to send a message back to ourselves
            payload = message.body.decode('utf-8')
            routing_key = message.routing_key

            print(
                f'Received amqp message: {routing_key}: {payload}')

        except Exception as err:  # pragma: no cover
            print(f'Error handling incoming AMQP message: {err}')


async def main():
    amqp = AmqpClient(
        host='localhost',
        port=5672,
        username='rabbitmq',
        password='rabbitmq',
        exchange_name='amq.topic',
        exchange_type='topic',
    )
    # await amqp.connect(asyncio.get_event_loop())
    await amqp.connect()
    print(f'this is conn {amqp.connection}')
    chan = await amqp.create_channel()
    print(f'this is chan {chan}')

    print('hello')
    await amqp.publish(
        # routing_key='test.okay',
        routing_key='device.codeA.get',
        payload="{'test': 'message'}",
        is_rpc=True,
        channel=chan,
        handle_rpc_message=handle_message
    )
    print('world')

    # await amqp.consume(chan, ['super.test', 'super.wolf'], handle_message)
    await asyncio.gather(
        # loop.create_task(
        amqp.consume(
            channel=chan,
            routing_keys=['super.test', 'super.wolf'],
            handle_message=handle_message,
            queue_name='python-client-test',
        ),
        # )
        # loop.create_task(
        amqp.consume(
            channel=chan,
            routing_keys=['other.test', 'super.wolf'],
            handle_message=handle_message,
            queue_name='python-client-test2',
        )
    )

# loop.create_task(main())

print('oh hi')
loop = asyncio.get_event_loop()
try:
    loop.create_task(main())
except:
    print("yayayayaya!!!!!!!!!")

loop.run_forever()

# asyncio.create_task(asyncio.get_event_loop().run_until_complete(main()))
# asyncio.create_task(rpc_request.timeout_request())
# asyncio.run(main())

# def start(self):
#     try:
#         loop = asyncio.get_event_loop()
#         loop.run_until_complete(self.run())
#         loop.run_forever()
#     except KeyboardInterrupt:  # pragma: no cover
#         log.info(f'asyncio loop closed')

```
