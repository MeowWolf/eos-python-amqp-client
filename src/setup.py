from setuptools import setup

setup(
    name='eos_amqp_client',
    version='0.0.1',
    description='An amqp client written in python to be used by other EOS software components',
    url='git@github.com:MeowWolf/',
    install_requires=[
        'aio-pika >= 6.8.0'
    ],
    author='Amo DelBello',
    author_email='adelbello@meowwolf.com',
    license='unlicensed',
    packages=['eos_amqp_client'],
    zip_safe=False
)
