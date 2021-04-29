from setuptools import find_packages, setup

setup(
    name='eos_amqp_client',
    version='0.0.1',
    description='An amqp client written in python to be used by other EOS software components',
    url='git@github.com:MeowWolf/eos-python-client.git',
    install_requires=[
        'aio-pika >= 6.8.0'
    ],
    author='Amo DelBello',
    author_email='adelbello@meowwolf.com',
    license='MIT',
    packages=find_packages(
        where='src',
        include=['eos_amqp_client', ],
        exclude=['tests', ]
    ),
    package_dir={"": "src"},
)
