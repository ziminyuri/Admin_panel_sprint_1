import logging

import environ

logging.basicConfig(filename="apps.log", level=logging.INFO)

env = environ.Env()
environ.Env.read_env('.env')