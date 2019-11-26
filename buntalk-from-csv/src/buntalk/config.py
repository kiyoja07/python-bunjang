import os
from dotenv import load_dotenv


load_dotenv()

BUNTALK_HOST = os.getenv('BUNTALK_HOST', '')
