from dotenv import load_dotenv, find_dotenv
import os

load_dotenv(find_dotenv())
print(os.getenv('SECRET_KEY'))
class ApplicationConfig:
    SECRET_KEY = 'baomat'
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    SQLALCHEMY_ECHO = True
    SQLALCHEMY_DATABASE_URI = "sqlite:///./db.sqlite"
    SESSION_TYPE = 'sqlalchemy'