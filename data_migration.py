from sqlalchemy import create_engine, MetaData, inspect, Table
from sqlalchemy.engine.url import URL
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker


def connect_db(database):
    """Подключение к бд на основе информации о параметрах подключения"""
    DATABASE = {
        'drivername': 'mysql',
        'host': 'localhost',
        'port': '3306',
        'username': 'root',
        'password': '1234',
        'database': database
    }

    global engine, DeclarativeBase, metadata, session, s

    engine = create_engine(URL.create(**DATABASE))

    DeclarativeBase = declarative_base()
    DeclarativeBase.metadata.create_all(engine)

    metadata = MetaData(engine)

    session = sessionmaker(bind=engine)
    s = session()


