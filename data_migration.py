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


def drop_table():
    """Удаление таблицы"""
    database = input('Введите бд для подключения: ')
    connect_db(database)

    all_tables = inspect(engine).get_table_names()
    print('Таблицы: ')
    print(*all_tables, sep=', ')
    name_table = input("Укажите название таблицы для удаления: ")
    if name_table in all_tables:
        table = Table(name_table, metadata, autoload=True)
        table.drop(engine)
        print('Таблица удалена')
    else:
        print('Такой таблицы нет')


def transliteration():
    """Транслит названия таблицы и её столбцов"""

    database = input('Введите бд для подключения: ')
    connect_db(database)

    def check_special_chars(text):
        """Замена спецсимолов в строке на '_' """
        special_chars = r' /[]~!@#$%^&*()?<>+{}"`:;.,\''
        if not set(special_chars).isdisjoint(text) == True:
            for c in special_chars:
                text = text.replace(c, "_")
            return text
        else:
            return text

    def translit(text):
        """транслит входящего текста"""
        cyrillic = 'абвгдеёжзийклмнопрстуфхцчшщъыьэюяАБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯ'
        latin = 'a|b|v|g|d|e|e|zh|z|i|i|k|l|m|n|o|p|r|s|t|u|f|kh|tc|ch|sh|shch||y||e|iu|ia|A|B|V|G|D|E|E|Zh|Z|I|I|K|L|M|N|O|P|R|S|T|U|F|Kh|Tc|Ch|Sh|Shch||Y||E|Iu|Ia'.split('|')
        return (text.translate({ord(k): v for k, v in zip(cyrillic, latin)}))

    def translit_table_columns():
        """Транслит названия таблицы и названия столбцов"""
        all_tables = inspect(engine).get_table_names()
        print('Таблицы: ', end='')
        print(*all_tables, sep=', ')
        name_table = input("Укажите название таблицы для транслита: ")
        table_rename = Table(name_table, metadata, autoload=True)
        table_rename.rename(translit(check_special_chars(name_table)))
        print('Таблица переименована')

        columns_table = table_rename.c.keys()
        for col in columns_table:
             engine.execute(f"ALTER TABLE {table_rename} RENAME COLUMN `{col}` TO `{translit(check_special_chars(col))}`")
        print('Столбцы переименованы')

    translit_table_columns()