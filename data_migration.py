from sqlalchemy import create_engine, MetaData, inspect, Table
from sqlalchemy.engine.url import URL
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import pandas as pd

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


def comparison_tables_rows():
    """Проверка одинаковое ли кол-во строк в двух таблицах"""

    database_1 = input('Введите бд для подключения: ')
    connect_db(database_1)
    all_tables = inspect(engine).get_table_names()
    print('Таблицы: ', end='')
    print(*all_tables, sep=', ')
    user_table = input('Укажите названия таблицы: ')
    my_table = Table(user_table, metadata, autoload=True, autoload_with=engine)
    database_1_count_rows = s.query(my_table).count()
    print(f'Количество строк в {user_table} {database_1_count_rows}')

    database_2 = input('Введите бд для подключения: ')
    connect_db(database_2)
    all_tables = inspect(engine).get_table_names()
    print('Таблицы: ', end='')
    print(*all_tables, sep=', ')
    user_table = input('Укажите названия таблицы: ')
    my_table = Table(user_table, metadata, autoload=True, autoload_with=engine)
    database_2_count_rows = s.query(my_table).count()
    print(f'Количество строк в {user_table} {database_2_count_rows}')

    return database_1_count_rows == database_2_count_rows


def insert_csv_xlsx_table():
    """Insert данных в таблицу на основе информации о её названии, пути к файлу, где хранятся вносимые данные"""

    database = input('Введите бд для подключения: ')
    connect_db(database)

    print('Таблицы:', end=' ')
    print(*inspect(engine).get_table_names(), sep=', ')
    table_name = input("Укажите название таблицы: ")
    type_file = input('Укажите тип файла(csv/xlsx): ')
    filepath = input('Укажите путь до файла: ')

    if type_file == 'csv':
        df = pd.read_csv(filepath, header=0, sep=',', encoding='utf8')
        df.to_sql(table_name, con=engine, index=False, if_exists='append')
    elif type_file == 'xlsx':
        df = pd.read_excel(filepath)
        df.to_sql(table_name, con=engine, index=False, if_exists='append')

    print(f'Данные в таблицу {table_name} успешно внесены')


def table_exists():
    """проверка существует ли таблица в бд"""

    database = input('Введите название бд: ')
    connect_db(database)
    name_table = input('Введите название таблицы: ')
    print(inspect(engine).has_table(name_table))


def transfer_table():
    """перенос таблицы из одной бд в другую"""

    database = input('Укажите из какой бд перенести таблицу: ')
    connect_db(database)
    input_table = input('Укажите названия таблицы для переноса: ')
    input_db = input('Укажите в какую бд перенести таблицу: ')
    engine.execute(f'CREATE TABLE {input_db}.{input_table} SELECT * FROM {database}.{input_table};')
    table = Table(input_table, metadata, autoload=True)
    table.drop(engine)
    print(f'Таблица {input_table} успешно перенесена')