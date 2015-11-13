#coding=utf-8
__author__ = "KomissarovAV"
__version__ = "0.16"
# программа разработа и протестирована при компиляторе python 3.4

import datetime, sys, os, mysql.connector
from mysql.connector import MySQLConnection, errorcode
from python_mysql_dbconfig import read_db_config

# объявление класса 'Таблица'
class Table:
    def __init__(self, name, id_field):
        self.name = name
        self.id_field = id_field

    # функция по изменению значения столбца таблицы
    def ResetCounter(self):
        print(" - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -")
        min = Table.GetMin(self)
        max = Table.GetMAX(self)
        if (min == None):
            print("Предупреждение: В столбце `{0}` таблицы `{1}` нет данных\n".format(table.id_field, table.name))
            return
        if (min == 1):
            print('Никаких изменений не требуется для столбца `{0}` в таблице `{1}`\n'
                       ' Минимальное значение столбца = {2}\n Максимальное значение столбца = {3}'
                       .format(table.id_field, table.name, min, max))
            return
        print('Было произведено изменение значений столбца `{0}` в таблице `{1}`\n'
                   ' Минимальное значение столбца было = {2}\n Максимальное значение столбца было = {3}'
                   .format(table.id_field, table.name, min, max))
        cursor.execute('Lock TABLES {0} WRITE;'.format(table.name))
        cursor.execute('UPDATE {0} SET {1} = {1} - {2} + 1'.format(table.name, table.id_field, min))
        cursor.execute('ALTER TABLE {0} AUTO_INCREMENT = 1'.format(table.name))
        cnx.commit()
        cursor.execute('UNLOCK TABLES;')

        print("     -       -       -       -       -       -       -       -       -    \n"
        ' Минимальное значение столбца стало = {0}\n Максимальное значение столбца стало = {1}'
                 .format(Table.GetMin(self), Table.GetMAX(self)))

    # функция для возврата максимального элемента столбца
    def GetMAX(self):
        return Table.GetValue('MAX({})'.format(table.id_field))

    # функция для возврата минимального элемента столбца
    def GetMin(self):
        return Table.GetValue('MIN({})'.format(table.id_field))

    # функция для нахождение максимального/минимального элемента столбца
    def GetValue(value):
        data = 0
        cursor.execute('SELECT {0} FROM {1}'.format(value, table.name))
        for i in cursor.fetchone():
            data = i
        return data

# основная программа
now_time = datetime.datetime.now()
db_config = read_db_config()

# создание файла-отчета в рабочем каталоге программы
sys.stdout = open(os.getcwd()+'/logs.txt', 'w')
print('Отчёт работы программы за {}\n      -       -       -       -       -       -       -       -       -    '
           .format(now_time.strftime('%d.%m.%Y %I:%M %p')))

try:
    cnx = MySQLConnection(**db_config)
    if cnx.is_connected():
        print('Подключение к базе данных прошло успешно')
        cursor = cnx.cursor(buffered=True)
        # массив таблиц и их столбцов в базе данных Monitoring
        tables_list = [
                        Table('CalculationTasks', 'id'), Table('EphemeridesGlonass', 'id'), Table('LoadedArchives', 'id'),
                        Table("LoadedRinexFiles", "id"), Table("Messages", "message_id"), Table("SatellitesStatuses", "id"),
                        Table("SatFiles", "id"), Table("Stations", "id"), Table("StationsReliability", "id"),Table("UEREs", "id")
                      ]
        for table in tables_list:
            table.ResetCounter()
        cursor.close()
        cnx.disconnect()
    else:
        print('При подключении к базе данных произошла ошибка\n')

except mysql.connector.Error as err:
    print(' - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * -')
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print('Что-то не так с вашим именем пользователя или паролем')
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print('Базы данных не существует')
    else:
        print('Произошла ошибка\nError code: {0}\nError message: {1}'
                   .format(err.errno, err.msg))
finally:
    print("==========================================================================\n")
    sys.stdout.close()
