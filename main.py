#coding=utf-8
__author__ = "KomissarovAV"
__version__ = "0.15"
# программа разработа и протестирована при компиляторе python 3.4

import datetime
import os
import mysql.connector
from mysql.connector import MySQLConnection, errorcode
from python_mysql_dbconfig import read_db_config

# объявление класса 'Таблица'
class Table:
    def __init__(self, name, id_field):
        self.name = name
        self.id_field = id_field

# функция для возврата максимального элемента столбца
def GetMAX(table_name, column):
    return GetValue('MAX({})'.format(column), table_name)

# функция для возврата минимального элемента столбца
def GetMin(table_name, column):
    return GetValue('MIN({})'.format(column), table_name)

# функция для нахождение максимального/минимального элемента столбца
def GetValue(value, table_name):
    data = 0
    cursor.execute('SELECT {0} FROM {1}'.format(value, table_name))
    for i in cursor.fetchone():
        data = i
    return data

# функция по изменению значения столбца таблицы
def ResetCounter(table_name, column):
    fout.write(" - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -\n")
    min = GetMin(table_name, column)
    max = GetMAX(table_name, column)
    if (min == None):
        fout.write("Предупреждение: В столбце `{0}` таблицы `{1}` нет данных\n".format(column, table_name))
        return
    if (min == 1):
        print(min == 1)
        fout.write('Никаких изменений не требуется для столбца `{0}` в таблице `{1}`\n'
                   ' Минимальное значение столбца = {2}\n Максимальное значение столбца = {3}\n'
                   .format(column, table_name, min, max))
        return
    fout.write('Было произведено изменение значений столбца `{0}` в таблице `{1}`\n'
			   ' Минимальное значение столбца было = {2}\n Максимальное значение столбца было = {3}\n'
               .format(column, table_name, min, max))
    cursor.execute('Lock TABLES {0} WRITE;'.format(table_name))
    cursor.execute('UPDATE {0} SET {1} = {1} - {2} + 1'.format(table_name, column, min))
    cursor.execute('ALTER TABLE {0} AUTO_INCREMENT = 1'.format(table_name))
    cnx.commit()
    cursor.execute('UNLOCK TABLES;')

    fout.write("     -       -       -       -       -       -       -       -       -    \n"
    ' Минимальное значение столбца стало = {0}\n Максимальное значение столбца стало = {1}\n'
             .format(GetMin(table_name, column), GetMAX(table_name, column)))

# основная программа
now_time = datetime.datetime.now()
db_config = read_db_config()

# создание файла-отчета в рабочем каталоге программы
fout = open(os.getcwd()+'/logs.txt', 'a')
fout.write('Отчёт работы программы за {}\n      -       -       -       -       -       -       -       -       -    \n'
           .format(now_time.strftime('%d.%m.%Y %I:%M %p')))

try:
    cnx = MySQLConnection(**db_config)
    if cnx.is_connected():
        fout.write('Подключение к базе данных прошло успешно\n')
        cursor = cnx.cursor(buffered=True)
        # массив таблиц и их столбцов в базе данных Monitoring
        tables_list = [
                        Table('CalculationTasks', 'id'),
            #Table('EphemeridesGlonass', 'id'), Table('LoadedArchives', 'id'),
                       #Table("LoadedRinexFiles", "id"), Table("Messages", "message_id"), Table("SatellitesStatuses", "id"),
                       #Table("SatFiles", "id"), Table("Stations", "id"), Table("StationsReliability", "id"),Table("UEREs", "id")
                      ]
        for table in tables_list:
            ResetCounter(table.name, table.id_field)
        cursor.close()
        cnx.disconnect()
    else:
        fout.write('При подключении к базе данных произошла ошибка\n')


except mysql.connector.Error as err:
    fout.write(' - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * -\n')
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        fout.write('Что-то не так с вашим именем пользователя или паролем\n')
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        fout.write('Базы данных не существует\n')
    else:
        fout.write('Произошла ошибка\nError code: {0}\nError message: {1}\n'
                   .format(err.errno, err.msg))
finally:
    fout.write("==========================================================================\n\n")
    fout.close()
