#coding=utf-8
__author__ = "KomissarovAV"

import datetime, sys, os, mysql.connector
from mysql.connector import MySQLConnection, errorcode
from python_mysql_dbconfig import read_db_config

class Table:
    def __init__(self, name, id_field):
        self.name = name
        self.id_field = id_field

    # функция по изменению значения столбца таблицы
    def ResetCounter(self):
        print(' - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -')
        min = self.GetMin()
        max = self.GetMAX()
        if (min == None):
            print('Предупреждение: В столбце `{0}` таблицы `{1}` нет данных\n'.format(table.id_field, table.name))
            return
        if (min == 1):
            print('Никаких изменений не требуется для столбца `{0}` в таблице `{1}`\n'
                  ' Минимальное значение столбца = {2}\n Максимальное значение столбца = {3}'
                  .format(table.id_field, table.name, min, max))
            return
        print('Было произведено изменение значений столбца `{0}` в таблице `{1}`\n'
              ' Минимальное значение столбца было = {2}\n Максимальное значение столбца было = {3}'
              .format(table.id_field, table.name, min, max))
        cursor.execute('UPDATE {0} SET {1} = {1} - {2} + 1'.format(table.name, table.id_field, min))
        cursor.execute('ALTER TABLE {0} AUTO_INCREMENT = 1'.format(table.name))
        cnx.commit()
        print('     -       -       -       -       -       -       -       -       -    \n'
              ' Минимальное значение столбца стало = {0}\n Максимальное значение столбца стало = {1}'
              .format(self.GetMIN(), self.GetMAX()))

    # функция для возврата минимального элемента столбца
    def GetMin(self):
        return self.GetValue('MIN({})'.format(table.id_field))

    # функция для возврата максимального элемента столбца
    def GetMAX(self):
        return self.GetValue('MAX({})'.format(table.id_field))

    # функция для нахождение максимального/минимального элемента столбца
    def GetValue(self, value):
        data = 0
        cursor.execute('SELECT {0} FROM {1}'.format(value, table.name))
        for i in cursor.fetchone():
            data = i
        return data

now_time = datetime.datetime.now()
# создание файла-отчета в рабочем каталоге программы
old_stdout = sys.stdout
sys.stdout = open(os.getcwd()+'/logs.txt', 'w')
print('Отчёт работы программы {}\n      -       -       -       -       -       -       -       -       -    '
      .format(now_time.strftime('%d.%m.%Y %I:%M %p')))

try:
    db_config = read_db_config()
    cnx = MySQLConnection(**db_config)
    print('Подключение к базе данных прошло успешно')
    cursor = cnx.cursor(buffered=True)
    tables_list = [
                   Table('CalculationTasks', 'id'), Table('EphemeridesGlonass', 'id'), Table('LoadedArchives', 'id'),
                   Table("LoadedRinexFiles", "id"), Table("Messages", "message_id"), Table("SatellitesStatuses", "id"),
                   Table("SatFiles", "id"), Table("Stations", "id"), Table("StationsReliability", "id"), Table("UEREs", "id")
                  ]
    for table in tables_list:
        # блокировка, вызов функции изменения и разблокировка таблицы
        cursor.execute('Lock TABLES {0} WRITE'.format(table.name))
        table.ResetCounter()
        cursor.execute('UNLOCK TABLES')
    cursor.close()
    cnx.disconnect()

except mysql.connector.Error as err:
    print(' - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * -\n'
          'Произошла ошибка:\n Error code: {0}\n Error message: {1}'.format(err.errno, err.msg))
finally:
    print('==========================================================================\n')
    sys.stdout.close()
    sys.stdout = old_stdout
