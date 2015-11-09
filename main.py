# coding=utf-8
__author__ = "KomissarovAV"
__version__ = "0.10"

import datetime
import mysql.connector
from mysql.connector import errorcode

# для работы со временем
now_time = datetime.datetime.now()

# создаем новый файл
# создание файла на дозапись
fo = open("C:\Users\logs.txt", "a")

#для тестирования
#fo = open("C:\Users\logs.txt", "w")

# выходной файл нужен, чтобы проверить, когда в последний раз запускалась программа
fo.write ("Последний раз программа запускалась ")
fo.write (now_time.strftime("%d.%m.%Y %I:%M %p\n"))

# функция для работы в БД Monitoring
def Mysql_code(table,column):
    # заблокировать таблицу для чтения и записи
    cursor.execute('Lock TABLES %s%s' % (table,' READ'))
    cursor.execute('Lock TABLES %s%s' % (table,' WRITE'))
    # назначение дополнительной переменной = минимальному значению столбца
    cursor.execute('SELECT @min:=MIN( %s %s' % ( column,') FROM %s%s' % (table,';')))
    # изменение значений столбца
    cursor.execute('UPDATE %s%s' % (table,' SET  %s%s' % (table,'.%s%s' % ( column,' =  %s%s' % (table,'.%s%s' % (column,'-@min+1;'))))))
    # сброс счетчика
    cursor.execute('ALTER TABLE  %s%s' %( table,' AUTO_INCREMENT = 1;'))
    # сохранение изменений в базе данных
    db.commit()
    # разблокировать таблицу для чтения и записи
    cursor.execute('UNLOCK TABLES;')

# функция для определения переполнения столбца в БД Monitoring
def func(table,column):
    # нахождение минимального элемента столбца
    cursor.execute('SELECT MIN( %s %s' % ( column,') FROM %s%s' % (table,';')))
    for i in cursor.fetchone():
        min = i
    # нахождение максимального элемента столбца
    cursor.execute('SELECT MAX( %s %s' % ( column,') FROM %s%s' % (table,';')))
    for i in cursor.fetchone():
        max = i
    if (min > 1):
        fo.write ('Было произведено изменение значений столбца %s%s' % ( column, ' в таблице %s %s' % (table, '\n')))
        fo.write ('Минимальное значение стобцаа %s%s' % ( column, ' = %s %s' % (min, '\n')))
        fo.write ('Максимальное значение стобцаа %s%s' % ( column, ' = %s %s' % (max, '\n')))
        Mysql_code(table, column)
    else:
        fo.write ('Никаких изменений не требуется для столбца %s%s' % ( column, ' в таблице %s %s' % (table, '\n')))
        fo.write ('Минимальное значение стобцаа %s%s' % ( column, ' = %s %s' % (min, '\n')))
        fo.write ('Максимальное значение стобцаа %s%s' % ( column, ' = %s %s' % (max, '\n')))

# основная программа
try:
    db =  mysql.connector.connect(host="128.0.0.1",
                            #host="192.168.12.241",  # имя хаста / IP
                            #user="monitoring",      # пользователь
                             user="root",
                            #passwd="monitoring",    # пароль
                             passwd="",
                             db="Monitoring")        # имя вашей базы данных

    # проверка, прошло ли подключение
    if db.is_connected():
        fo.write ("Подключение к базе данных прошло успешно\n")

    # подготовка объекта курсор
    cursor = db.cursor()

    # массив таблиц в базе данных Monitoring
    array = ["SatFiles","Stations"]
    #array = ["CalculationTasks","EphemeridesGlonass","LoadedArchives","LoadedRinexFiles","Messages","SatellitesStatuses","SatFiles","Stations","StationsReliability","UEREs"]
    for table in array:
        # отделение данных для наглядности
        fo.write(" - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -\n")
        if (table == "Messages"):
            column = "message_id"
            func(table,column)
        else:
            column = "id"
            func(table,column)

    # отключение курсора
    cursor.close()
    # отсоединить от сервера
    db.close()

# вывод ошибки
except mysql.connector.Error as e:
    fo.write ("Подключение к базе данных не произошло - ПРОИЗОШЛА ОШИБКА\n")
    # код ошибки
    fo.write ("Error code: %s%s" % ( e.errno, '\n'))
    # его SQLSTATE значение
    fo.write ("SQLSTATE value: %s%s" % ( e.sqlstate, '\n'))
    # сообщение об ошибке
    fo.write ("Error message: %s%s" % ( e.msg, '\n'))

# завершение программы
finally:
    fo.write("========================================================================\n\n")
    # закрать текстовый файл
    fo.close()
