#!/usr/bin/env python3
import os
from os import walk
import re
from dateutil.parser import parse

# Функция получения даты из строки форматов DDMMYY, DD(sep)MM(sep)YY, DDMMYYYY, DD(sep)MM(sep)YYYY, YYYYMMDD, YYYY(sep)MM(sep)DD
# (sep) сепаратор может быть из набора [./-,*+]
# В неявной дате типа '\d{2}-\d{2}-\d{2}', если год слева, при вызове из функции параметр l_day надо установить в False
def getDate(string, l_day=True):
    l_year = ''
    str_date = ''
    
    string = string[:10]
        
    # форматируем строку без сепараторов
    if re.fullmatch('\d{8}', string):
        str_date = string[:2] if l_day else string[6:8]
        str_date = str_date + '-' + (string[2:4] if l_day else string[4:6])
        str_date = str_date + '-' + (string[4:8] if l_day else string[:4])
    elif re.fullmatch('\d{6}', string):
        str_date = string[:2] if l_day else string[4:6]
        str_date = str_date + '-' + string[2:4]
        str_date = str_date + '-' + (string[4:6] if l_day else string[:2])
    else:
        str_date = string
    
    # приводим к одному виду
    if re.fullmatch('(\d{2}|\d{4})[,*+-/.]\d{2}[,*+-/.](\d{2}|\d{4})', str_date):    
        str_date = re.sub('[,*+-/.]', '-', str_date)
    else:
        raise ValueError('unsupported date format: {0}'.format(str_date))
    
    # устанавливаем параметры для парсера
    if re.fullmatch('\d{2}-\d{2}-\d{4}', str_date):        
        l_day=True
        l_year = False
    elif re.fullmatch('\d{4}-\d{2}-\d{2}', str_date): 
        l_day=False
        l_year = True
    elif len(str_date) == 8:
        l_year = not l_day
    
    # проверяем месяц на валидность
    month = 0       
    if len(str_date) == 8:
        month = int(str_date[3:5])
    elif len(str_date) == 10:
        month = int(str_date[3:5] if l_day else str_date[5:7])
    
    if 1 < month > 12:
        raise ValueError('month must be in 1..12: {0}'.format(str_date))

    return (parse(str_date, dayfirst=l_day, yearfirst=l_year).date())

# Функция возвращает список имён файлов по дате (переданная или минимальная (по умолчанию)/максимальная)
def getFilesByDate(path, load_date=False, min_date=True):
    if os.path.isdir(path):
        # Собираем названия файлов в папке
        files = []
        for (_, _, filenames) in walk(path):
            files.extend(filenames)

        # Делаем справочник по датам
        result = {}
        for i in files:
            date = getDate(i[i.find('.') - 8:i.find('.')])
            try:
                result[date].append(i)
            except Exception:
                result[date] = []
                result[date].append(i)

        # Возвращаем количество записей в справочнике и справочник (по запрошенной или минимальной дате)
        if len(result) > 0:
            if load_date and load_date in result:
                result = result[load_date]
                return len(result), result, load_date
            elif not load_date:
                load_date = min(result) if min_date else max(result)
                result = result[load_date]
                return len(result), result, load_date
            else:
                raise OSError('002: нет файлов по запрошенной дате: {0}'.format(load_date))
        else:
            raise OSError('003: пустая папка: {0}'.format(path))
    else:
        raise OSError('001: неверный путь: {0}'.format(path))
