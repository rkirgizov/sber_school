#!/usr/bin/env python3
import configparser
config = configparser.ConfigParser()
config_file = '/home/de3at/kela/config.ini'
config.read(config_file)

# загружаем конфиги в переменные окружения для использования в других модулях
import os
os.environ["ROOT_PATH"] = config['KELA']['root_path']
os.environ["INIT_COMPLETED"] = config['KELA']['init_completed']
os.environ["STAGE_LIFE"] = config['KELA']['stage_life']
import sys
sys.path.insert(1, '{0}/py_scripts'.format(os.environ["ROOT_PATH"]))

# kela modules
from logger import KelaLogger
log = KelaLogger('kela.log').get_logger(__name__)
import utils
import init
import etl
import report

if __name__ == '__main__':
    cmd = ''
    if len(sys.argv) > 1:
        cmd = sys.argv[1]
    else: 
        s = input('\n   Kela Project\n   1. Загрузка данных (etl)\n   2. Формирование отчёта (report)\n   7. Инициализация хранилища (init)\n   0. Выход (exit)\n   Введите команду или цифру: ')
        cmd = s
    try:
        if cmd in ['etl', '1']:
            if os.environ["INIT_COMPLETED"] == '1':
                s = input('   Введите дату загрузки: ')
                if s != '':
                    try:
                        load_date = utils.getDate(s)
                        s = input('   Будут загружены данные за {0}. Начать загрузку? Да-Yes-Enter/...: '.format(load_date))
                        if s in ['Yes', 'Y', 'yes', 'y', 'Да', 'да', 'Д', 'д', '']:
                            log.info('   ЗАГРУЗКА ДАННЫХ ЗА {0}...'.format(load_date))
                            etl.start(load_date)
                        else:
                            log.info("   Загрузка данных отменена!")                        
                    except ValueError as ve:
                        log.warning('   Ошибка формата даты! ValueError: {0}'.format(str(ve)))
                        raise
                else:
                    s = input('   Будут загружены все данные по датам. Начать загрузку? Да-Yes-Enter/...')
                    if s in ['Yes', 'Y', 'yes', 'y', 'Да', 'да', 'Д', 'д', '']:
                        etl.start()
                    else:
                        log.info("   Загрузка данных отменена!")                          
            else:
                log.info("   Ошибка! Инициализация хранилища не завершена. Для инициализации хранилища используйте команду (init).")
        elif cmd in ['report', '2']:
            report.start()
        elif cmd in ['init', '7']:
            s = input('   Инициализация хранилища данных проекта.\n   Старые данные будут уничтожены!\n   Вы уверены? Да-Yes-Enter/...: ')
            if s in ['Yes', 'Y', 'yes', 'y', 'Да', 'да', 'Д', 'д', '']:
                config['KELA']['init_completed'] = '0'
                with open(config_file, 'w') as configfile:
                    config.write(configfile)
                init.start()
            else:
                log.info("   Инициализация хранилища отменена!")
        elif cmd in ['exit', '0']:
            pass
        else:
            raise NotImplementedError('Unknown command')
    except NotImplementedError as nie:
        log.info('   Неверная команда! Допустимые команды: (etl, report, init, exit) Err: {0}'.format(str(nie)))
    except OSError as ose:
        log.info('   Err: {0}'.format(str(ose)))
    except Exception:
        raise
    
