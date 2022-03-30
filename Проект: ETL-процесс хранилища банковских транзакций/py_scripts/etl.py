#!/usr/bin/env python3
import os
import sys
import pandas as pd
import configparser
config = configparser.ConfigParser()
config_file = '/home/de3at/kela/config.ini'
config.read(config_file)
os.environ["ROOT_PATH"] = config['KELA']['root_path']
os.environ["INIT_COMPLETED"] = config['KELA']['init_completed']
os.environ["STAGE_LIFE"] = config['KELA']['stage_life']
sys.path.insert(1, '{0}/py_scripts'.format(os.environ["ROOT_PATH"]))




# kela modules
import init
import utils
import report
import msql
from msql import KelaSQL
from logger import KelaLogger

log = KelaLogger('kela.log').get_logger(__name__)
log_f = KelaLogger('kela.log').get_logger(__name__ + '_f', True)

path_data = '{0}/data/'.format(os.environ["ROOT_PATH"])
path_arch = '{0}/archive/'.format(os.environ["ROOT_PATH"])

def start(load_date=False):
    msql.conn = msql.get_conn()   
    with msql.conn as conn:
        conn.jconn.setAutoCommit(False)
        try:
            log.info('   ЗАГРУЗКА ВСЕХ ДАННЫХ...')
            log.info("   1. Обрабатываем данные из базы Oracle")
            # Очистка временных хранилищ, stage_life хранится в config.ini
            log.info("   Очистка временных хранилищ...")
            source = {
                'kela_stg_accounts_del',
                'kela_stg_cards_del',
                'kela_stg_clients_del',
            }
            sqlTruncate = KelaSQL('truncate', source)
            sqlTruncate.execute()
            del sqlTruncate
            condition = "WHERE load_dt <= (current_date - INTERVAL '{0}' DAY)".format(int(os.environ["STAGE_LIFE"]))
            source = {
                'kela_stg_accounts': condition,
                'kela_stg_cards': condition,
                'kela_stg_clients': condition,
            }
            sqlDelete = KelaSQL('delete', source)
            sqlDelete.execute()
            del sqlDelete 
            # ETL из базы данных обрабатываем из скрипта
            log.info("   Загрузка и трансформация данных в DWH...")
            fd = open('{0}/sql_scripts/dml_main.sql'.format(os.environ["ROOT_PATH"]), 'r')
            sqlFile = fd.read()
            fd.close()
            source = sqlFile.split(';')
            sqlScript = KelaSQL('script', source)
            sqlScript.execute()
            del sqlScript

            log.info("   2. Обрабатываем файлы с данными {0} ...".format('{0}/data'.format(os.environ["ROOT_PATH"])))            
            def processFiles (load_date):
                # Чтение файлов по расширению
                def getDFByFileExtension(file, sepa=','):
                    file_ext = file[file.find('.') + 1:]
                    if file_ext == 'xlsx':
                        return pd.read_excel(path_data + file)
                    if file_ext == 'txt':
                        return pd.read_csv(path_data + file, sep=sepa)        
                count, files, file_date = utils.getFilesByDate(path_data, load_date)
                
                if count  >= 1:
                    # Очищаем временные хранилища
                    log.info("   Очистка временных хранилищ...")
                    log.info("   Данные за {0}".format(str(file_date)))
                    condition = "WHERE load_dt <= (current_date - INTERVAL '{0}' DAY)".format(int(os.environ["STAGE_LIFE"]))
                    source = {
                        'kela_stg_pssprt_blcklst': condition,        
                        'kela_stg_terminals': condition, 
                        'kela_stg_terminals_del': '',
                        'kela_stg_transactions': condition,      
                    }
                    sqlDelete = KelaSQL('delete', source)
                    sqlDelete.execute()
                    del sqlDelete           
                     
                    log.info("   Загрузка и трансформация данных в DWH...")
                    for file in files:
                        df = getDFByFileExtension(file)
                       
                        # Паспорта
                        if file.find('passport_blacklist') >= 0:
                            log_f.info("   Обрабатываем файл {0} ...".format(file))
                            try:
                                df = getDFByFileExtension(file)
                                # Собираем стейдж, фильтруем по паспорту, так как в источнике не новые записи, а снимок
                                values_list = df.fillna('').values.tolist()
                                for i in values_list:
                                    i[0] = str(i[0])    # timestamp из xlsx в строку, иначе ошибка
                                source = {
                                    'table': 'kela_stg_pssprt_blcklst',
                                    'fields': 'entry_dt, passport_num', 
                                    'values_list': values_list,
                                    'conditions': 'passport_num',
                                } 
                                sqlInsertAll = KelaSQL('insert_all', source)
                                sqlInsertAll.execute()
                                del sqlInsertAll
                                # Загрузка в DWH всех новых паспортов
                                source = {
                                    'table': 'kela_dwh_dim_pssprt_blcklst',
                                    'using': 'kela_stg_pssprt_blcklst',
                                    'conditions': 'src.passport_num = tgt.passport_num',
                                    'matched_update': {},
                                    'not_matched_insert': {
                                        'fields': 'passport_num, entry_dt',
                                        'values': "src.passport_num, to_date(src.entry_dt, 'YYYY-MM-DD HH24:Mi:SS')",   
                                        'insert_conditions': '1=1' 
                                    }
                                }
                                sqlMergeTable = KelaSQL('merge_table', source)
                                sqlMergeTable.execute()
                                del sqlMergeTable
                                # Обновляем метаданные, за дату обновления берём дату файла
                                source = {
                                        'table': 'kela_meta',
                                        'fields_value': "update_dt = to_date('{file_date}', 'YYYY-MM-DD')".format(file_date=file_date),
                                        'where': "schema_name = 'DE3AT' AND table_name = 'KELA_DWH_DIM_PSSPRT_BLCKLST'"
                                    }                    
                                sqlUpdate = KelaSQL('update', source)
                                sqlUpdate.execute()
                                del sqlUpdate
                                
                                # Бэкапим файл
                                os.replace(path_data + file, path_arch + '{0}.backup'.format(file))
                            except Exception as e:
                                raise

                        # Терминалы
                        if file.find('terminals') >= 0:
                            df = getDFByFileExtension(file)
                            log_f.info("   Обрабатываем файл {0} ...".format(file))
                            try:
                                df = getDFByFileExtension(file)
                                # Основной стейдж
                                values_list = df.fillna('').values.tolist()
                                values_list[:] = [i[:] + [str(file_date)] for i in values_list]
                                source = {
                                    'table': 'kela_stg_terminals',
                                    'fields': 'terminal_id, terminal_type, terminal_city, terminal_address, create_dt', 
                                    'values_list': values_list,
                                    'conditions': 'terminal_type, terminal_city, terminal_address',
                                } 
                                sqlInsertAll = KelaSQL('insert_all', source)
                                sqlInsertAll.execute()
                                del sqlInsertAll                                
                                
                                # Стейдж контроля удалений
                                # Оставляем столбец с terminal_id
                                values_list[:] = [i[:1] for i in values_list]
                                source = {
                                    'table': 'kela_stg_terminals_del',
                                    'fields': 'terminal_id', 
                                    'values_list': values_list,
                                    'conditions': '',
                                } 
                                sqlInsertAll = KelaSQL('insert_all', source)
                                sqlInsertAll.execute()
                                del sqlInsertAll   
                        
                                # Загрузка в DWH                   
                                # Закрываем изменённые и добавляем новые записи
                                source = {
                                    'table': 'kela_dwh_dim_terminals_hist',
                                    'using': 'kela_stg_terminals',
                                    'conditions': """src.terminal_id = tgt.terminal_id""",
                                    'matched_update': {
                                        'set_fields': "effective_to = to_date(src.create_dt, 'YYYY-MM-DD') - INTERVAL '1' DAY",         
                                        'set_conditions': """current_date BETWEEN tgt.effective_from AND tgt.effective_to
                                            AND src.create_dt in (select max(create_dt) from kela_stg_terminals)
                                            AND (1=0  
                                            OR (src.terminal_type <> tgt.terminal_type) 
                                            OR (src.terminal_type IS NULL AND tgt.terminal_type IS NOT NULL) 
                                            OR (src.terminal_type IS NOT NULL and tgt.terminal_type IS NULL)
                                            OR (src.terminal_city <> tgt.terminal_city) 
                                            OR (src.terminal_city IS NULL AND tgt.terminal_city IS NOT NULL) 
                                            OR (src.terminal_city IS NOT NULL and tgt.terminal_city IS NULL)
                                            OR (src.terminal_address <> tgt.terminal_address) 
                                            OR (src.terminal_address IS NULL AND tgt.terminal_address IS NOT NULL) 
                                            OR (src.terminal_address IS NOT NULL and tgt.terminal_address IS NULL))
                                        """
                                    },
                                    'not_matched_insert': {
                                        'fields': 'terminal_id, terminal_type, terminal_city, terminal_address, create_dt, effective_from, effective_to',
                                        'values': """src.terminal_id, src.terminal_type, src.terminal_city, src.terminal_address, 
                                                        to_date(src.create_dt, 'YYYY-MM-DD'), to_date(src.create_dt, 'YYYY-MM-DD'), 
                                                        to_date('2999-12-31','YYYY-MM-DD')""",
                                        'insert_conditions': '1=1'
                                    }
                                }
                                sqlMergeTable = KelaSQL('merge_table', source)
                                sqlMergeTable.execute()
                                del sqlMergeTable
                                # Вставляем новые версии изменённых записей
                                source = {
                                    'target_table': 'kela_dwh_dim_terminals_hist',
                                    'fields': 'terminal_id, terminal_type, terminal_city, terminal_address, create_dt, effective_from, effective_to',
                                    'values': '',
                                    'select': {
                                        'distinct': 'distinct',
                                        'fields': """rght.terminal_id, rght.terminal_type, rght.terminal_city, rght.terminal_address, to_date(rght.create_dt,'YYYY-MM-DD'), to_date(rght.create_dt,'YYYY-MM-DD'), to_date('2999-12-31','YYYY-MM-DD')""", 
                                        'left_table': 'kela_dwh_dim_terminals_hist lft',
                                        'join': {
                                            'type': 'inner join',
                                            'right_table': 'kela_stg_terminals rght',
                                            'on': 'lft.terminal_id = rght.terminal_id'
                                        },
                                        'where':  """rght.create_dt in (select max(create_dt) from kela_stg_terminals where terminal_id = rght.terminal_id) and current_date > (select max(effective_to) from kela_dwh_dim_terminals_hist where terminal_id = rght.terminal_id)"""
                                    },
                                }
                                sqlInsertInto = KelaSQL('insert_into', source)
                                sqlInsertInto.execute()
                                del sqlInsertInto

                                # Добавляем версию удалённых записей в целевую таблицу
                                source = {
                                    'target_table': 'kela_dwh_dim_terminals_hist',
                                    'fields': 'terminal_id, terminal_type, terminal_city, terminal_address, create_dt, effective_from, effective_to, deleted_flg',
                                    'values': '',
                                    'select': {
                                        'distinct': 'distinct',
                                        'fields': """terminal_id, terminal_type, terminal_city, terminal_address, to_date('{file_date}', 'YYYY-MM-DD'), to_date('{file_date}', 'YYYY-MM-DD'), to_date('2999-12-31','YYYY-MM-DD'), 1""".format(file_date=file_date),
                                        'left_table': 'kela_dwh_dim_terminals_hist',
                                        'join': {},
                                        'where':  """current_date BETWEEN effective_from AND effective_to
                                                        AND deleted_flg = '0'
                                                        AND terminal_id IN (
                                                            SELECT 
                                                                tgt.terminal_id
                                                            FROM 
                                                                kela_dwh_dim_terminals_hist tgt 
                                                            LEFT JOIN 
                                                                kela_stg_terminals_del stg
                                                                ON tgt.terminal_id = stg.terminal_id
                                                            WHERE
                                                                stg.terminal_id IS NULL )"""
                                    },
                                }
                                sqlInsertInto = KelaSQL('insert_into', source)
                                sqlInsertInto.execute()
                                del sqlInsertInto

                                # Обновляем конечную дату валидности у удалённых записей
                                source = {
                                        'table': 'kela_dwh_dim_terminals_hist',
                                        'fields_value': "effective_to = to_date('{file_date}', 'YYYY-MM-DD') - INTERVAL '1' DAY".format(file_date=file_date),
                                        'where': """current_date BETWEEN effective_from AND effective_to
                                                    AND deleted_flg = '0'
                                                    AND terminal_id IN (
                                                        SELECT 
                                                            dwh.terminal_id
                                                        FROM 
                                                            kela_dwh_dim_terminals_hist dwh 
                                                        LEFT JOIN
                                                            kela_stg_terminals_del stg
                                                            ON dwh.terminal_id = stg.terminal_id
                                                        WHERE
                                                            stg.terminal_id IS NULL )"""
                                    }                    
                                sqlUpdate = KelaSQL('update', source)
                                sqlUpdate.execute()
                                del sqlUpdate

                                # Метаданные
                                source = {
                                        'table': 'kela_meta',
                                        'fields_value': "update_dt = to_date('{file_date}', 'YYYY-MM-DD')".format(file_date=file_date),
                                        'where': "schema_name = 'DE3AT' AND table_name = 'KELA_DWH_DIM_TERMINALS_HIST'"
                                    }                    
                                sqlUpdate = KelaSQL('update', source)
                                sqlUpdate.execute()
                                del sqlUpdate

                                # Бэкапим файл
                                os.replace(path_data + file, path_arch + '{0}.backup'.format(file))
                            except Exception as e:
                                raise


                        # Транзакции
                        if file.find('transactions') >= 0:
                            pass
                            log_f.info("   Загружаем стейджи из {0} ...".format(file))
                            try:
                                df = getDFByFileExtension(file, '\n')
                                values_list = df.fillna('').values.tolist()
                                # Разбиваем на поля
                                for i in values_list:
                                    string = i[0]
                                    i.clear()
                                    for v in string.split(';'):
                                        i.append(v)
                                source = {
                                    'table': 'kela_stg_transactions',
                                    'fields': 'trans_id, trans_date, amt, card_num, oper_type, oper_result, terminal', 
                                    'values_list': values_list,
                                    'values_template': ':0, :1, :2, :3, :4, :5, :6',
                                } 
                                sqlInsertList = KelaSQL('insert_list', source)
                                sqlInsertList.execute()
                                del sqlInsertList     

                                # Загрузка в DWH
                                source = {
                                    'table': 'kela_dwh_fact_transactions',
                                    'using': 'kela_stg_transactions',
                                    'conditions': 'src.trans_id = tgt.trans_id',
                                    'matched_update': {},
                                    'not_matched_insert': {
                                        'fields': 'trans_id, trans_date, amt, card_num, oper_type, oper_result, terminal',
                                        'values': "src.trans_id, to_date(src.trans_date, 'YYYY-MM-DD HH24:Mi:SS'), src.amt, src.card_num, src.oper_type, src.oper_result, src.terminal",   
                                        'insert_conditions': '1=1' 
                                    } 
                                }
                                sqlMergeTable = KelaSQL('merge_table', source)
                                sqlMergeTable.execute()
                                del sqlMergeTable

                                # Метаданные
                                source = {
                                        'table': 'kela_meta',
                                        'fields_value': "update_dt = to_date('{file_date}', 'YYYY-MM-DD')".format(file_date=file_date),
                                        'where': "schema_name = 'DE3AT' AND table_name = 'KELA_DWH_FACT_TRANSACTIONS'"
                                    }                    
                                sqlUpdate = KelaSQL('update', source)
                                sqlUpdate.execute()
                                del sqlUpdate

                                os.replace(path_data + file, path_arch + '{0}.backup'.format(file))
                            except Exception as e:
                                raise 

                    if not load_date:
                        # Повторяем операцию до окончания файлов
                        processFiles(load_date)
                
                else:
                    pass
                
            processFiles(load_date)

            conn.commit()
        except OSError as oe:
            err = str(oe)[:3]
            if err in ['001', '002', '003']:
                log.info('   Файлы данных не найдены. Err {0}'.format(str(oe)))
                conn.commit()
        except Exception as e:
            conn.rollback()
            conn.close()
            log.warning('   Критическая ошибка!!! DML запросы отменены. Лог записан в kela.log.\nОшибка: {0}'.format(str(e)))

        # Собираем данные для отчётности
        report.start()

    try:
        msql.conn.close()
    except Exception:
        pass
        
        
        
    log.info('   Процесс завершён успешно. Лог записан в kela.log')

if __name__ == '__main__':
    if os.environ["INIT_COMPLETED"] == '1':
        start()                            
    else:
        log_f.info("   Ошибка! Инициализация хранилища не завершена.\n   Для запуска процесса инициализации воспользуйтесь файлом main.py.")
        
    