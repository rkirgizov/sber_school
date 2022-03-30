#!/usr/bin/env python3
import os
import sys
sys.path.insert(1, '{0}py_scripts'.format(os.environ["ROOT_PATH"]))
import configparser 
config = configparser.ConfigParser()
config_file = '/home/de3at/kela/config.ini'
config.read(config_file)

# kela modules
from logger import KelaLogger
log = KelaLogger('kela.log').get_logger(__name__)
import msql
from msql import KelaSQL

def start():
    msql.conn = msql.get_conn()   
    with msql.conn as conn:
        conn.jconn.setAutoCommit(False)
        try:
            log.info("   ИНИЦИАЛИЗАЦИЯ ХРАНИЛИЩА...")
            log.info("   1. Удаляем существующие таблицы...")
            source = {
                'kela_meta',
                'kela_stg_transactions',
                'kela_stg_pssprt_blcklst',
                'kela_stg_terminals',
                'kela_stg_cards',
                'kela_stg_accounts',
                'kela_stg_clients',
                'kela_stg_terminals_del',
                'kela_stg_cards_del',
                'kela_stg_accounts_del',
                'kela_stg_clients_del',
                'kela_dwh_fact_transactions',
                'kela_dwh_dim_terminals_hist',
                'kela_dwh_dim_pssprt_blcklst',
                'kela_dwh_dim_cards_hist',
                'kela_dwh_dim_accounts_hist',
                'kela_dwh_dim_clients_hist',
                'kela_rep_fraud',
            }
            ignored_errs = ['ORA-00942']
            sqlDrop = KelaSQL('drop', source, ignored_errs)
            sqlDrop.execute()
            del sqlDrop
            
            log.info("   2. Создаём таблицы...")
            source = [
                # Таблица с метаданными
                ('kela_meta', 'schema_name VARCHAR(30), table_name VARCHAR(30), update_dt DATE'),
                # Временные хранилища
                ('kela_stg_cards', 'SELECT * FROM bank.cards WHERE 1=0'),
                ('kela_stg_accounts', 'SELECT * FROM bank.accounts WHERE 1=0'),
                ('kela_stg_clients', 'SELECT * FROM bank.clients WHERE 1=0'),
                ('kela_stg_terminals', 'terminal_id VARCHAR2(7), terminal_type CHAR(3), terminal_city VARCHAR2(50), terminal_address VARCHAR2(100), create_dt VARCHAR2(10)'),
                ('kela_stg_transactions', 'trans_id VARCHAR2(20), trans_date VARCHAR2(20), amt VARCHAR2(20), card_num VARCHAR2(19), oper_type VARCHAR2(15), oper_result VARCHAR2(10), terminal VARCHAR2(7)'),
                ('kela_stg_pssprt_blcklst', 'passport_num VARCHAR2(11), entry_dt VARCHAR2(20)'),
                ('kela_stg_terminals_del', 'SELECT terminal_id FROM kela_stg_terminals WHERE 1=0'),
                ('kela_stg_cards_del', 'SELECT card_num FROM kela_stg_cards WHERE 1=0'),
                ('kela_stg_accounts_del', 'SELECT account FROM kela_stg_accounts WHERE 1=0'),
                ('kela_stg_clients_del', 'SELECT client_id FROM kela_stg_clients WHERE 1=0'),
                # Целевые таблицы
                ('kela_dwh_fact_transactions', 'SELECT * FROM kela_stg_transactions WHERE 1=0'),
                ('kela_dwh_dim_terminals_hist', 'SELECT * FROM kela_stg_terminals WHERE 1=0'),
                ('kela_dwh_dim_pssprt_blcklst', 'SELECT * FROM kela_stg_pssprt_blcklst WHERE 1=0'),
                ('kela_dwh_dim_cards_hist', 'SELECT * FROM kela_stg_cards WHERE 1=0'),
                ('kela_dwh_dim_accounts_hist', 'SELECT * FROM kela_stg_accounts WHERE 1=0'),
                ('kela_dwh_dim_clients_hist', 'SELECT * FROM kela_stg_clients WHERE 1=0'),   
                # Витрина
                ('kela_rep_fraud', 'event_dt DATE, trans_id VARCHAR2(20), passport VARCHAR2(11), fio VARCHAR2(100), phone VARCHAR2(20), event_type VARCHAR2(50), report_dt DATE'),                             
            ]
            sqlCreate = KelaSQL('create', source)
            sqlCreate.execute()
            del sqlCreate   

            log.info("   3. Редактируем таблицы...")
            source = {
                    'add': [
                        ('kela_dwh_dim_terminals_hist', "effective_from DATE, effective_to DATE, deleted_flg CHAR(1) DEFAULT '0'"),
                        ('kela_dwh_dim_cards_hist', "effective_from DATE, effective_to DATE, deleted_flg CHAR(1) DEFAULT '0'"),
                        ('kela_dwh_dim_accounts_hist', "effective_from DATE, effective_to DATE, deleted_flg CHAR(1) DEFAULT '0'"),
                        ('kela_dwh_dim_clients_hist', "effective_from DATE, effective_to DATE, deleted_flg CHAR(1) DEFAULT '0'"),
                        ('kela_stg_transactions', 'load_dt DATE DEFAULT current_date'),
                        ('kela_stg_pssprt_blcklst', 'load_dt DATE DEFAULT current_date'),
                        ('kela_stg_terminals', 'load_dt DATE DEFAULT current_date'),
                        ('kela_stg_cards', 'load_dt DATE DEFAULT current_date'),
                        ('kela_stg_accounts', 'load_dt DATE DEFAULT current_date'),
                        ('kela_stg_clients', 'load_dt DATE DEFAULT current_date'),
                    ],
                    'modify': [
                        ('kela_dwh_fact_transactions', 'trans_date DATE'),
                        ('kela_dwh_dim_pssprt_blcklst', 'entry_dt DATE'),
                        ('kela_dwh_dim_terminals_hist', 'create_dt DATE'),
                    ],
                    'drop column': [
                        ('kela_dwh_dim_cards_hist', 'update_dt'),
                        ('kela_dwh_dim_accounts_hist', 'update_dt'),
                        ('kela_dwh_dim_clients_hist', 'update_dt'),
                    ],
                }
            sqlAlter = KelaSQL('alter', source)
            sqlAlter.execute()
            del sqlAlter   

            log.info("   4. Заполняем метаданные...")
            values_list = [
                ['DE3AT', 'KELA_DWH_FACT_TRANSACTIONS'],
                ['DE3AT', 'KELA_DWH_DIM_TERMINALS_HIST'],
                ['DE3AT', 'KELA_DWH_DIM_PSSPRT_BLCKLST'],
                ['DE3AT', 'KELA_DWH_DIM_CARDS_HIST'],
                ['DE3AT', 'KELA_DWH_DIM_ACCOUNTS_HIST'],
                ['DE3AT', 'KELA_DWH_DIM_CLIENTS_HIST'],
            ]
            source = {
                'table': 'kela_meta',
                'fields': 'schema_name, table_name', 
                'values_list': values_list,
                'conditions': '',
            } 
            sqlInsertAll = KelaSQL('insert_all', source)
            sqlInsertAll.execute()
            del sqlInsertAll              
            
            # Сохраняем окончание инициализации в конфиг
            config['KELA']['init_completed'] = '1'                
            with open(config_file, 'w') as configfile:
                config.write(configfile)   
                         
            log.info('   Процесс завершён успешно. Лог записан в kela.log')

            # Подтверждаем DML запросы
            conn.commit()
            
        except Exception as e:
            conn.rollback()
            conn.close()
            log.warning('   Критическая ошибка!!! DML запросы отменены. Лог записан в kela.log.\nОшибка: {0}'.format(str(e)))
            raise
