#!/usr/bin/env python3
import jaydebeapi
import re
import os
import sys
sys.path.insert(1, '{0}/py_scripts'.format(os.environ["ROOT_PATH"]))

# kela modules
from logger import KelaLogger

# функция получения коннекта к базе
def get_conn():
    conn = jaydebeapi.connect(
        'oracle.jdbc.driver.OracleDriver',
        'jdbc:oracle:thin:de3at/bardthebowman@de-oracle.chronosavant.ru:1521/deoracle',
        ['de3at','bardthebowman'],
        '/home/de3at/ojdbc8.jar'
    )
    return conn    

# Хранение коннекта для реализации транзакций
conn = ''

class KelaSQL:
    """Не успел"""
 
    def __init__(self, template='', source={}, ignored_errs=[]):
        # """Constructor"""
        self.template = template
        self.source = source
        self.log = KelaLogger('kela.log').get_logger(__name__)
        self.log_f = KelaLogger('kela.log').get_logger(__name__ + '_f', True)
        # sql шаблоны
        self.templates = {
            'script': '',
            # DDL секция
            'create': 'create table {table} {str_as} ({source})',
            'alter': 'alter table {table} {alter_type} ',
            'truncate': 'truncate table {0} drop storage',
            'drop': 'drop table {0}',
            # DML секция
            'insert_into': 'insert into {target_table} ({fields})',
            # insert all
            'insert_all': 'insert all into {table} ({fields}) values ({fields}) with source_data as ({values_dual}) select {fields} from source_data src',
            'where_not_exists': ' where not exists (select 1 from {table} tgt where {conditions})',
            # ---
            'insert_list': 'insert into {table} ({fields}) values ({values})',
            'merge_list': '''merge into {table} tgt
                                        using (select {values} from dual) src
                                        on ({conditions})
                                        when not matched then insert ({fields})
                                        values ({values})''',
            'merge_table': 'merge into {table} tgt using {using} src on ({conditions})',
            'update': 'update {table} set {fields_value}',
            'delete': 'delete from {table} {condition}',
        }
        # ignored sql errors
        self.ignored_errs = ignored_errs 

    # Выполнение множественного запроса
    def execute_many(self, curs, query, values_list, rowcount=False):
        count = 0
        try:
            curs.executemany(query, values_list)
            count = curs.rowcount if rowcount else count + 1
        except jaydebeapi.DatabaseError as jde:
            msg = str(jde)[str(jde).find('ORA-'):len(str(jde))-1]
            err = msg[:9]
            if err in self.ignored_errs:
                pass
            else:
                jde = re.sub('\s+', ' ', str(jde))
                query = re.sub('\s+', ' ', query)
                self.log.warning('!!! Критическая ошибка: шаблон ({0}).\nЗапрос ({1}),\nОшибка ({2})'.format(self.template, query, jde))
                conn.rollback()
                raise
        except Exception:
            conn.rollback()
            raise
        return count
    
    # Выполнение запроса
    def execute_query(self, curs, query, rowcount=False):
        count = 0
        try:
            curs.execute(query)
            count = curs.rowcount if rowcount else count + 1
            # count = curs.rowcount if curs.rowcount >= 0 else count + 1
            # print (self.template + ", count = " + str(count) + ", rowcount = " + str(curs.rowcount))
        except jaydebeapi.DatabaseError as jde:
            msg = str(jde)[str(jde).find('ORA-'):len(str(jde))-1]
            err = msg[:9]
            if err in self.ignored_errs:
                pass
            else:
                jde = re.sub('\s+', ' ', str(jde))
                query = re.sub('\s+', ' ', query)
                self.log.warning('!!! Критическая ошибка: шаблон ({0}).\nЗапрос ({1}),\nОшибка ({2})'.format(self.template, query, jde))
                conn.rollback()
                raise
        except Exception:
            conn.rollback()
            raise
        return count
    
    # Подготовка запроса
    def execute(self):
        with conn.cursor() as curs:
            count = 0
            if self.template == 'script':
                # 'script': ''
                countQueries = 0
            
                for query in self.source:
                    # if len(query) > 5:
                    countQueries += 1
                    count += self.execute_query(curs, query)
            
                self.log_f.info('Выполнено запросов: {0} из {1}'.format(str(count), str(countQueries)))
            # ---------------------------------------------------------------------------------------------      
            # START DDL секция          
            # ---------------------------------------------------------------------------------------------      
            if self.template == 'drop':
                # 'drop': 'drop table {0}'
                for table in self.source:
                    query = self.templates[self.template].format(table)
                    count += self.execute_query(curs, query)
            
                self.log_f.info('Удалено таблиц: {0}'.format(str(count)))
           
            elif self.template == 'truncate':
                # 'truncate': 'truncate table {0} drop storage'
                for table in self.source:
                    query = self.templates[self.template].format(table)
                    count += self.execute_query(curs, query)
            
                self.log_f.info('Очищено таблиц: {0}'.format(str(count)))
            
            elif self.template == 'create':
                # 'create': 'create table {table} {str_as} ({source})'
                for i in self.source:
                    if re.match('select', i[1], flags=re.IGNORECASE):
                        query = self.templates[self.template].format(table=i[0], str_as='as', source=i[1])
                    else:
                        query = self.templates[self.template].format(table=i[0], str_as='', source=i[1])
                    count += self.execute_query(curs, query)
            
                self.log_f.info('Создано таблиц: {0}'.format(str(count)))
            
            elif self.template == 'alter':
                # 'alter': 'alter table {table} {alter_type} ',
                for type in sorted(self.source):
                    for i in self.source[type]:
                        query = self.templates[self.template] + ('{fields}' if type == 'drop column' else '({fields})')
                        query = query.format(table=i[0], alter_type=type, fields=i[1])
                        count += self.execute_query(curs, query)
            
                self.log_f.info('Изменено таблиц: {0}'.format(str(count)))  
            
            # # --------------------------------------------------------------------------------------------      
            # # START DML секция          
            # # --------------------------------------------------------------------------------------------                                         
            elif self.template == 'delete':
                # 'delete': 'delete from {table} {condition}'
                for table, condition in self.source.items():
                    query = self.templates[self.template].format(table=table, condition=condition)
                    count += self.execute_query(curs, query)
                
                self.log_f.info('Очищено таблиц: {0}'.format(str(count)))
            
            elif self.template == 'insert_list':
                # 'insert_list': 'insert into {table} ({fields}) values ({values})'
                query = self.templates[self.template].format(table=self.source['table'], fields=self.source['fields'], values=self.source['values_template'])
                count = self.execute_many(curs, query, self.source['values_list'], True)  
                self.log_f.info('Добавлено строк: {0} в ({1})'.format(str(count), self.source['table']))      
            
            elif self.template == 'merge_list':
                # 'merge_list': '''merge into {table} tgt
                #                         using (select {values} from dual) src
                #                         on ({conditions})
                #                         when not matched then insert ({fields})
                #                         values ({values})'''
                table = ''
                fields = ''
                values_list = ''
            
                for k, v in self.source.items():
                    table = k
                    fields = v[0]
                    values_list = v[1]
                    key_fields = v[2]
            
                for v in values_list:
                    conditions = ''
                    for field, key in key_fields.items():
                        conditions += field + "='" + v[key] + "' and "
                    values = ''
                    for vv in v:
                        values += "'" + vv + "'" + ", "
                    query = self.templates[self.template].format(table = table, fields = fields, values = values[:-2], conditions=conditions[:-5])

                count += self.execute_query(curs, query, True)
                self.log_f.info('Добавлено строк: {0} в ({1})'.format(str(count), table))           
            
            elif self.template == 'merge_table':
                # 'merge_table': 'merge into {table} tgt using {using} src on ({conditions})'
                source = self.source
                need_not_matched = True if len(source['not_matched_insert']) >= 1 else False
                need_matched = True if len(source['matched_update']) >= 1 else False
                query = self.templates[self.template]
                query = query.format(table=source['table'], using=source['using'], conditions=source['conditions'])
            
                if need_matched:
                    query = query + ' when matched then update set {set_fields} where {set_conditions}'
                    query = (query.format(set_fields=source['matched_update']['set_fields'], 
                                                            set_conditions=source['matched_update']['set_conditions']))
                if need_not_matched:
                    query = query + ' when not matched then insert ({fields}) values ({values}) where ({insert_conditions})'
                    query = (query.format(fields=source['not_matched_insert']['fields'], 
                                                            values=source['not_matched_insert']['values'], 
                                                            insert_conditions=source['not_matched_insert']['insert_conditions']))

                count += self.execute_query(curs, query, True)
                self.log_f.info('Объединено строк: {0} в ({1})'.format(str(count), source['table']))
            
            elif self.template == 'insert_into':
                # 'insert_into': 'insert into {target_table} ({fields})'
                source = self.source
                need_values = True if len(source['values']) >= 1 else False
                need_select = True if len(source['select']) >= 1 else False
                query = self.templates[self.template].format(target_table=source['target_table'], fields=source['fields'])
                
                if need_values:
                    query = query + ' values ({values})'
                    query = query.format(values=source['values'])
                
                if need_select:
                    need_distinct = True if len(source['select']['distinct']) >= 1 else False
                    need_join = True if len(source['select']['join']) >= 1 else False
                    need_where = True if len(source['select']['where']) >= 1 else False
                    query = query + (' select distinct {fields} from {left_table}' if need_distinct else ' select {fields} from {left_table}')
                    query = query.format(fields=source['select']['fields'], left_table=source['select']['left_table'])
                    if need_join:
                        query = query + ' {join_type} {right_table} on {on}'
                        query = query.format(join_type=source['select']['join']['type'], right_table=source['select']['join']['right_table'], on=source['select']['join']['on'])
                    if need_where:
                        query = query + ' where {where}'
                        query = query.format(where=source['select']['where'])

                count += self.execute_query(curs, query, True)
                self.log_f.info('Добавлено строк: {0} в ({1})'.format(str(count), source['target_table']))
            
            elif self.template == 'insert_all':
                # 'insert_all': 'insert all into {table} ({fields}) values ({fields}) with source_data as ({values_dual}) select {fields} from source_data src'
                # 'where_not_exists': ' where not exists (select 1 from {table} tgt where {conditions})'
                query = self.templates[self.template]
                source = self.source
                values_list = source['values_list']
                values_dual = ''
                conditions = ''
                fields = source['fields'].split(sep=', ')
                
                for values in values_list:
                    for i in range(0, len(fields)):
                        values_dual += ('select ' if i == 0 else '') + "'" + values[i] + "'" + " as " + fields[i] + (' from dual ' if i == len(fields) - 1 else ', ')
                    if values != values_list[-1]:
                        values_dual += "union all "
                
                if len(source['conditions']) > 1:
                    query += self.templates['where_not_exists']                    
                    condition_fields = source['conditions'].split(sep=', ')
                    for i in range(0, len(condition_fields)):
                        conditions += 'src.{0} = tgt.{0}{1}'.format(condition_fields[i], ' and ' if i < len(condition_fields) - 1 else '')
                
                query = query.format(table= source['table'], 
                                                        fields= source['fields'], 
                                                        values_dual= values_dual, 
                                                        conditions= conditions)
                
                count += self.execute_query(curs, query, rowcount=True)
                self.log_f.info('Добавлено строк: {0} в ({1})'.format(str(count), source['table']))
                
            elif self.template == 'update':
                # 'update': 'update {table} set {fields_value}'
                source = self.source
                need_where = True if len(source['where']) >= 1 else False
                query = self.templates[self.template].format(table=source['table'], fields_value=source['fields_value'])                    
                
                if need_where:
                    query = query + '  where {condition}'
                    query = query.format(condition=source['where'])
                count += self.execute_query(curs, query)
                
                self.log_f.info('Обновлено {0} строк(а) в ({1})'.format(str(count), source['table']))

#---------------------------------------------------
# Запросы по отчётам
#---------------------------------------------------
# Основная заджойненная выборка
report_main_template = """with report_template as (
    select distinct
        transactions.trans_id as trans_id,
        transactions.trans_date as event_dt,
        transactions.card_num as card_num,
        terminals.terminal_city as city,
        accounts.account as account,
        accounts.valid_to as account_valid_to,
        clients.last_name || ' ' || clients.first_name || ' ' || clients.patronymic as fio,
        clients.passport_num as passport,
        clients.passport_valid_to as passport_valid_to,
        clients.phone as phone
    from
        kela_dwh_fact_transactions transactions
    inner join
        kela_dwh_dim_terminals_hist terminals
        on (trim(transactions.terminal) = trim(terminals.terminal_id))
    inner join
        kela_dwh_dim_cards_hist cards
        on (trim(transactions.card_num) = trim(cards.card_num))
    inner join
        kela_dwh_dim_accounts_hist accounts
        on (trim(cards.account) = trim(accounts.account))
    inner join
        kela_dwh_dim_clients_hist clients
        on (trim(accounts.client) = trim(clients.client_id))
    )"""
# Словарь с подвыборками
report_templates = {
    'report_invalid_passport_template': {
        'name': '   Операции при просроченном или заблокированном паспорте...',
        'template': """select distinct
                            template.event_dt as event_dt,
                            template.trans_id as trans_id,
                            template.passport as passport,
                            template.fio as fio,
                            template.phone as phone,
                            'Invalid Passport' as event_type,
                            current_date as report_dt
                        from
                            kela_dwh_fact_transactions transactions
                        inner join
                            report_template template
                            on (trim(transactions.trans_id) = trim(template.trans_id))
                        where 1=1
                                and (passport_valid_to < event_dt
                                        or passport in (select passport_num from kela_dwh_dim_pssprt_blcklst where entry_dt <= event_dt))"""
        },
    'report_expired_account_template': {
        'name': '   Операции при недействующем договоре...',
        'template': """select distinct
                            template.event_dt as event_dt,
                            template.trans_id as trans_id,
                            template.passport as passport,
                            template.fio as fio,
                            template.phone as phone,
                            'Expired Account' as event_type,
                            current_date as report_dt
                        from
                            kela_dwh_fact_transactions transactions
                        inner join
                            report_template template
                            on (trim(transactions.trans_id) = trim(template.trans_id))
                        where 1=1
                                and account_valid_to < event_dt"""
        },
    'report_cities_per_hour_template': {
        'name': '   Операции в разных городах в течение одного часа...',
        'template': """select distinct
                        template.event_dt as event_dt,
                        template.trans_id as trans_id,
                        template.passport as passport,
                        template.fio as fio,
                        template.phone as phone,
                        'Cities Per Hour' as event_type,
                        current_date as report_dt
                    from
                        kela_dwh_fact_transactions transactions
                    inner join
                        report_template template
                        on (trim(transactions.trans_id) = trim(template.trans_id))
                    inner join 
                        (select 
                            trans_id as trans_id,
                            event_dt as event_dt,
                            card_num as card_num,
                            city as city
                        from
                            report_template join_table
                        where 1=1
                            and trim(card_num) = trim(join_table.card_num)
                            and trim(city) = trim(join_table.city)) sub_template
                    on (trim(template.card_num)=trim(sub_template.card_num))
                    where 1=1
                        and trim(template.city) <> trim(sub_template.city)
                        and (case 
                                when template.event_dt > sub_template.event_dt
                                    then (template.event_dt - sub_template.event_dt) * 24 * 60
                                when sub_template.event_dt > template.event_dt
                                    then (sub_template.event_dt - template.event_dt) * 24 * 60
                                end) <= 60"""
        },
    'report_amount_selection_template': {
        'name': '   Попытка подбора суммы в течение 20 минут...',
        'template': """select distinct
                            template.event_dt as event_dt,
                            template.trans_id as trans_id,
                            template.passport as passport,
                            template.fio as fio,
                            template.phone as phone,
                            'Amount Selection' as event_type,
                            current_date as report_dt
                        from
                            kela_dwh_fact_transactions transactions
                        inner join
                            report_template template
                            on (trim(transactions.trans_id) = trim(template.trans_id))
                        inner join
                            (select
                                card_num as card_num,
                                trans_date as trans_date_last,
                                trans_id as trans_id_last,
                                lag(trans_id, 1) over (partition by card_num order by trans_date) as trans_id_prev1,
                                lag(trans_id, 2) over (partition by card_num order by trans_date) as trans_id_prev2,
                                lag(trans_id, 3) over (partition by card_num order by trans_date) as trans_id_prev3,
                                to_number(amt, '99999D99', 'NLS_NUMERIC_CHARACTERS='',.''') as amt_last,
                                lag(to_number(amt, '99999D99', 'NLS_NUMERIC_CHARACTERS='',.'''), 1) over (partition by card_num order by trans_date) as amt_prev1,
                                lag(to_number(amt, '99999D99', 'NLS_NUMERIC_CHARACTERS='',.'''), 2) over (partition by card_num order by trans_date) as amt_prev2,
                                lag(to_number(amt, '99999D99', 'NLS_NUMERIC_CHARACTERS='',.'''), 3) over (partition by card_num order by trans_date) as amt_prev3,
                                oper_result
                            from
                                kela_dwh_fact_transactions join_table
                            ) sub_template
                            on (trim(transactions.trans_id) = trim(sub_template.trans_id_last))
                        where 1=1
                            and amt_last > 1
                            and (amt_last < amt_prev1) and (amt_prev1 < amt_prev2) and (amt_prev2 < amt_prev3)
                            and 20 > ((trans_date_last - (select trans_date from kela_dwh_fact_transactions where trans_id = trans_id_prev3)) * 24 * 60)
                            and (trans_id_last in (select trans_id from kela_dwh_fact_transactions where oper_result = 'SUCCESS'))
                            and (trans_id_prev1 in (select trans_id from kela_dwh_fact_transactions where oper_result = 'REJECT'))
                            and (trans_id_prev2 in (select trans_id from kela_dwh_fact_transactions where oper_result = 'REJECT'))
                            and (trans_id_prev3 in (select trans_id from kela_dwh_fact_transactions where oper_result = 'REJECT'))"""
        },
}