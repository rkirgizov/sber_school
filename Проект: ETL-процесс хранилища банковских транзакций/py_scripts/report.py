#!/usr/bin/env python3
# kela modules
import msql
from msql import KelaSQL
from logger import KelaLogger

log = KelaLogger('kela.log').get_logger(__name__)
log_f = KelaLogger('kela.log').get_logger(__name__ + '_f', True)

def start():
    msql.conn = msql.get_conn()   
    with msql.conn as conn:
        conn.jconn.setAutoCommit(False)
        try:
            log.info("   ФОРМИРОВАНИЕ ОТЧЁТОВ...")
            for _, templates in msql.report_templates.items():
                log.info(templates['name'])
                using_query = '({0} {1})'.format(msql.report_main_template, templates['template'])
                source = {
                        'table': 'kela_rep_fraud',
                        'using': using_query,
                        'conditions': 'src.trans_id = tgt.trans_id',
                        'matched_update': {},
                        'not_matched_insert': {
                            'fields': 'event_dt, trans_id, passport, fio, phone, event_type, report_dt',
                            'values': 'src.event_dt, src.trans_id, src.passport, src.fio, src.phone, src.event_type, src.report_dt',   
                            'insert_conditions': '1=1' 
                        }
                    }
                sqlMergeTable = KelaSQL('merge_table', source)
                sqlMergeTable.execute()
                del sqlMergeTable
            conn.commit()
        except Exception as e:
            conn.rollback()
            conn.close()
            log.warning('   Критическая ошибка!!! DML запросы отменены. Лог записан в kela.log.\nОшибка: {0}'.format(str(e)))
    try:
        msql.conn.close()
    except Exception:
        pass
        
    # log.info('   Процесс завершён успешно. Лог записан в kela.log')

