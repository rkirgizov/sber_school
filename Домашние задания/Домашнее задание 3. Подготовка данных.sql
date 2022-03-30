-- На основании таблиц DE_COMMON.LOG и DE_COMMON.IP постройте структурированную таблицу посещений:
-- DE3AT.XXXX_LOG (
--   DT DATE,
--   LINK VARCHAR2( 50 ),
--   USER_AGENT VARCHAR2( 200 ),
--   REGION VARCHAR2(
-- 30 )
-- )
-- Также постройте отчет:
-- DE3AT.XXXX_LOG_REPORT (
--   REGION VARCHAR2( 20 ),
--   BROWSER
-- VARCHAR2( 10 )
-- )
-- в каких областях какой браузер является наиболее используемым.
-- Просьба быть внимательным к названиям таблиц и полей – проверка полуавтоматическая.
-- Под
-- USER_AGENT подразумевается вся строка описания клиента, под BROWSER – только название
-- браузера (Opera, Safari…).
-- XXXX означает ваши 4 уникальные буквы.
-- На сервере должны быть созданы и наполнены данными таблицы, в classroom надо прислать файл с
-- SQL кодом их создания.
-- Важные замечания (вплоть до причины незачета задания):
--  • Не используйте регулярные выражения там, где можно обойтись без них.
--  • То, что вы видите в выводе клиента – это не всегда именно то, что содержится в базе данных. 
-- Для информации: в реальной жизни задача будет звучать так – «У нас есть две таблицы, LOG и IP, нужно
-- структурировать данные и построить отчет в каких регионах какой браузер самый популярный».

-- KELA_LOG
create table DE3AT.KELA_LOG (
    DT DATE,
    LINK VARCHAR2 (50),
    USER_AGENT VARCHAR2 (200),
    REGION VARCHAR2 (30)
)

insert into DE3AT.KELA_LOG (DT, LINK, USER_AGENT, REGION)
select tlog.dt_, tlog.link_, tlog.user_agent_, tip.region_
from
    (select 
        trim (chr(09) from substr (val, 1, instr (val, chr(09), 1, 1))) as ip_,
        to_date (substr (val, instr (val, chr(09), 1, 1), 16),'YYYYMMDDHH24MISS') as dt_,
        substr (val, instr (val, 'http', 1),
            instr (val, chr(09), instr (val, 'http', 1), 1) - instr (val, 'http', 1, 1)) as link_,
        ltrim (rtrim (substr (val, instr (val, chr(09), instr (val, 'http', 1), 3)), 'n ' || chr(09)), chr(09) || ' ') as user_agent_
    from 
        de_common.log) tlog
inner join
    (select 
        trim (substr (val, 1, instr (val, ' ', 1, 1))) as ip_,
        trim (substr (val, instr (val, ' ', 1, 1))) as region_
    from 
        de_common.ip) tip
    on tlog.ip_ = tip.ip_

-- KELA_LOG_REPORT
create table DE3AT.KELA_LOG_REPORT (
    REGION VARCHAR2(30),
    BROWSER VARCHAR2(10)
)

insert into DE3AT.KELA_LOG_REPORT (REGION, BROWSER) 
-- временная таблица со счётчиками использования
with log_report_tempo as (
    select 
        region as region_, 
        substr (user_agent, 1, instr (user_agent, '/') - 1) as browser_,
        count (substr (user_agent, 1, instr (user_agent, '/') - 1)) as count_
    from
        DE3AT.KELA_LOG
    group by region, substr (user_agent, 1, instr (user_agent, '/') - 1)
)
-- вытаскиваем наиболее часто используемые
select 
        region_, 
        browser_
    from 
        log_report_tempo lrt
    where 
        count_ = (select max(count_) from log_report_tempo where lrt.region_ = region_)
    order by 
        region_
