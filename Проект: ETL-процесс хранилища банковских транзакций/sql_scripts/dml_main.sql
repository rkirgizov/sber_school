-- kela_dwh_dim_cards_hist - заполняем временные хранилища/
INSERT INTO
    kela_stg_cards (card_num, account, create_dt, update_dt)
WITH last_update_dt as (
    SELECT
        update_dt
    FROM
        kela_meta
    WHERE
        schema_name = 'DE3AT' AND
        table_name = 'KELA_DWH_DIM_CARDS_HIST'
    )
SELECT
    card_num, account, create_dt, update_dt
FROM
    bank.cards
WHERE
    coalesce(update_dt, create_dt) > (select update_dt from last_update_dt)
    OR (select update_dt from last_update_dt) IS NULL;
-- kela_dwh_dim_cards_hist - заполняем таблицу контроля удалений/
INSERT INTO
    kela_stg_cards_del (card_num)
SELECT
    card_num
FROM
    bank.cards;
-- kela_dwh_dim_cards_hist - закрываем изменённые записи и добавляем новые/
MERGE INTO
    kela_dwh_dim_cards_hist dwh
USING
    kela_stg_cards stg
    on (dwh.card_num = stg.card_num)
WHEN matched THEN
    UPDATE
        SET
            effective_to = stg.update_dt - INTERVAL '1' DAY
        WHERE 
            current_date BETWEEN dwh.effective_from AND dwh.effective_to
        AND (1=0  
        OR (stg.account <> dwh.account)
        OR (stg.account IS NULL AND dwh.account IS NOT NULL)
        OR (stg.account IS NOT NULL and dwh.account IS NULL))  
WHEN NOT matched
    THEN 
        INSERT (
            card_num,
            account,
            create_dt,
            effective_from,
            effective_to
            ) 
        VALUES (
            stg.card_num,
            stg.account,
            stg.create_dt,
            coalesce(stg.update_dt, stg.create_dt),
            to_date('2999-12-31 23:59:59','YYYY-MM-DD HH24:MI:SS')
            );
-- kela_dwh_dim_cards_hist - добавляем новую версию изменённых записей/
INSERT INTO 
    kela_dwh_dim_cards_hist (
        card_num,
        account,
        create_dt,
        effective_from,
        effective_to
        ) 
SELECT 
    stg.card_num,
    stg.account,
    stg.create_dt,
    coalesce(stg.update_dt, stg.create_dt),
    to_date('2999-12-31 23:59:59','YYYY-MM-DD HH24:MI:SS')
FROM 
    kela_dwh_dim_cards_hist dwh 
INNER join                                    
    kela_stg_cards stg
    ON dwh.card_num = stg.card_num
WHERE
    current_date > (select max(effective_to) from kela_dwh_dim_cards_hist where account = stg.account);  
-- kela_dwh_dim_cards_hist - добавляем версию удалённых записей в целевую таблицу/
INSERT INTO 
    kela_dwh_dim_cards_hist (
        card_num,
        account,
        create_dt,
        effective_from,
        effective_to,
        deleted_flg
        )
SELECT
    card_num,
    account,
    create_dt,
    current_date,
    to_date('2999-12-31 23:59:59','YYYY-MM-DD HH24:MI:SS'),
    '1'
FROM
    kela_dwh_dim_cards_hist 
WHERE   
    current_date BETWEEN effective_from AND effective_to
    AND deleted_flg = '0'
    AND card_num IN (
        SELECT 
            dwh.card_num
        FROM 
            kela_dwh_dim_cards_hist dwh 
        LEFT JOIN 
            kela_stg_cards_del stg
            ON dwh.card_num = stg.card_num
        WHERE
            stg.card_num IS NULL );
-- kela_dwh_dim_cards_hist - обновляем конечную дату валидности у удалённых записей/
UPDATE 
    kela_dwh_dim_cards_hist
SET 
    effective_to = current_date - INTERVAL '1' DAY
WHERE   
    current_date BETWEEN effective_from AND effective_to
    AND deleted_flg = '0'
    AND card_num IN (
        SELECT 
            dwh.card_num
        FROM 
            kela_dwh_dim_cards_hist dwh 
        LEFT JOIN
            kela_stg_cards_del stg
            ON dwh.card_num = stg.card_num
        WHERE
            stg.card_num IS NULL );
-- kela_dwh_dim_cards_hist - обновляем метаданные/
UPDATE 
    kela_meta
SET 
    update_dt = coalesce ( 
        (SELECT 
            max(coalesce(stg.update_dt, stg.create_dt)) 
        FROM 
            kela_stg_cards stg),
        update_dt)
WHERE
    schema_name = 'DE3AT' 
    AND table_name = 'KELA_DWH_DIM_CARDS_HIST';
-- kela_dwh_dim_accounts_hist - заполняем временные хранилища/
INSERT INTO
    kela_stg_accounts (account, valid_to, client, create_dt, update_dt)
WITH last_update_dt as (
    SELECT
        update_dt
    FROM
        kela_meta
    WHERE
        schema_name = 'DE3AT' AND
        table_name = 'KELA_DWH_DIM_ACCOUNTS_HIST'
    )
SELECT
    account, valid_to, client, create_dt, update_dt
FROM
    bank.accounts
WHERE
    coalesce(update_dt, create_dt) > (select update_dt from last_update_dt)
    OR (select update_dt from last_update_dt) IS NULL;
-- kela_dwh_dim_accounts_hist - заполняем таблицу контроля удалений/
INSERT INTO
    kela_stg_accounts_del (account)
SELECT
    account
FROM
    bank.accounts;
-- kela_dwh_dim_accounts_hist - закрываем изменённые записи и добавляем новые/
MERGE INTO
    kela_dwh_dim_accounts_hist dwh
USING
    kela_stg_accounts stg
    on (dwh.account = stg.account)
WHEN matched THEN
    UPDATE
        SET
            effective_to = stg.update_dt - INTERVAL '1' DAY
        WHERE 
            current_date BETWEEN dwh.effective_from AND dwh.effective_to
        AND (1=0  
            OR (stg.valid_to <> dwh.valid_to)
            OR (stg.valid_to IS NULL AND dwh.valid_to IS NOT NULL)
            OR (stg.valid_to IS NOT NULL and dwh.valid_to IS NULL)
            OR (stg.client <> dwh.client)
            OR (stg.client IS NULL AND dwh.client IS NOT NULL)
            OR (stg.client IS NOT NULL and dwh.client IS NULL)
        )
WHEN NOT matched
    THEN 
        INSERT (
            account,
            valid_to,
            client,
            create_dt,
            effective_from,
            effective_to
            ) 
        VALUES (
            stg.account,
            stg.valid_to,
            stg.client,
            stg.create_dt,
            coalesce(stg.update_dt, stg.create_dt),
            to_date('2999-12-31 23:59:59','YYYY-MM-DD HH24:MI:SS')
            );
-- kela_dwh_dim_accounts_hist - добавляем новую версию изменённых записей/
INSERT INTO 
    kela_dwh_dim_accounts_hist (
        account,
        valid_to,
        client,
        create_dt,
        effective_from,
        effective_to
        ) 
SELECT 
    stg.account,
    stg.valid_to,
    stg.client,
    stg.create_dt,
    coalesce(stg.update_dt, stg.create_dt),
    to_date('2999-12-31 23:59:59','YYYY-MM-DD HH24:MI:SS')
FROM 
    kela_dwh_dim_accounts_hist dwh 
INNER join                                    
    kela_stg_accounts stg
    ON dwh.account = stg.account
WHERE
    current_date > (select max(effective_to) from kela_dwh_dim_accounts_hist where account = stg.account);  
-- kela_dwh_dim_accounts_hist - добавляем версию удалённых записей в целевую таблицу/
INSERT INTO 
    kela_dwh_dim_accounts_hist (
        account,
        valid_to,
        client,
        create_dt,
        effective_from,
        effective_to,
        deleted_flg
        )
SELECT
    account,
    valid_to,
    client,
    create_dt,
    current_date,
    to_date('2999-12-31 23:59:59','YYYY-MM-DD HH24:MI:SS'),
    '1'
FROM
    kela_dwh_dim_accounts_hist 
WHERE   
    current_date BETWEEN effective_from AND effective_to
    AND deleted_flg = '0'
    AND account IN (
        SELECT 
            dwh.account
        FROM 
            kela_dwh_dim_accounts_hist dwh 
        LEFT JOIN
            kela_stg_accounts_del stg
            ON dwh.account = stg.account
        WHERE
            stg.account IS NULL );
-- kela_dwh_dim_accounts_hist - обновляем конечную дату валидности у удалённых записей/
UPDATE 
    kela_dwh_dim_accounts_hist
SET 
    effective_to = current_date - INTERVAL '1' DAY
WHERE   
    current_date BETWEEN effective_from AND effective_to
    AND deleted_flg = '0'
    AND account IN (
        SELECT 
            dwh.account
        FROM 
            kela_dwh_dim_accounts_hist dwh 
        LEFT JOIN
            kela_stg_accounts_del stg
            ON dwh.account = stg.account
        WHERE
            stg.account IS NULL );
-- kela_dwh_dim_accounts_hist - обновляем метаданные/
UPDATE 
    kela_meta
SET 
    update_dt = coalesce ( 
        (SELECT 
            max(coalesce(stg.update_dt, stg.create_dt)) 
        FROM 
            kela_stg_accounts stg),
        update_dt)
WHERE
    schema_name = 'DE3AT' 
    AND table_name = 'KELA_DWH_DIM_ACCOUNTS_HIST';
-- kela_dwh_dim_clients_hist - заполняем временные хранилища/
INSERT INTO
    kela_stg_clients (client_id, last_name, first_name, patronymic, date_of_birth, passport_num, passport_valid_to, phone, 
        create_dt, update_dt)
WITH last_update_dt as (
    SELECT
        update_dt
    FROM
        kela_meta
    WHERE
        schema_name = 'DE3AT' AND
        table_name = 'KELA_DWH_DIM_CLIENTS_HIST'
    )
SELECT
    client_id, last_name, first_name, patronymic, date_of_birth, passport_num, passport_valid_to, phone, 
        create_dt, update_dt
FROM
    bank.clients
WHERE
    coalesce(update_dt, create_dt) > (select update_dt from last_update_dt)
    OR (select update_dt from last_update_dt) IS NULL;
-- kela_dwh_dim_clients_hist - заполняем таблицу контроля удалений/
INSERT INTO
    kela_stg_clients_del (client_id)
SELECT
    client_id
FROM
    bank.clients;
-- kela_dwh_dim_clients_hist - закрываем изменённые записи и добавляем новые/
MERGE INTO
    kela_dwh_dim_clients_hist dwh
USING
    kela_stg_clients stg
    on (dwh.client_id = stg.client_id)
WHEN matched THEN
    UPDATE
        SET
            effective_to = stg.update_dt - INTERVAL '1' DAY
        WHERE 
            current_date BETWEEN dwh.effective_from AND dwh.effective_to
        AND (1=0  
            OR (stg.last_name <> dwh.last_name)
            OR (stg.last_name IS NULL AND dwh.last_name IS NOT NULL)
            OR (stg.last_name IS NOT NULL and dwh.last_name IS NULL)
            OR (stg.first_name <> dwh.first_name)
            OR (stg.first_name IS NULL AND dwh.first_name IS NOT NULL)
            OR (stg.first_name IS NOT NULL and dwh.first_name IS NULL)
            OR (stg.patronymic <> dwh.patronymic)
            OR (stg.patronymic IS NULL AND dwh.patronymic IS NOT NULL)
            OR (stg.patronymic IS NOT NULL and dwh.patronymic IS NULL)
            OR (stg.date_of_birth <> dwh.date_of_birth)
            OR (stg.date_of_birth IS NULL AND dwh.date_of_birth IS NOT NULL)
            OR (stg.date_of_birth IS NOT NULL and dwh.date_of_birth IS NULL)
            OR (stg.passport_num <> dwh.passport_num)
            OR (stg.passport_num IS NULL AND dwh.passport_num IS NOT NULL)
            OR (stg.passport_num IS NOT NULL and dwh.passport_num IS NULL)
            OR (stg.passport_valid_to <> dwh.passport_valid_to)
            OR (stg.passport_valid_to IS NULL AND dwh.passport_valid_to IS NOT NULL)
            OR (stg.passport_valid_to IS NOT NULL and dwh.passport_valid_to IS NULL)
            OR (stg.phone <> dwh.phone)
            OR (stg.phone IS NULL AND dwh.phone IS NOT NULL)
            OR (stg.phone IS NOT NULL and dwh.phone IS NULL)
        )
WHEN NOT matched
    THEN 
        INSERT (
            client_id, 
            last_name, 
            first_name, 
            patronymic, 
            date_of_birth, 
            passport_num, 
            passport_valid_to, 
            phone, 
            create_dt,
            effective_from,
            effective_to
            ) 
        VALUES (
            stg.client_id,
            stg.last_name,
            stg.first_name,
            stg.patronymic,
            stg.date_of_birth,
            stg.passport_num,
            stg.passport_valid_to,
            stg.phone,
            stg.create_dt,
            coalesce(stg.update_dt, stg.create_dt),
            to_date('2999-12-31 23:59:59','YYYY-MM-DD HH24:MI:SS')
            );
-- kela_dwh_dim_clients_hist - добавляем новую версию изменённых записей/
INSERT INTO 
    kela_dwh_dim_clients_hist (
        client_id, 
        last_name, 
        first_name, 
        patronymic, 
        date_of_birth, 
        passport_num, 
        passport_valid_to, 
        phone, 
        create_dt,
        effective_from,
        effective_to
        ) 
SELECT 
    stg.client_id,
    stg.last_name,
    stg.first_name,
    stg.patronymic,
    stg.date_of_birth,
    stg.passport_num,
    stg.passport_valid_to,
    stg.phone,
    stg.create_dt,
    coalesce(stg.update_dt, stg.create_dt),
    to_date('2999-12-31 23:59:59','YYYY-MM-DD HH24:MI:SS')
FROM 
    kela_dwh_dim_clients_hist dwh 
INNER join                                    
    kela_stg_clients stg
    ON dwh.client_id = stg.client_id
WHERE
    current_date > (select max(effective_to) from kela_dwh_dim_clients_hist where client_id = stg.client_id);  
-- kela_dwh_dim_clients_hist - добавляем версию удалённых записей в целевую таблицу/
INSERT INTO 
    kela_dwh_dim_clients_hist (
        client_id, 
        last_name, 
        first_name, 
        patronymic, 
        date_of_birth, 
        passport_num, 
        passport_valid_to, 
        phone, 
        create_dt,
        effective_from,
        effective_to,
        deleted_flg
        )
SELECT
    client_id, 
    last_name, 
    first_name, 
    patronymic, 
    date_of_birth, 
    passport_num, 
    passport_valid_to, 
    phone, 
    create_dt,
    current_date,
    to_date('2999-12-31 23:59:59','YYYY-MM-DD HH24:MI:SS'),
    '1'
FROM
    kela_dwh_dim_clients_hist 
WHERE   
    current_date BETWEEN effective_from AND effective_to
    AND deleted_flg = '0'
    AND client_id IN (
        SELECT 
            dwh.client_id
        FROM 
            kela_dwh_dim_clients_hist dwh 
        LEFT JOIN 
            kela_stg_clients_del stg
            ON dwh.client_id = stg.client_id
        WHERE
            stg.client_id IS NULL );
-- kela_dwh_dim_clients_hist - обновляем конечную дату валидности у удалённых записей/
UPDATE 
    kela_dwh_dim_clients_hist
SET 
    effective_to = current_date - INTERVAL '1' DAY
WHERE   
    current_date BETWEEN effective_from AND effective_to
    AND deleted_flg = '0'
    AND client_id IN (
        SELECT 
            dwh.client_id
        FROM 
            kela_dwh_dim_clients_hist dwh 
        LEFT JOIN
            kela_stg_clients_del stg
            ON dwh.client_id = stg.client_id
        WHERE
            stg.client_id IS NULL );
-- kela_dwh_dim_clients_hist - обновляем метаданные/
UPDATE 
    kela_meta
SET 
    update_dt = coalesce ( 
        (SELECT 
            max(coalesce(stg.update_dt, stg.create_dt)) 
        FROM 
            kela_stg_clients stg),
        update_dt)
WHERE
    schema_name = 'DE3AT' 
    AND table_name = 'KELA_DWH_DIM_CLIENTS_HIST'