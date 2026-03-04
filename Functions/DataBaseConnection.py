"""
Подключение к внешней БД MSSQL.
Поддержка SSH-туннеля для доступа к БД.
Все функции, связанные с подключением к БД, находятся в этом модуле.
"""
import logging

logger = logging.getLogger(__name__)

# Глобальные объекты: туннель и соединение (заполняются при вызове connect)
_ssh_tunnel = None
_db_connection = None
_connection_error = None


def get_connection():
    """Возвращает активное соединение с БД или None при ошибке."""
    return _db_connection


def get_current_database():
    """Возвращает имя текущей базы данных подключения или None."""
    conn = get_connection()
    if not conn:
        return None
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT DB_NAME()")
            row = cur.fetchone()
            return row[0] if row else None
    except Exception:
        return None


def get_connection_error():
    """Возвращает сообщение об ошибке подключения или None."""
    return _connection_error


def get_databases_list():
    """Возвращает список имён баз данных на сервере (для подсказки при выборе DB_NAME)."""
    conn = get_connection()
    if not conn:
        return None
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT name FROM sys.databases ORDER BY name")
            return [row[0] for row in cur.fetchall()]
    except Exception as e:
        logger.warning("Не удалось получить список баз: %s", e)
        return None


def is_connected():
    """Проверка, установлено ли соединение с БД."""
    if _db_connection is None:
        return False
    try:
        with _db_connection.cursor() as cur:
            cur.execute("SELECT 1")
        return True
    except Exception:
        return False


def connect():
    """
    Устанавливает соединение с MSSQL.
    При USE_SSH=true поднимает SSH-туннель, затем подключается к БД.
    Вызывается из AppConfig.ready() при старте Django.
    """
    global _ssh_tunnel, _db_connection, _connection_error

    if _db_connection is not None and is_connected():
        return
    _connection_error = None

    from django.conf import settings
    cfg = settings.DB_CONFIG

    if not cfg.get('password'):
        _connection_error = "Не задан DB_PASSWORD в .env"
        logger.warning(_connection_error)
        return

    try:
        if cfg.get('use_ssh'):
            import sshtunnel
            _ssh_tunnel = sshtunnel.SSHTunnelForwarder(
                (cfg['ssh_host'], cfg['ssh_port']),
                ssh_username=cfg['ssh_user'],
                ssh_password=cfg.get('ssh_password') or None,
                remote_bind_address=(cfg['host'], cfg['port']),
            )
            _ssh_tunnel.start()
            host = '127.0.0.1'
            port = _ssh_tunnel.local_bind_port
            logger.info("SSH туннель запущен: %s -> %s:%s", cfg['ssh_host'], cfg['host'], cfg['port'])
        else:
            host = cfg['host']
            port = cfg['port']

        import pymssql
        _db_connection = pymssql.connect(
            server=host,
            port=port,
            user=cfg['user'],
            password=cfg['password'],
            database=cfg['database'],
        )
        logger.info("Подключение к БД установлено: %s@%s", cfg['user'], cfg['database'])
    except Exception as e:
        _connection_error = str(e)
        logger.exception("Ошибка подключения к БД: %s", e)
        if _ssh_tunnel:
            try:
                _ssh_tunnel.stop()
            except Exception:
                pass
            _ssh_tunnel = None
        _db_connection = None


def disconnect():
    """Закрывает соединение и SSH-туннель (для корректного завершения)."""
    global _ssh_tunnel, _db_connection
    if _db_connection:
        try:
            _db_connection.close()
        except Exception:
            pass
        _db_connection = None
    if _ssh_tunnel:
        try:
            _ssh_tunnel.stop()
        except Exception:
            pass
        _ssh_tunnel = None
    logger.info("Соединение с БД закрыто.")


def get_schema():
    """
    Возвращает схему БД: список таблиц с колонками и связи (FK).
    Учитывает только пользовательские схемы (исключены sys, INFORMATION_SCHEMA).
    Важно: в .env задайте DB_NAME = имя базы, где лежат ваши таблицы (не master,
    если ваши таблицы в другой базе).
    Возвращает None при ошибке или отсутствии подключения.
    """
    conn = get_connection()
    if not conn:
        return None
    try:
        with conn.cursor() as cur:
            # Только пользовательские таблицы; исключаем системные схемы и таблицы master (spt_*)
            cur.execute("""
                SELECT TABLE_SCHEMA, TABLE_NAME
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_TYPE = 'BASE TABLE'
                  AND TABLE_SCHEMA NOT IN ('sys', 'INFORMATION_SCHEMA')
                  AND TABLE_NAME NOT LIKE 'spt_%'
                ORDER BY TABLE_SCHEMA, TABLE_NAME
            """)
            tables_rows = cur.fetchall()
            # Колонки только для этих схем и без spt_*
            cur.execute("""
                SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA NOT IN ('sys', 'INFORMATION_SCHEMA')
                  AND TABLE_NAME NOT LIKE 'spt_%'
                ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION
            """)
            columns_rows = cur.fetchall()
            # Связи (FK) — только между таблицами пользовательских схем
            cur.execute("""
                SELECT
                    KCU1.TABLE_SCHEMA + '.' + KCU1.TABLE_NAME,
                    KCU1.COLUMN_NAME,
                    KCU2.TABLE_SCHEMA + '.' + KCU2.TABLE_NAME,
                    KCU2.COLUMN_NAME
                FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc
                INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE KCU1
                    ON KCU1.CONSTRAINT_CATALOG = rc.CONSTRAINT_CATALOG
                    AND KCU1.CONSTRAINT_SCHEMA = rc.CONSTRAINT_SCHEMA
                    AND KCU1.CONSTRAINT_NAME = rc.CONSTRAINT_NAME
                    AND KCU1.TABLE_SCHEMA NOT IN ('sys', 'INFORMATION_SCHEMA')
                    AND KCU1.TABLE_NAME NOT LIKE 'spt_%'
                INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE KCU2
                    ON KCU2.CONSTRAINT_CATALOG = rc.UNIQUE_CONSTRAINT_CATALOG
                    AND KCU2.CONSTRAINT_SCHEMA = rc.UNIQUE_CONSTRAINT_SCHEMA
                    AND KCU2.CONSTRAINT_NAME = rc.UNIQUE_CONSTRAINT_NAME
                    AND KCU2.ORDINAL_POSITION = KCU1.ORDINAL_POSITION
                    AND KCU2.TABLE_SCHEMA NOT IN ('sys', 'INFORMATION_SCHEMA')
                    AND KCU2.TABLE_NAME NOT LIKE 'spt_%'
            """)
            fk_rows = cur.fetchall()
        # Build tables list with columns (use short names: schema.table)
        schema_tables = {}
        for row in tables_rows:
            schema, name = row[0], row[1]
            full_name = f"{schema}.{name}"
            schema_tables[full_name] = {"name": full_name, "columns": []}
        for row in columns_rows:
            schema, tname, col_name, data_type = row[0], row[1], row[2], row[3]
            full_name = f"{schema}.{tname}"
            if full_name in schema_tables:
                schema_tables[full_name]["columns"].append({
                    "name": col_name,
                    "data_type": data_type or "",
                })
        tables = list(schema_tables.values())
        relations = []
        for row in fk_rows:
            relations.append({
                "from_table": row[0],
                "from_column": row[1],
                "to_table": row[2],
                "to_column": row[3],
            })
        return {"tables": tables, "relations": relations}
    except Exception as e:
        logger.exception("Ошибка получения схемы БД: %s", e)
        return None


def get_tables_list():
    """Возвращает список имён таблиц в формате schema.name (для выбора и валидации)."""
    conn = get_connection()
    if not conn:
        return None
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT TABLE_SCHEMA, TABLE_NAME
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_TYPE = 'BASE TABLE'
                  AND TABLE_SCHEMA NOT IN ('sys', 'INFORMATION_SCHEMA')
                  AND TABLE_NAME NOT LIKE 'spt_%%'
                ORDER BY TABLE_SCHEMA, TABLE_NAME
            """)
            return [f"{row[0]}.{row[1]}" for row in cur.fetchall()]
    except Exception as e:
        logger.exception("Ошибка получения списка таблиц: %s", e)
        return None


def get_table_data(table_name, limit=10):
    """
    Выгружает до limit записей из таблицы.
    table_name: полное имя (schema.table) или только имя таблицы.
    Возвращает {"columns": [...], "rows": [[...], ...]} или None при ошибке.
    """
    conn = get_connection()
    if not conn:
        return None
    tables = get_tables_list()
    if not tables:
        return None
    table_name = (table_name or "").strip()
    if not table_name:
        return None
    full_name = None
    if "." in table_name:
        if table_name in tables:
            full_name = table_name
    else:
        for t in tables:
            if t.split(".")[-1].upper() == table_name.upper():
                full_name = t
                break
    if not full_name:
        return None
    schema, name = full_name.split(".", 1)
    limit_val = min(max(1, int(limit)), 1000)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT TOP (%s) * FROM [%s].[%s]" % (limit_val, schema, name),
            )
            columns = [desc[0] for desc in cur.description]
            rows = [list(row) for row in cur.fetchall()]
            for row in rows:
                for i, v in enumerate(row):
                    if v is not None and hasattr(v, "isoformat"):
                        row[i] = v.isoformat()
            return {"columns": columns, "rows": rows}
    except Exception as e:
        logger.exception("Ошибка выгрузки данных таблицы %s: %s", full_name, e)
        return None
