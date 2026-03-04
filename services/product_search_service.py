"""
Поиск товаров в MENUITEMS с fallback: точное совпадение → LIKE → по словам.
Возвращает список (SIFR, NAME) для подстановки в запросы продаж.
"""
import logging

logger = logging.getLogger(__name__)


def _run_query(sql: str, params: tuple):
    """Выполнение запроса к БД RK7 (внешнее подключение)."""
    from Functions.DataBaseConnection import get_connection
    conn = get_connection()
    if not conn:
        return None
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall()
    except Exception as e:
        logger.warning("Ошибка запроса поиска товара: %s", e)
        return None


def find_product_exact(name: str):
    """Этап 1: точное совпадение NAME = name."""
    if not (name or "").strip():
        return []
    sql = """
        SELECT SIFR, NAME
        FROM dbo.MENUITEMS
        WHERE NAME = %s
    """
    rows = _run_query(sql, (name.strip(),))
    return list(rows) if rows else []


def find_product_like(name: str):
    """Этап 2: поиск по подстроке NAME LIKE %name%."""
    if not (name or "").strip():
        return []
    sql = """
        SELECT SIFR, NAME
        FROM dbo.MENUITEMS
        WHERE NAME LIKE %s
    """
    rows = _run_query(sql, (f"%{name.strip()}%",))
    return list(rows) if rows else []


def find_product_by_words(name: str):
    """Этап 3: все слова запроса должны встречаться в NAME (для «пиццы» → «Пицца Маргарита»)."""
    words = [w.strip() for w in (name or "").split() if w.strip()]
    if not words:
        return []
    conditions = ["NAME LIKE %s"] * len(words)
    params = [f"%{w}%" for w in words]
    where_clause = " AND ".join(conditions)
    sql = f"""
        SELECT SIFR, NAME
        FROM dbo.MENUITEMS
        WHERE {where_clause}
    """
    rows = _run_query(sql, tuple(params))
    return list(rows) if rows else []


def search_product(name: str):
    """
    Умный поиск товара: точное → LIKE → по словам.
    Возвращает список кортежей (SIFR, NAME) или пустой список.
    """
    if not (name or "").strip():
        return []
    n = name.strip()
    result = find_product_exact(n)
    if result:
        return result
    result = find_product_like(n)
    if result:
        return result
    result = find_product_by_words(n)
    if result:
        return result
    return []
