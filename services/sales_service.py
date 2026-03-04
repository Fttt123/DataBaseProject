"""
Продажи RK7: только выполнение SQL через функции.
Использует product_search_service и date_service. Без фильтра FINISHED по умолчанию для отладки.
"""
import logging
from datetime import datetime, timedelta

from Functions.DataBaseConnection import get_connection

from services.product_search_service import search_product
from services.date_service import get_month_range

logger = logging.getLogger(__name__)

MAX_DATE_RANGE_DAYS = 730
MAX_TOP_N = 50


def run_query(sql: str, params: tuple):
    """Выполнение запроса к БД RK7. Возвращает список строк или None."""
    conn = get_connection()
    if not conn:
        return None
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall()
    except Exception as e:
        logger.exception("Ошибка запроса к БД: %s", e)
        return None


def get_sales_by_product(product_ids: list, date_from: str, date_to: str, use_finished: bool = False):
    """
    Сумма QUANTITY и PAYSUM по списку блюд (MENUITEMS.SIFR) за период.
    Цепочка: MENUITEMS.SIFR = SESSIONDISHES.SIFR, SESSIONDISHES.DISHUNI = PAYBINDINGS.DISHUNI, PAYBINDINGS.VISIT = VISITS.SIFR.
    date_from, date_to — строки 'YYYY-MM-DD HH:MM:SS'.
    use_finished: если True, фильтровать по v.FINISHED = 1.
    """
    if not product_ids:
        return None
    placeholders = ",".join(["%s"] * len(product_ids))
    finished_clause = " AND v.FINISHED = 1" if use_finished else ""
    sql = f"""
        SELECT ISNULL(SUM(pb.QUANTITY), 0), ISNULL(SUM(pb.PAYSUM), 0)
        FROM dbo.PAYBINDINGS pb
        INNER JOIN dbo.SESSIONDISHES sd ON sd.UNI = pb.DISHUNI
        INNER JOIN dbo.VISITS v ON pb.VISIT = v.SIFR
        WHERE sd.SIFR IN ({placeholders})
          AND v.QUITTIME >= %s
          AND v.QUITTIME < %s
          {finished_clause}
    """
    params = list(product_ids) + [date_from, date_to]
    rows = run_query(sql, tuple(params))
    if rows is None:
        return None
    qty = int(rows[0][0]) if rows and rows[0][0] is not None else 0
    rev = float(rows[0][1]) if rows and rows[0][1] is not None else 0.0
    return (qty, rev)


def get_product_sales_for_month(product_name: str, year: int, month: int, use_finished: bool = False):
    """
    Продажи по товару за месяц: поиск товара (3 этапа) → диапазон месяца → get_sales_by_product.
    Возвращает dict: products_found, quantity, revenue или error.
    """
    products = search_product(product_name)
    if not products:
        return {"error": "Товар не найден", "products_found": []}

    product_ids = [p[0] for p in products]
    date_from, date_to = get_month_range(year, month)
    result = get_sales_by_product(product_ids, date_from, date_to, use_finished=use_finished)

    if result is None:
        return {"error": "Ошибка запроса к БД", "products_found": [p[1] for p in products]}

    quantity, revenue = result
    return {
        "products_found": [p[1] for p in products],
        "quantity": quantity,
        "revenue": revenue,
        "year": year,
        "month": month,
    }


def _parse_date_range(date_from: str, date_to: str):
    """Преобразует date_from/date_to в (start, end_exclusive) для SQL: QUITTIME >= start AND QUITTIME < end_exclusive."""
    try:
        d1 = datetime.strptime(date_from[:10], "%Y-%m-%d")
        d2 = datetime.strptime(date_to[:10], "%Y-%m-%d")
        start = d1.strftime("%Y-%m-%d %H:%M:%S")
        end_exclusive = (d2 + timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")
        return start, end_exclusive
    except (ValueError, TypeError):
        return None, None


def get_product_sales_for_range(product_name: str, date_from: str, date_to: str, use_finished: bool = False):
    """Продажи по товару за произвольный период (date_from, date_to — YYYY-MM-DD)."""
    products = search_product(product_name)
    if not products:
        return {"error": "Товар не найден", "products_found": []}

    start, end = _parse_date_range(date_from, date_to)
    if not start or not end:
        return {"error": "Неверный формат дат", "products_found": [p[1] for p in products]}

    result = get_sales_by_product([p[0] for p in products], start, end, use_finished=use_finished)
    if result is None:
        return {"error": "Ошибка запроса к БД", "products_found": [p[1] for p in products]}

    quantity, revenue = result
    return {
        "products_found": [p[1] for p in products],
        "quantity": quantity,
        "revenue": revenue,
        "date_from": date_from,
        "date_to": date_to,
    }


def get_total_sales(date_from: str, date_to: str, use_finished: bool = False):
    """Общая выручка за период (SUM(PAYSUM))."""
    start, end = _parse_date_range(date_from, date_to)
    if not start or not end:
        return None
    finished_clause = " AND v.FINISHED = 1" if use_finished else ""
    sql = f"""
        SELECT ISNULL(SUM(pb.PAYSUM), 0)
        FROM dbo.PAYBINDINGS pb
        INNER JOIN dbo.VISITS v ON pb.VISIT = v.SIFR
        WHERE v.QUITTIME >= %s AND v.QUITTIME < %s
        {finished_clause}
    """
    rows = run_query(sql, (start, end))
    if rows is None:
        return None
    return float(rows[0][0]) if rows and rows[0][0] is not None else 0.0


def get_top_products(date_from: str, date_to: str, limit: int = 10, use_finished: bool = False):
    """Топ товаров по количеству продаж за период. Цепочка: MENUITEMS → SESSIONDISHES → PAYBINDINGS → VISITS."""
    start, end = _parse_date_range(date_from, date_to)
    if not start or not end:
        return None
    limit = min(max(1, limit), MAX_TOP_N)
    finished_clause = " AND v.FINISHED = 1" if use_finished else ""
    sql = f"""
        SELECT TOP (%s) mi.NAME, SUM(pb.QUANTITY) AS TotalQty
        FROM dbo.PAYBINDINGS pb
        INNER JOIN dbo.SESSIONDISHES sd ON sd.DISHUNI = pb.DISHUNI
        INNER JOIN dbo.MENUITEMS mi ON mi.SIFR = sd.SIFR
        INNER JOIN dbo.VISITS v ON pb.VISIT = v.SIFR
        WHERE v.QUITTIME >= %s AND v.QUITTIME < %s
        {finished_clause}
        GROUP BY mi.NAME
        ORDER BY TotalQty DESC
    """
    rows = run_query(sql, (limit, start, end))
    if rows is None:
        return None
    return [{"name": row[0], "qty": int(row[1])} for row in rows]
