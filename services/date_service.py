"""
Сервис работы с датами для отчётов RK7.
Стандарт: исключающая верхняя граница — в SQL всегда QUITTIME >= start AND QUITTIME < end.
"""
from datetime import datetime


def get_month_range(year: int, month: int):
    """
    Возвращает (start, end_exclusive) для указанного месяца.
    start — первый день месяца 00:00:00.
    end_exclusive — первый день СЛЕДУЮЩЕГО месяца 00:00:00 (для условия QUITTIME < end).
    Так период всегда корректен и не зависит от 23:59:59 и дробных секунд.
    """
    start = datetime(year, month, 1, 0, 0, 0)
    if month == 12:
        end_exclusive = datetime(year + 1, 1, 1, 0, 0, 0)
    else:
        end_exclusive = datetime(year, month + 1, 1, 0, 0, 0)
    return start.strftime("%Y-%m-%d %H:%M:%S"), end_exclusive.strftime("%Y-%m-%d %H:%M:%S")
