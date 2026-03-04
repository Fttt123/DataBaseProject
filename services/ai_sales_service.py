"""
AI-аналитика по продажам R-Keeper 7.

ИИ возвращает только JSON (action + parameters). SQL формирует только Python.
Допустимые действия — жёсткий whitelist. Параметры валидируются (даты, лимиты).
"""
import json
import logging
from datetime import datetime

from django.conf import settings

logger = logging.getLogger(__name__)

# Список разрешённых действий (никогда не выполняем произвольный SQL от ИИ)
ALLOWED_ACTIONS = {
    "get_product_sales",
    "get_total_sales",
    "compare_month_sales",
    "get_top_products",
    "chat",
    "unknown",
}

# Ограничения безопасности
MAX_DATE_RANGE_DAYS = 730  # не более 2 лет
MAX_TOP_N = 50

SYSTEM_PROMPT_TEMPLATE = """Ты дружелюбный AI-ассистент для системы R-Keeper 7 (продажи, чеки, номенклатура).

Текущая дата: {current_date}. Используй её, когда пользователь говорит «за прошлый месяц», «в этом месяце», «за ноябрь» и т.п. — подставляй конкретные date_from, date_to в формате YYYY-MM-DD.

Ты НЕ пишешь SQL. Ты анализируешь запрос и возвращаешь ОДИН JSON-объект без markdown и без текста вокруг.

Формат ответа (строго только JSON):
{{"action": "название_операции", "parameters": {{...}}}}

Доступные action:

1) chat — обычный разговор: приветствие, благодарность, общие вопросы не про отчёты. Ответь коротко и по-человечески.
   parameters: {{"message": "твой ответ пользователю текстом"}}

2) get_product_sales — продажи товара за период. Либо year+month: {{"product_name": "название", "year": 2026, "month": 2}}, либо {{"product_name": "название", "date_from": "YYYY-MM-DD", "date_to": "YYYY-MM-DD"}}

3) get_total_sales — общая выручка за период
   parameters: {{"date_from": "YYYY-MM-DD", "date_to": "YYYY-MM-DD"}}

4) compare_month_sales — сравнение продаж товара в месяце со средним за предыдущие месяцы
   parameters: {{"product_name": "название", "month": "YYYY-MM"}}

5) get_top_products — топ товаров по количеству продаж за период
   parameters: {{"date_from": "YYYY-MM-DD", "date_to": "YYYY-MM-DD", "limit": 10}}
   limit по умолчанию 10, максимум 50.

Для get_product_sales можно передать период двумя способами:
   - по месяцу: {{"product_name": "название", "year": 2026, "month": 2}} (удобно для «за прошлый месяц»);
   - по диапазону: {{"product_name": "название", "date_from": "YYYY-MM-DD", "date_to": "YYYY-MM-DD"}}.

Если запрос про продажи/аналитику — выбери подходящий action 2–5 и подставь даты или year/month относительно текущей даты.
Если запрос — просто общение (привет, спасибо, как дела, что умеешь) — верни action "chat" с твоим ответом в parameters.message.
Если запрос непонятен — верни: {{"action": "unknown", "parameters": {{}}}}.

Даты: YYYY-MM-DD или year (число), month (число 1–12). Никакого текста вне JSON."""


def _validate_date_range(date_from: str, date_to: str) -> bool:
    """Проверка: период не больше MAX_DATE_RANGE_DAYS."""
    try:
        d1 = datetime.strptime(date_from[:10], "%Y-%m-%d").date()
        d2 = datetime.strptime(date_to[:10], "%Y-%m-%d").date()
        if d1 > d2:
            return False
        return (d2 - d1).days <= MAX_DATE_RANGE_DAYS
    except (ValueError, TypeError):
        return False


# Используем сервисы: SQL только через sales_service, поиск товара через product_search_service.
USE_FINISHED_FILTER = False  # Включить True, когда убедимся, что данные корректны (QUITTIME не NULL и т.д.)


def ask_ai_for_action(user_message: str):
    """
    Отправляет запрос пользователя в ИИ, ожидает только JSON с action и parameters.
    В промпт подставляется текущая дата для ориентации по «прошлый месяц» и т.п.
    Возвращает dict или None при ошибке. Валидирует action по whitelist.
    """
    api_key = getattr(settings, "AI_API_KEY", "") or ""
    if not api_key:
        return None
    current_date = datetime.now().strftime("%Y-%m-%d")
    system_prompt = SYSTEM_PROMPT_TEMPLATE.format(current_date=current_date)
    try:
        from openai import OpenAI
        client = OpenAI(api_key=api_key)
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_message},
            ],
            temperature=0,
        )
        content = (response.choices[0].message.content or "").strip()
        # Убрать markdown-обёртки если ИИ вернул ```json ... ```
        if content.startswith("```"):
            lines = content.split("\n")
            content = "\n".join(lines[1:-1] if lines[-1].strip() == "```" else lines[1:])
        data = json.loads(content)
        action = (data.get("action") or "").strip()
        if action not in ALLOWED_ACTIONS:
            logger.warning("ИИ вернул недопустимый action: %s", action)
            return {"action": "unknown", "parameters": {}}
        data["parameters"] = data.get("parameters") or {}
        logger.info("AI action: %s, params: %s", action, data["parameters"])
        return data
    except json.JSONDecodeError as e:
        logger.warning("ИИ вернул не JSON: %s", e)
        return None
    except Exception as e:
        logger.exception("Ошибка вызова ИИ: %s", e)
        return None


def generate_human_response(raw_result: str, user_message: str) -> str:
    """Второй проход ИИ: превращает сырые данные в понятный аналитический ответ."""
    api_key = getattr(settings, "AI_API_KEY", "") or ""
    if not api_key:
        return raw_result
    try:
        from openai import OpenAI
        client = OpenAI(api_key=api_key)
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "Ты аналитик. Кратко и по делу оформи ответ на основе данных. Без лишних вступлений. На русском."},
                {"role": "user", "content": f"Вопрос пользователя: {user_message}\n\nДанные из БД: {raw_result}\n\nСформируй краткий ответ."},
            ],
            temperature=0.3,
        )
        return (response.choices[0].message.content or raw_result).strip()
    except Exception as e:
        logger.warning("Ошибка форматирования ответа ИИ: %s", e)
        return raw_result


def handle_user_query(user_message: str) -> str:
    """
    Главный обработчик: ИИ → JSON → валидация → свой SQL → результат → (опционально) ИИ форматирует ответ.
    """
    if not (user_message or "").strip():
        return "Напишите запрос по продажам (период, товар, топ товаров и т.п.)."

    ai_data = ask_ai_for_action(user_message)
    if not ai_data:
        return "Сейчас не удалось обработать запрос (нет API-ключа или ошибка ИИ). Проверьте настройки."

    action = ai_data.get("action", "")
    params = ai_data.get("parameters", {})

    if action == "unknown":
        return "Я могу отвечать только по продажам и аналитике R-Keeper 7. Уточните вопрос (например: продажи за период, топ товаров, сравнение месяцев)."

    if action == "chat":
        return (params.get("message") or "").strip() or "Чем могу помочь?"

    raw_result = None

    if action == "get_product_sales":
        from services.sales_service import get_product_sales_for_month, get_product_sales_for_range
        pn = (params.get("product_name") or "").strip()
        if not pn:
            raw_result = "Не указано название товара."
        elif params.get("year") is not None and params.get("month") is not None:
            try:
                y, m = int(params["year"]), int(params["month"])
                if not (1 <= m <= 12):
                    raw_result = "Месяц должен быть от 1 до 12."
                else:
                    res = get_product_sales_for_month(pn, y, m, use_finished=USE_FINISHED_FILTER)
                    if "error" in res:
                        raw_result = res["error"]
                        if res.get("products_found"):
                            raw_result += f" Найдены: {', '.join(res['products_found'][:5])}."
                    else:
                        qty, rev = res["quantity"], res["revenue"]
                        names = ", ".join(res["products_found"][:3])
                        raw_result = f"За {y}-{m:02d} по товару «{pn}» (учтено: {names}): продано {qty} шт., выручка {rev:,.2f} руб."
            except (ValueError, TypeError):
                raw_result = "Неверный год или месяц."
        else:
            df = (params.get("date_from") or "")[:10]
            dt = (params.get("date_to") or "")[:10]
            if not df or not dt:
                raw_result = "Укажите период: date_from и date_to (YYYY-MM-DD) или year и month."
            elif not _validate_date_range(df, dt):
                raw_result = "Период слишком большой или даты неверные."
            else:
                res = get_product_sales_for_range(pn, df, dt, use_finished=USE_FINISHED_FILTER)
                if "error" in res:
                    raw_result = res["error"]
                    if res.get("products_found"):
                        raw_result += f" Найдены: {', '.join(res['products_found'][:5])}."
                else:
                    qty, rev = res["quantity"], res["revenue"]
                    names = ", ".join(res["products_found"][:3])
                    raw_result = f"За {df} — {dt} по товару «{pn}» (учтено: {names}): продано {qty} шт., выручка {rev:,.2f} руб."

    elif action == "get_total_sales":
        from services.sales_service import get_total_sales as svc_get_total_sales
        df = (params.get("date_from") or "")[:10]
        dt = (params.get("date_to") or "")[:10]
        if not df or not dt:
            raw_result = "Укажите период (date_from, date_to)."
        elif not _validate_date_range(df, dt):
            raw_result = "Период слишком большой или даты неверные."
        else:
            val = svc_get_total_sales(df, dt, use_finished=USE_FINISHED_FILTER)
            if val is None:
                raw_result = "Ошибка запроса к БД."
            else:
                raw_result = f"Общая выручка за {df} — {dt}: {val:,.2f} руб."

    elif action == "compare_month_sales":
        from services.sales_service import get_product_sales_for_month
        pn = (params.get("product_name") or "").strip()
        month = (params.get("month") or "")[:7]
        if not pn or not month:
            raw_result = "Укажите товар (product_name) и месяц (YYYY-MM)."
        else:
            try:
                year, month_num = int(month[:4]), int(month[5:7])
                if not (1 <= month_num <= 12):
                    raw_result = "Неверный месяц."
                else:
                    res_cur = get_product_sales_for_month(pn, year, month_num, use_finished=USE_FINISHED_FILTER)
                    if "error" in res_cur:
                        raw_result = res_cur["error"]
                    else:
                        cur_qty = res_cur["quantity"]
                        prev_qtys = []
                        for i in range(1, 4):
                            pm = month_num - i
                            py = year
                            if pm < 1:
                                pm += 12
                                py -= 1
                            r = get_product_sales_for_month(pn, py, pm, use_finished=USE_FINISHED_FILTER)
                            if "error" not in r:
                                prev_qtys.append(r["quantity"])
                        avg_prev = sum(prev_qtys) / len(prev_qtys) if prev_qtys else None
                        pct = ((cur_qty - avg_prev) / avg_prev * 100) if avg_prev else None
                        raw_result = f"За {month} товар «{pn}»: продано {cur_qty} шт."
                        if avg_prev is not None:
                            raw_result += f" Среднее за предыдущие 3 месяца: {avg_prev:.1f}. "
                        if pct is not None:
                            raw_result += f" Изменение: {pct:+.1f}%."
            except (ValueError, TypeError):
                raw_result = "Неверный формат месяца (YYYY-MM)."

    elif action == "get_top_products":
        from services.sales_service import get_top_products as svc_get_top_products
        df = (params.get("date_from") or "")[:10]
        dt = (params.get("date_to") or "")[:10]
        limit = min(max(1, int(params.get("limit") or 10)), MAX_TOP_N)
        if not df or not dt:
            raw_result = "Укажите период (date_from, date_to)."
        elif not _validate_date_range(df, dt):
            raw_result = "Период слишком большой или даты неверные."
        else:
            rows = svc_get_top_products(df, dt, limit=limit, use_finished=USE_FINISHED_FILTER)
            if rows is None:
                raw_result = "Ошибка запроса к БД."
            else:
                lines = [f"{i+1}. {r['name']}: {r['qty']} шт." for i, r in enumerate(rows)]
                raw_result = "Топ за период:\n" + "\n".join(lines)

    else:
        raw_result = "Неизвестная операция."

    return generate_human_response(raw_result, user_message)
