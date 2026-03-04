import json
from django.shortcuts import render
from django.http import JsonResponse
from django.views.decorators.http import require_http_methods
from django.views.decorators.csrf import ensure_csrf_cookie

from Functions.DataBaseConnection import (
    get_connection_error,
    is_connected,
    get_tables_list,
    get_table_data,
)


@ensure_csrf_cookie
def chat_page(request):
    """Главная страница — чат для работы с БД."""
    if is_connected():
        db_status = 'connected'
        db_status_text = 'БД подключена'
    else:
        db_status = 'disconnected'
        err = get_connection_error()
        db_status_text = 'БД не подключена' + (f': {err}' if err else '')
    return render(request, 'chat.html', {
        'db_status': db_status,
        'db_status_text': db_status_text,
    })


@require_http_methods(['POST'])
def chat_api(request):
    """API чата: при наличии AI_API_KEY — ИИ-аналитика по продажам RK7, иначе подсказка."""
    try:
        data = json.loads(request.body)
        message = (data.get('message') or '').strip()
    except Exception:
        return JsonResponse({'reply': 'Неверный формат запроса.'}, status=400)

    from django.conf import settings
    if getattr(settings, 'AI_API_KEY', ''):
        from services.ai_sales_service import handle_user_query
        reply = handle_user_query(message)
    else:
        reply = (
            'Добавьте в .env переменную AI_API_KEY (ключ OpenAI), '
            'чтобы включить ИИ-ответы по продажам R-Keeper 7.'
        )
    return JsonResponse({'reply': reply})


@ensure_csrf_cookie
def table_data_page(request):
    """Страница выбора таблицы и просмотра данных."""
    return render(request, 'table_data.html', {})


@require_http_methods(['GET'])
def tables_list_api(request):
    """API: список таблиц для выбора."""
    tables = get_tables_list()
    if tables is None:
        return JsonResponse({'error': 'Нет подключения к БД.'}, status=503)
    return JsonResponse({'tables': tables})


@require_http_methods(['POST'])
def table_data_api(request):
    """API: выгрузка данных таблицы (имя и лимит в JSON)."""
    try:
        data = json.loads(request.body)
        table_name = (data.get('table_name') or '').strip()
        limit = int(data.get('limit', 10))
    except (json.JSONDecodeError, TypeError, ValueError):
        return JsonResponse({'error': 'Неверный формат запроса.'}, status=400)
    result = get_table_data(table_name, limit=limit)
    if result is None:
        return JsonResponse({'error': 'Таблица не найдена или ошибка выгрузки.'}, status=400)
    return JsonResponse(result)
