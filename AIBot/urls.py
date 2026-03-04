from django.urls import path
from . import views

urlpatterns = [
    path('', views.chat_page, name='chat'),
    path('data/', views.table_data_page, name='table_data'),
    path('api/chat/', views.chat_api, name='chat_api'),
    path('api/tables/', views.tables_list_api, name='tables_list_api'),
    path('api/table-data/', views.table_data_api, name='table_data_api'),
]
