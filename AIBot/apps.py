import warnings

from django.apps import AppConfig

# Убираем предупреждения cryptography/paramiko про TripleDES (не ошибка, на работу не влияет)
try:
    from cryptography.utils import CryptographyDeprecationWarning
    warnings.filterwarnings('ignore', category=CryptographyDeprecationWarning)
except ImportError:
    warnings.filterwarnings('ignore', message='.*TripleDES.*', module='paramiko')


class AibotConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'AIBot'

    def ready(self):
        """При запуске приложения выполняем подключение к внешней БД."""
        from Functions.DataBaseConnection import connect
        connect()
