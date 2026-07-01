import os
from pathlib import Path
from decouple import config

BASE_DIR = Path(__file__).resolve().parent.parent


def _env_list(name: str, default: str) -> list[str]:
    raw = config(name, default=default)
    raw = raw.strip()
    if raw.startswith('[') and raw.endswith(']'):
        raw = raw[1:-1]
    return [
        item.strip().strip('"').strip("'")
        for item in raw.split(',')
        if item.strip().strip('"').strip("'")
    ]


SECRET_KEY = config('SECRET_KEY')
DEBUG = config('DEBUG', default=False, cast=bool)
ALLOWED_HOSTS = _env_list('ALLOWED_HOSTS', 'localhost,127.0.0.1,10.110.202.84')

INSTALLED_APPS = [
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',

    # Apps de terceros
    'channels',
    'rest_framework',

    # Apps locales
    'queue_app',
]

MIDDLEWARE = [
    'django.middleware.security.SecurityMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
]

ROOT_URLCONF = 'config.urls'

TEMPLATES = [{
    'BACKEND': 'django.template.backends.django.DjangoTemplates',
    'DIRS': [BASE_DIR / 'templates'],
    'APP_DIRS': True,
    'OPTIONS': {
        'context_processors': [
            'django.template.context_processors.request',
            'django.contrib.auth.context_processors.auth',
            'django.contrib.messages.context_processors.messages',
        ],
    },
}]

# Punto de entrada Channels para trafico HTTP y WebSocket.
ASGI_APPLICATION = 'config.asgi.application'

# Migracion planificada a SQL Server:
# 1. El DBA debe crear una base vacia para NEXHUS y un usuario con permisos de
#    lectura/escritura sobre las tablas que Django creara con `manage.py migrate`.
# 2. El servidor Windows donde corra Django/Celery necesita el driver ODBC de
#    SQL Server instalado. Recomendado: "ODBC Driver 18 for SQL Server".
# 3. Antes de cambiar este bloque, instalar en el entorno Python:
#    `mssql-django` y `pyodbc`, y agregarlos a requirements.txt.
# 4. Las variables esperadas para produccion seran:
#    DB_ENGINE=mssql
#    DB_NAME=<base_sql_server>
#    DB_USER=<usuario_sql_server>
#    DB_PASSWORD=<password_sql_server>
#    DB_HOST=<host_o_ip_sql_server>
#    DB_PORT=1433
#    DB_DRIVER=ODBC Driver 18 for SQL Server
# 5. Despues de configurar SQL Server, ejecutar `manage.py migrate` para crear
#    estructura y migrar los datos actuales desde db.sqlite3 con un proceso
#    controlado. No borrar db.sqlite3 hasta validar conteos de pallets, HUs y logs.
# SQLite queda como valor local por defecto mientras se completa la migracion.
# En OneDrive/Windows evitamos WAL porque sus archivos `-wal` y `-shm` pueden
# quedar bloqueados por sincronizacion o procesos duplicados y provocar errores
# intermitentes como "attempt to write a readonly database".
SQLITE_TIMEOUT_SECONDS = config('SQLITE_TIMEOUT_SECONDS', default=30, cast=float)
SQLITE_JOURNAL_MODE = config('SQLITE_JOURNAL_MODE', default='DELETE')
QUEUE_STATS_CACHE_SECONDS = config('QUEUE_STATS_CACHE_SECONDS', default=0.5, cast=float)
LOCAL_DATA_DIR = Path(
    config(
        'NEXHUS_DATA_DIR',
        default=str(Path(os.environ.get('LOCALAPPDATA', BASE_DIR)) / 'NEXHUS'),
    )
)
SQLITE_DB_PATH = Path(config('SQLITE_DB_PATH', default=str(LOCAL_DATA_DIR / 'db.sqlite3')))
SQLITE_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.sqlite3',
        'NAME': SQLITE_DB_PATH,
        'OPTIONS': {
            'timeout': SQLITE_TIMEOUT_SECONDS,
            'init_command': '; '.join([
                f'PRAGMA journal_mode={SQLITE_JOURNAL_MODE}',
                f'PRAGMA busy_timeout={int(SQLITE_TIMEOUT_SECONDS * 1000)}',
            ]),
        },
    }
}

REDIS_URL = config('REDIS_URL', default='redis://localhost:6379/0')

# Panel de soporte para controlar procesos locales desde Diagnostico.
# Mantener deshabilitado si no existe una clave privada en .env.
NEXHUS_SERVICE_CONTROL_PASSWORD = config('NEXHUS_SERVICE_CONTROL_PASSWORD', default='')
NEXHUS_SERVICE_CONTROL_ENABLED = config(
    'NEXHUS_SERVICE_CONTROL_ENABLED',
    default=bool(NEXHUS_SERVICE_CONTROL_PASSWORD),
    cast=bool,
)
NEXHUS_REDIS_EXE = config(
    'NEXHUS_REDIS_EXE',
    default=r'C:\Users\LOPEZHEJ\Downloads\Redis-x64-5.0.14.1\redis-server.exe',
)
NEXHUS_DAPHNE_BIND = config('NEXHUS_DAPHNE_BIND', default='0.0.0.0')
NEXHUS_DAPHNE_PORT = config('NEXHUS_DAPHNE_PORT', default=9821, cast=int)
NEXHUS_DAPHNE_SELF_STOP_ENABLED = config(
    'NEXHUS_DAPHNE_SELF_STOP_ENABLED',
    default=False,
    cast=bool,
)

# Channels y Celery comparten Redis para mantener cola y eventos UI sincronizados.
CHANNEL_LAYERS = {
    'default': {
        'BACKEND': 'channels_redis.core.RedisChannelLayer',
        'CONFIG': {
            'hosts': [REDIS_URL],
        },
    },
}

# Celery debe usar un solo worker para evitar sesiones SAP GUI concurrentes.
CELERY_BROKER_URL = REDIS_URL
CELERY_RESULT_BACKEND = REDIS_URL
CELERY_ACCEPT_CONTENT = ['json']
CELERY_TASK_SERIALIZER = 'json'
CELERY_WORKER_CONCURRENCY = 1
CELERY_WORKER_PREFETCH_MULTIPLIER = 1

# Constantes SAP leidas desde .env con valores por defecto para el entorno local.
SAP_SISTEMA = config('SAP_SISTEMA', default='LUP')
SAP_TX_MOVEINBHU = config('SAP_TX_MOVEINBHU', default='/nZMOVEINBHU')
SAP_TX_TIJSEP = config('SAP_TX_TIJSEP', default='/nZMMTIJSEP')
SAP_CONNECTION_NAME = config('SAP_CONNECTION_NAME', default='LUP Production [Public]')
SAP_LOGON_EXE = config(
    'SAP_LOGON_EXE',
    default=r'C:\Program Files (x86)\SAP\FrontEnd\SAPgui\saplogon.exe',
)
SAP_LOGIN_USER = config('SAP_LOGIN_USER', default='')
SAP_LOGIN_PASSWORD = config('SAP_LOGIN_PASSWORD', default='')
SAP_LOGIN_LANGUAGE = config('SAP_LOGIN_LANGUAGE', default='EN')
SAP_STRICT_LOGIN_USER = config('SAP_STRICT_LOGIN_USER', default=True, cast=bool)
SAP_PROBE_BEFORE_WORK = config('SAP_PROBE_BEFORE_WORK', default=True, cast=bool)
SAP_CLOSE_WHEN_QUEUE_IDLE = config('SAP_CLOSE_WHEN_QUEUE_IDLE', default=True, cast=bool)
SAP_STARTUP_TIMEOUT_SECONDS = config('SAP_STARTUP_TIMEOUT_SECONDS', default=30, cast=float)
SAP_STARTUP_POLL_SECONDS = config('SAP_STARTUP_POLL_SECONDS', default=1, cast=float)
# Tras enviar /nTRANSACCION, SAP se espera de forma activa hasta que la
# transaccion objetivo este lista. Este retardo evita sondear antes de que GUI
# procese el comando sin imponer las antiguas pausas fijas de 0.7 segundos.
SAP_TRANSACTION_MIN_WAIT_SECONDS = config(
    'SAP_TRANSACTION_MIN_WAIT_SECONDS', default=0.05, cast=float,
)
SAP_TRANSACTION_READY_TIMEOUT_SECONDS = config(
    'SAP_TRANSACTION_READY_TIMEOUT_SECONDS', default=15, cast=float,
)
SAP_COM_CONNECT_TIMEOUT_SECONDS = config('SAP_COM_CONNECT_TIMEOUT_SECONDS', default=30, cast=float)
SAP_COM_PHASE_TIMEOUT_SECONDS = config('SAP_COM_PHASE_TIMEOUT_SECONDS', default=60, cast=float)
SAP_COM_ZE16_TIMEOUT_SECONDS = config('SAP_COM_ZE16_TIMEOUT_SECONDS', default=180, cast=float)
SAP_COM_CALL_WARN_SECONDS = config('SAP_COM_CALL_WARN_SECONDS', default=5, cast=float)
SAP_COM_MAX_CONSECUTIVE_TIMEOUTS = config('SAP_COM_MAX_CONSECUTIVE_TIMEOUTS', default=2, cast=int)
SAP_COM_COOLDOWN_SECONDS = config('SAP_COM_COOLDOWN_SECONDS', default=30, cast=float)
SAP_SESSION_RESPONSIVE_SECONDS = config('SAP_SESSION_RESPONSIVE_SECONDS', default=2, cast=float)
SAP_SESSION_TRACE_ENABLED = config('SAP_SESSION_TRACE_ENABLED', default=True, cast=bool)

STATIC_URL = '/static/'
STATICFILES_DIRS = [BASE_DIR / 'static']
STATIC_ROOT = BASE_DIR / 'staticfiles'

# Endurecimiento de produccion activo por defecto cuando DEBUG=False.
SECURE_SSL_REDIRECT = config('SECURE_SSL_REDIRECT', default=not DEBUG, cast=bool)
SESSION_COOKIE_SECURE = config('SESSION_COOKIE_SECURE', default=not DEBUG, cast=bool)
CSRF_COOKIE_SECURE = config('CSRF_COOKIE_SECURE', default=not DEBUG, cast=bool)
SECURE_HSTS_SECONDS = config('SECURE_HSTS_SECONDS', default=0 if DEBUG else 31536000, cast=int)
SECURE_HSTS_INCLUDE_SUBDOMAINS = config(
    'SECURE_HSTS_INCLUDE_SUBDOMAINS',
    default=not DEBUG,
    cast=bool,
)
CSRF_TRUSTED_ORIGINS = _env_list(
    'CSRF_TRUSTED_ORIGINS',
    'http://localhost:9821,http://127.0.0.1:9821,http://10.110.202.84:9821',
)
SECURE_HSTS_PRELOAD = config('SECURE_HSTS_PRELOAD', default=not DEBUG, cast=bool)

DEFAULT_AUTO_FIELD = 'django.db.models.BigAutoField'

# Logging compartido para vistas HTTP, Channels y tareas Celery.
LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'hu_format': {
            'format': '%(asctime)s [%(levelname)s] %(name)s - %(message)s',
            'datefmt': '%H:%M:%S',
        },
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'formatter': 'hu_format',
        },
        'file': {
            'class': 'logging.handlers.TimedRotatingFileHandler',
            'filename': BASE_DIR / 'logs' / 'hu_queue.log',
            'when': 'midnight',
            'formatter': 'hu_format',
            'encoding': 'utf-8',
        },
    },
    'root': {
        'handlers': ['console', 'file'],
        'level': 'INFO',
    },
}
