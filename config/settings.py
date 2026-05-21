from pathlib import Path
from decouple import config

BASE_DIR = Path(__file__).resolve().parent.parent

SECRET_KEY = config('SECRET_KEY')
DEBUG = config('DEBUG', default=False, cast=bool)
ALLOWED_HOSTS = [
    host.strip()
    for host in config('ALLOWED_HOSTS', default='localhost,127.0.0.1').split(',')
    if host.strip()
]

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

# SQLite es el valor local por defecto. Sobrescribe este bloque en produccion.
DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.sqlite3',
        'NAME': BASE_DIR / 'db.sqlite3',
    }
}

REDIS_URL = config('REDIS_URL', default='redis://localhost:6379/0')

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
