from .settings import *

DEBUG = True
DATABASES['default']['NAME'] = BASE_DIR / 'db_dev.sqlite3'
