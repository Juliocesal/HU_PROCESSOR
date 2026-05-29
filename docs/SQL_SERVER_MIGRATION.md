# Migracion a SQL Server

Este documento resume lo que necesita el DBA o desarrollador encargado de
migrar NEXHUS desde SQLite a SQL Server.

## Estado actual

El proyecto actualmente usa SQLite como base local:

```python
DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": BASE_DIR / "db.sqlite3",
    }
}
```

Archivo activo:

```text
db.sqlite3
```

Archivos como `db.sqlite3-wal` y `db.sqlite3-shm` son temporales de SQLite y no
se migran como bases independientes.

## Objetivo recomendado

Para produccion empresarial, SQL Server debe quedar como base principal del
sistema. SQLite debe conservarse solo como respaldo historico temporal hasta
validar la migracion.

Si SQL Server se usara primero solo para consulta, al menos deben replicarse:

```text
queue_app_pallet
queue_app_huitem
queue_app_scanlog
```

## Requisitos del servidor

En el servidor donde corre Django/Celery:

1. Python virtualenv del proyecto activo.
2. Driver ODBC de SQL Server instalado.
   Recomendado: `ODBC Driver 18 for SQL Server`.
3. Acceso de red al servidor SQL Server por el puerto definido por Infra.
   Usualmente: `1433`.
4. Redis se mantiene separado. SQL Server no reemplaza Redis para Celery ni
   Channels.

## Dependencias Python necesarias

Agregar a `requirements.txt`:

```text
mssql-django
pyodbc
```

Instalacion:

```powershell
venv\Scripts\pip.exe install mssql-django pyodbc
```

## Variables de conexion esperadas

Agregar al `.env` de produccion:

```env
DB_ENGINE=mssql
DB_NAME=NEXHUS
DB_USER=nexhus_user
DB_PASSWORD=CAMBIAR_PASSWORD
DB_HOST=SERVIDOR_SQL_O_IP
DB_PORT=1433
DB_DRIVER=ODBC Driver 18 for SQL Server
DB_TRUST_CERT=yes
```

Si SQL Server usa autenticacion integrada de Windows, el bloque de conexion
debe revisarse con Infra/DBA antes de desplegar.

## Bloque Django recomendado

El bloque `DATABASES` de `config/settings.py` debe parametrizarse para permitir
SQLite local y SQL Server en produccion:

```python
DATABASES = {
    "default": {
        "ENGINE": config("DB_ENGINE", default="django.db.backends.sqlite3"),
        "NAME": config("DB_NAME", default=BASE_DIR / "db.sqlite3"),
        "USER": config("DB_USER", default=""),
        "PASSWORD": config("DB_PASSWORD", default=""),
        "HOST": config("DB_HOST", default=""),
        "PORT": config("DB_PORT", default=""),
        "OPTIONS": {
            "driver": config("DB_DRIVER", default="ODBC Driver 18 for SQL Server"),
            "TrustServerCertificate": config("DB_TRUST_CERT", default="yes"),
        },
    }
}
```

## Tablas principales del negocio

### queue_app_pallet

Entidad padre del procesamiento. Agrupa HUs por pallet.

Campos importantes:

| Campo | Uso |
| --- | --- |
| `id` | Identificador interno del pallet. En UI se muestra como P01, P02, etc. |
| `created_at` | Hora de creacion del pallet. |
| `processing_started_at` | Inicio real del ciclo F1 a PDF del pallet. |
| `processing_finished_at` | Fin del ciclo del pallet, tanto en exito como en error. |
| `f2_done_at` | Hora en que termino F2/Separazione para el pallet. |
| `receipt_done_at` | Hora en que termino generacion/impresion PDF. |
| `pdf_status` | Resultado PDF: `ok` o `error`. |
| `pdf_msg` | Mensaje del resultado PDF. |
| `pdf_ms` | Tiempo del tramo PDF en milisegundos. |
| `status` | Estado del pallet: `active`, `ready`, `done`. |
| `origin_code` | Origen detectado: THA, BRA/ATL, ITA, FHR, CNA, etc. |

Consultas frecuentes:

```sql
WHERE id = ?
WHERE origin_code = ?
WHERE status = ?
WHERE created_at BETWEEN ? AND ?
WHERE processing_finished_at BETWEEN ? AND ?
WHERE receipt_done_at BETWEEN ? AND ?
WHERE pdf_status = ?
```

### queue_app_huitem

Tabla principal para trazabilidad por HU.

Campos importantes:

| Campo | Uso |
| --- | --- |
| `id` | Identificador interno del HUItem. |
| `hu_code` | HU escaneado por el operador. |
| `pallet_id` | Relacion con `queue_app_pallet.id`. |
| `origin_code` | Origen detectado del HU. |
| `added_at` | Hora en que el HU fue escaneado/capturado. |
| `status` | Estado actual/final del HU. |
| `phase1_msg` | Mensaje de F1 / Order Acknowledge. |
| `phase2_msg` | Mensaje de F2 / Separazione. |
| `phase2_ms` | Tiempo de F2 en milisegundos. |
| `error_msg` | Problema presentado si hubo error. |
| `processing_started_at` | Hora en que Celery tomo el HU para SAP. |
| `processing_ms` | Tiempo total de procesamiento del HU. |
| `processed_at` | Hora final del procesamiento del HU. |
| `f1_done_at` | Hora en que termino F1. |
| `receipt_done_at` | Hora de recibo/PDF cuando aplica al HU. |
| `pdf_status` | Estado PDF copiado al HU. |
| `pdf_msg` | Mensaje PDF copiado al HU. |
| `pdf_ms` | Tiempo PDF copiado al HU. |

Estados posibles:

```text
pending
processing
ok
duplicate
error
hu_not_found
```

Consultas frecuentes:

```sql
WHERE hu_code = ?
WHERE pallet_id = ?
WHERE origin_code = ?
WHERE status = ?
WHERE added_at BETWEEN ? AND ?
WHERE processing_started_at BETWEEN ? AND ?
WHERE processed_at BETWEEN ? AND ?
WHERE pdf_status = ?
WHERE error_msg <> ''
```

### queue_app_scanlog

Auditoria de intentos de escaneo. Sirve para revisar duplicados, errores de
captura o eventos que no terminaron como HU procesable.

Campos importantes:

| Campo | Uso |
| --- | --- |
| `id` | Identificador del log. |
| `hu_code` | HU escaneado. |
| `scanned_at` | Hora del intento de escaneo. |
| `result` | Resultado del intento: queued, duplicate, error, etc. |
| `message` | Detalle del resultado. |

Consultas frecuentes:

```sql
WHERE hu_code = ?
WHERE result = ?
WHERE scanned_at BETWEEN ? AND ?
```

## Tablas Django que tambien deben existir

Si SQL Server sera la base principal, Django necesita tambien sus tablas de
sistema:

```text
auth_group
auth_group_permissions
auth_permission
auth_user
auth_user_groups
auth_user_user_permissions
django_admin_log
django_content_type
django_migrations
django_session
```

Estas tablas las crea Django con:

```powershell
venv\Scripts\python.exe manage.py migrate
```

No conviene crearlas manualmente salvo que el DBA tenga un motivo especifico.

## Orden recomendado de migracion

1. Respaldar `db.sqlite3`.
2. Respaldar carpeta `queue_app/migrations/`.
3. Instalar driver ODBC en el servidor.
4. Instalar `mssql-django` y `pyodbc`.
5. Crear base SQL Server vacia.
6. Crear usuario SQL Server con permisos sobre esa base.
7. Parametrizar `DATABASES` con variables `.env`.
8. Ejecutar `manage.py migrate` apuntando a SQL Server.
9. Migrar datos desde SQLite.
10. Validar conteos y relaciones.
11. Probar flujo completo del sistema.
12. Mantener `db.sqlite3` como respaldo hasta cierre de validacion.

## Migracion de datos

### Opcion 1: migracion Django completa

Exportar desde SQLite:

```powershell
venv\Scripts\python.exe manage.py dumpdata --exclude auth.permission --exclude contenttypes --indent 2 > sqlite_data.json
```

Cargar en SQL Server despues de cambiar `DATABASES`:

```powershell
venv\Scripts\python.exe manage.py loaddata sqlite_data.json
```

Esta opcion es rapida, pero puede arrastrar datos de desarrollo.

### Opcion 2: migracion controlada recomendada

Migrar solo tablas de negocio:

```text
queue_app_pallet
queue_app_huitem
queue_app_scanlog
```

Luego crear usuarios reales directamente en produccion o migrar solo usuarios
autorizados.

Esta opcion es mas segura para produccion.

## Validaciones despues de migrar

Validar estructura:

```powershell
venv\Scripts\python.exe manage.py check
venv\Scripts\python.exe manage.py showmigrations
```

Validar pruebas:

```powershell
venv\Scripts\python.exe manage.py test queue_app
```

Validar conteos:

```sql
SELECT COUNT(*) FROM queue_app_pallet;
SELECT COUNT(*) FROM queue_app_huitem;
SELECT COUNT(*) FROM queue_app_scanlog;
```

Comparar contra SQLite antes de apagarlo.

## Indices recomendados para reporting

Ademas de las llaves creadas por Django, SQL Server deberia considerar indices
para consultas frecuentes:

```sql
-- HU exacto.
CREATE INDEX IX_huitem_hu_code ON queue_app_huitem (hu_code);

-- Reportes por pallet.
CREATE INDEX IX_huitem_pallet_id ON queue_app_huitem (pallet_id);

-- Reportes por estado/origen/fecha.
CREATE INDEX IX_huitem_status_origin_processed
ON queue_app_huitem (status, origin_code, processed_at);

-- Reportes por PDF.
CREATE INDEX IX_huitem_pdf_status ON queue_app_huitem (pdf_status);

-- Pallets por origen/estado/fecha.
CREATE INDEX IX_pallet_origin_status_created
ON queue_app_pallet (origin_code, status, created_at);

-- Auditoria de escaneo.
CREATE INDEX IX_scanlog_hu_scanned ON queue_app_scanlog (hu_code, scanned_at);
```

Antes de crear indices en produccion, revisar volumen real de datos y patrones
de consulta.

## Flujos que deben probarse contra SQL Server

1. Escanear HU nuevo.
2. Escanear HU duplicado.
3. Crear pallet manualmente.
4. Procesar F1 correctamente.
5. Procesar F2 correctamente.
6. Forzar error F1.
7. Forzar error F2.
8. Generar PDF correcto.
9. Forzar error PDF/impresora.
10. Reprocesar todos los HUs.
11. Reprocesar solo HUs con error.
12. Borrar HU fallido y recalcular pallet.
13. Limpiar cola.
14. Validar WebSocket/UI despues de cada evento.
15. Validar Celery con concurrencia 1.

## Notas operativas

- Redis sigue siendo obligatorio para Celery y Channels.
- Celery debe mantenerse con `CELERY_WORKER_CONCURRENCY=1` para evitar sesiones
  SAP GUI concurrentes.
- El servidor que ejecute Celery debe tener SAP GUI disponible.
- No borrar `db.sqlite3` hasta validar SQL Server con pruebas reales.
- Si se mantiene SQLite y SQL Server al mismo tiempo, definir claramente cual es
  la fuente de verdad. Lo recomendado en produccion es que sea SQL Server.
