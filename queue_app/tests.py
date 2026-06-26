from datetime import timedelta
import logging
import time
from types import SimpleNamespace
from unittest.mock import MagicMock, patch


from asgiref.sync import async_to_sync
from django.db import DatabaseError
from django.test import TestCase, override_settings
from django.utils import timezone


from core.sap_client import (
    SAPClient,
    SAPBusySessionError,
    SAPCOMBlockedError,
    SAPCOMFatalBlockedError,
    SAP_TRANSACTION_MIN_WAIT_SECONDS,
)
from core.ze16_client import ZE16Client
from queue_app import service_control
from queue_app.consumers import QueueConsumer
from queue_app.models import HUItem, Pallet
from queue_app.tasks import _get_ze16_receipts_with_retries, process_queue_task
from queue_app.utils import emit_queue_done, emit_queue_status, pallets_ready_for_pdf_queryset
from queue_app.views import (
    CELERY_NO_WORKER_MESSAGE,
    QUEUE_CONTINUOUS_IDLE_TIMEOUT_SECONDS,
    _check_celery_status,
)


class DocumentationPageTests(TestCase):
    def test_documentation_page_renders(self):
        response = self.client.get('/documentation/')

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'Documentación del Proyecto')




class PalletTimingTests(TestCase):
    def test_processing_time_uses_f1_start_not_pallet_creation(self):
        now = timezone.now()
        pallet = Pallet.objects.create(
            created_at=now - timedelta(hours=2),
            processing_started_at=now - timedelta(seconds=65),
            processing_finished_at=now,
            receipt_done_at=now,
        )


        self.assertEqual(pallet.processing_time_display, '1min 5s')


    def test_processing_time_stops_on_error_without_receipt(self):
        now = timezone.now()
        pallet = Pallet.objects.create(
            processing_started_at=now - timedelta(seconds=65),
            processing_finished_at=now,
            pdf_status=Pallet.PDF_STATUS_ERROR,
            pdf_msg='PDF omitido: HU con error',
        )


        self.assertEqual(pallet.processing_time_display, '1min 5s')


    def test_processing_time_is_empty_before_processing_starts(self):
        pallet = Pallet.objects.create()


        self.assertEqual(pallet.processing_time_display, '')


class SystemStatusTests(TestCase):
    def test_system_status_returns_diagnostic_contract(self):
        with (
            patch('queue_app.views._check_redis_status', return_value={'ok': True, 'message': 'Redis responde.'}),
            patch('queue_app.views._check_celery_status', return_value={'ok': True, 'message': '1 worker', 'workers': ['worker1']}),
            patch('queue_app.views._check_database_status', return_value={'ok': True, 'message': 'DB responde.'}),
            patch('queue_app.views._check_sap_status_for_diagnostics', return_value={'ok': True, 'connected': True, 'user': 'BOT1', 'message': 'Sesion SAP activa.'}),
            patch('queue_app.views._queue_diagnostic_snapshot', return_value={'is_locked': False, 'pending_hus': 0}),
        ):
            response = self.client.get('/api/system-status/')

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload['ok'])
        self.assertTrue(payload['django']['ok'])
        self.assertTrue(payload['redis']['ok'])
        self.assertTrue(payload['celery']['ok'])
        self.assertTrue(payload['database']['ok'])
        self.assertTrue(payload['sap']['connected'])
        self.assertIn('queue', payload)


    def test_system_status_skips_heavy_queue_snapshot_when_redis_is_down(self):
        redis_status = {
            'ok': False,
            'message': 'Redis no responde.',
            'error': 'timeout',
        }
        degraded_queue = {
            'is_locked': False,
            'pending_hus': 0,
            'runtime_unavailable': True,
        }
        with (
            patch('queue_app.views._check_redis_status', return_value=redis_status),
            patch('queue_app.views._queue_diagnostic_snapshot') as heavy_snapshot,
            patch('queue_app.views._queue_diagnostic_snapshot_without_redis', return_value=degraded_queue),
            patch('queue_app.views._check_celery_status', return_value={'ok': False, 'workers': []}),
            patch('queue_app.views._check_database_status', return_value={'ok': True, 'message': 'DB responde.'}),
            patch('queue_app.views._check_sap_status_for_diagnostics', return_value={'ok': False, 'connected': False}),
        ):
            response = self.client.get('/api/system-status/')

        self.assertEqual(response.status_code, 200)
        heavy_snapshot.assert_not_called()
        payload = response.json()
        self.assertFalse(payload['ok'])
        self.assertTrue(payload['queue']['runtime_unavailable'])


    def test_celery_without_workers_is_reported_as_error(self):
        inspector = SimpleNamespace(ping=lambda: {})
        current_app = SimpleNamespace(
            control=SimpleNamespace(inspect=lambda timeout=1: inspector)
        )

        with patch('celery.current_app', current_app):
            status = _check_celery_status(redis_ok=True)

        self.assertFalse(status['ok'])
        self.assertEqual(status['message'], CELERY_NO_WORKER_MESSAGE)
        self.assertEqual(status['workers'], [])


    def test_celery_without_ping_but_active_queue_is_reported_as_busy_warning(self):
        inspector = SimpleNamespace(ping=lambda: {})
        current_app = SimpleNamespace(
            control=SimpleNamespace(inspect=lambda timeout=1: inspector)
        )
        queue_status = {
            'is_locked': True,
            'worker_heartbeat_alive': True,
            'worker_stale': False,
            'processing_hus': 0,
            'processing_pallet_ids': [],
            'idle_remaining_seconds': 120,
            'last_operational_status': {'mode': 'waiting'},
        }

        with patch('celery.current_app', current_app):
            status = _check_celery_status(redis_ok=True, queue_status=queue_status)

        self.assertTrue(status['ok'])
        self.assertEqual(status['level'], 'warn')
        self.assertFalse(status['control_ping_ok'])
        self.assertIn('cola esta activa', status['message'])


    def test_celery_without_ping_and_stale_worker_is_reported_as_error(self):
        inspector = SimpleNamespace(ping=lambda: {})
        current_app = SimpleNamespace(
            control=SimpleNamespace(inspect=lambda timeout=1: inspector)
        )
        queue_status = {
            'is_locked': True,
            'worker_heartbeat_alive': False,
            'worker_stale': True,
            'processing_hus': 0,
            'processing_pallet_ids': [],
            'idle_remaining_seconds': 120,
            'last_operational_status': {'mode': 'waiting'},
        }

        with patch('celery.current_app', current_app):
            status = _check_celery_status(redis_ok=True, queue_status=queue_status)

        self.assertFalse(status['ok'])
        self.assertEqual(status['level'], 'error')
        self.assertEqual(status['message'], CELERY_NO_WORKER_MESSAGE)


class ServiceControlTests(TestCase):
    @override_settings(
        NEXHUS_SERVICE_CONTROL_ENABLED=False,
        NEXHUS_SERVICE_CONTROL_PASSWORD='',
    )
    def test_service_control_status_requires_configuration(self):
        response = self.client.get('/api/service-control/status/')

        self.assertEqual(response.status_code, 403)
        payload = response.json()
        self.assertFalse(payload['ok'])
        self.assertFalse(payload['enabled'])


    @override_settings(
        NEXHUS_SERVICE_CONTROL_ENABLED=True,
        NEXHUS_SERVICE_CONTROL_PASSWORD='secret',
    )
    def test_service_control_login_accepts_configured_password(self):
        with patch('queue_app.views.get_services_status', return_value={}):
            response = self.client.post(
                '/api/service-control/login/',
                data='{"password": "secret"}',
                content_type='application/json',
            )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload['ok'])
        self.assertTrue(payload['authenticated'])


    @override_settings(
        NEXHUS_SERVICE_CONTROL_ENABLED=True,
        NEXHUS_SERVICE_CONTROL_PASSWORD='secret',
    )
    def test_service_control_action_requires_login(self):
        response = self.client.post('/api/service-control/redis/start/')

        self.assertEqual(response.status_code, 401)
        self.assertFalse(response.json()['authenticated'])


    def test_redis_running_exposes_restart_only(self):
        fake_client = SimpleNamespace(info=lambda section=None: {'redis_version': '5.0.14.1'})

        with patch('queue_app.service_control._redis_client', return_value=fake_client):
            status = service_control._redis_status()

        self.assertEqual(status['state'], 'running')
        self.assertFalse(status['actions']['start'])
        self.assertFalse(status['actions']['stop'])
        self.assertTrue(status['actions']['restart'])


    def test_daphne_status_does_not_expose_control_actions(self):
        with patch('queue_app.service_control._tcp_port_open', return_value=True):
            status = service_control._daphne_status()

        self.assertEqual(status['state'], 'running')
        self.assertFalse(any(status['actions'].values()))


class SAPTransactionReadinessTests(TestCase):
    def test_transaction_wait_confirms_ready_field_after_minimum_delay(self):
        client = SAPClient()
        client._session = SimpleNamespace(
            Busy=False,
            Info=SimpleNamespace(Transaction='ZMOVEINBHU'),
        )
        client._find = lambda element_id: object() if element_id == 'target-field' else None

        with patch('core.sap_client.time.sleep') as sleep:
            field_ready, readiness, _elapsed_ms, busy_seen = client._wait_for_transaction_ready(
                '/nZMOVEINBHU',
                ready_field='target-field',
            )

        self.assertTrue(field_ready)
        self.assertEqual(readiness, 'field')
        self.assertFalse(busy_seen)
        sleep.assert_called_once_with(SAP_TRANSACTION_MIN_WAIT_SECONDS)


    def test_transaction_wait_returns_tree_when_target_field_needs_navigation(self):
        client = SAPClient()
        client._session = SimpleNamespace(
            Busy=False,
            Info=SimpleNamespace(Transaction='ZMMTIJSEP'),
        )
        client._find = lambda element_id: object() if 'cntlIMAGE_CONTAINER' in element_id else None

        with patch('core.sap_client.time.sleep'):
            field_ready, readiness, _elapsed_ms, _busy_seen = client._wait_for_transaction_ready(
                '/nZMMTIJSEP',
                ready_field='target-field',
            )

        self.assertFalse(field_ready)
        self.assertEqual(readiness, 'tree')


class SAPWorkerSessionTests(TestCase):
    def test_session_is_reused_within_pallet_and_checked_before_next_pallet(self):
        from queue_app.services.hu_processing_service import SAPWorkerSession

        sap_client = MagicMock()
        sap_client.is_session_healthy.return_value = True

        with patch('core.sap_client.SAPClient', return_value=sap_client):
            session = SAPWorkerSession()
            try:
                first_client, first_action = session.acquire(pallet_id=1)
                second_client, second_action = session.acquire(pallet_id=1)
                third_client, third_action = session.acquire(pallet_id=2)
            finally:
                session.close()

        self.assertIs(first_client, sap_client)
        self.assertIs(second_client, sap_client)
        self.assertIs(third_client, sap_client)
        self.assertEqual((first_action, second_action, third_action), (
            'connected',
            'reused',
            'health_checked',
        ))
        sap_client.connect.assert_called_once_with(initialize_com=False)
        sap_client.is_session_healthy.assert_called_once()


    def test_first_connect_can_force_new_login_without_session_scan(self):
        from queue_app.services.hu_processing_service import SAPWorkerSession

        sap_client = MagicMock()

        with patch('core.sap_client.SAPClient', return_value=sap_client):
            session = SAPWorkerSession(force_new_login_on_first_connect=True)
            try:
                session.acquire(pallet_id=1)
            finally:
                session.close()

        sap_client.connect.assert_called_once_with(
            initialize_com=False,
            force_new_login=True,
        )

    def test_worker_can_close_only_cached_sap_session(self):
        from queue_app.services.hu_processing_service import SAPWorkerSession

        sap_client = MagicMock()
        sap_client.close_current_session.return_value = 1

        with patch('core.sap_client.SAPClient', return_value=sap_client):
            session = SAPWorkerSession()
            try:
                session.acquire(pallet_id=1)
                closed = session.close_current_session(timeout=1)
            finally:
                session.close()

        self.assertEqual(closed, 1)
        sap_client.close_current_session.assert_called_once()


    @override_settings(SAP_COM_COOLDOWN_SECONDS=0)
    def test_com_operation_timeout_raises_controlled_error(self):
        from queue_app.services.hu_processing_service import SAPWorkerSession

        sap_client = MagicMock()

        with patch('core.sap_client.SAPClient', return_value=sap_client):
            session = SAPWorkerSession()
            try:
                session.prepare(pallet_id=1)
                started_at = time.perf_counter()
                with self.assertRaises(SAPCOMBlockedError):
                    session.call(
                        1,
                        'sap.test_blocked',
                        lambda _client: time.sleep(0.05),
                        timeout=0.01,
                    )
            finally:
                session.close()

        self.assertLess(time.perf_counter() - started_at, 0.5)
        self.assertIsNone(session._executor)


    @override_settings(SAP_COM_COOLDOWN_SECONDS=0, SAP_COM_MAX_CONSECUTIVE_TIMEOUTS=1)
    def test_com_operation_timeout_can_abort_current_run(self):
        from queue_app.services.hu_processing_service import SAPWorkerSession

        sap_client = MagicMock()

        with patch('core.sap_client.SAPClient', return_value=sap_client):
            session = SAPWorkerSession()
            try:
                session.prepare(pallet_id=1)
                with self.assertRaises(SAPCOMFatalBlockedError):
                    session.call(
                        1,
                        'sap.test_fatal_blocked',
                        lambda _client: time.sleep(0.05),
                        timeout=0.01,
                    )
            finally:
                session.close()

        metrics = session.metrics()
        self.assertEqual(metrics['consecutive_timeouts'], 1)
        self.assertEqual(metrics['aborted_runs'], 1)
        self.assertIsNone(session._executor)


class QueueWorkerCloseTests(TestCase):
    def test_close_sap_after_queue_idle_uses_worker_session_without_global_scan(self):
        from queue_app.services.queue_worker_service import close_sap_after_queue_idle

        sap_worker_session = MagicMock()
        sap_worker_session.close_current_session.return_value = 1

        with patch('core.sap_client.SAPClient.close_sessions') as close_sessions:
            close_sap_after_queue_idle(
                logger=logging.getLogger(__name__),
                sap_worker_session=sap_worker_session,
            )

        sap_worker_session.close_current_session.assert_called_once()
        close_sessions.assert_not_called()


class DiagnosticsSapStatusTests(TestCase):
    def test_sap_status_skips_live_probe_while_queue_is_locked(self):
        from queue_app.services import diagnostics_service

        old_future = diagnostics_service._sap_status_future
        old_cache = diagnostics_service._sap_status_cache
        old_circuit_until = diagnostics_service._sap_status_circuit_until
        diagnostics_service._sap_status_future = None
        diagnostics_service._sap_status_cache = None
        diagnostics_service._sap_status_circuit_until = 0
        try:
            with (
                patch('queue_app.services.diagnostics_service.is_queue_locked', return_value=True),
                patch.object(diagnostics_service._sap_status_executor, 'submit') as submit,
            ):
                status = diagnostics_service.get_fast_sap_status()
        finally:
            diagnostics_service._sap_status_future = old_future
            diagnostics_service._sap_status_cache = old_cache
            diagnostics_service._sap_status_circuit_until = old_circuit_until

        self.assertEqual(status['status'], 'stale')
        self.assertEqual(status['code'], 'SAP_WORKER_ACTIVE')
        self.assertEqual(status['reason'], 'queue_locked')
        self.assertFalse(status['connected'])
        submit.assert_not_called()

    def test_sap_status_reports_worker_connected_cache_while_queue_is_locked(self):
        from queue_app.services import diagnostics_service

        old_future = diagnostics_service._sap_status_future
        old_cache = diagnostics_service._sap_status_cache
        old_circuit_until = diagnostics_service._sap_status_circuit_until
        diagnostics_service._sap_status_future = None
        diagnostics_service._sap_status_cache = None
        diagnostics_service._sap_status_circuit_until = 0
        try:
            diagnostics_service.set_cached_sap_connected(user='LOPEZHJC')
            with (
                patch('queue_app.services.diagnostics_service.is_queue_locked', return_value=True),
                patch.object(diagnostics_service._sap_status_executor, 'submit') as submit,
            ):
                status = diagnostics_service.get_fast_sap_status()
        finally:
            diagnostics_service._sap_status_future = old_future
            diagnostics_service._sap_status_cache = old_cache
            diagnostics_service._sap_status_circuit_until = old_circuit_until

        self.assertEqual(status['status'], 'connected')
        self.assertTrue(status['connected'])
        self.assertEqual(status['user'], 'LOPEZHJC')
        submit.assert_not_called()


class ReceiptSAPReuseTests(TestCase):
    @override_settings(SAP_LOGIN_USER='LOPEZHJC')
    def test_ze16_pdf_reuses_worker_sap_client_without_reconnecting(self):
        from queue_app.services import receipt_service

        pallet = Pallet.objects.create(status=Pallet.STATUS_READY)
        HUItem.objects.create(
            hu_code='TH0000268197',
            pallet=pallet,
            status=HUItem.STATUS_OK,
        )
        sap_client = MagicMock()
        sap_client.session = object()
        sap_worker_session = MagicMock()

        def run_on_worker_client(pallet_id, operation, callback, timeout=None):
            self.assertEqual(pallet_id, pallet.pk)
            self.assertEqual(operation, 'sap.ze16.receipt_query')
            return callback(sap_client)

        sap_worker_session.call.side_effect = run_on_worker_client

        with (
            patch('core.sap_client.SAPClient') as sap_client_class,
            patch.object(ZE16Client, 'get_receipts_for_pallet', return_value={'TH0000268197': '72195396'}),
            patch('core.pdf_receipt.PalletReceiptPDF.generate', return_value='receipt.pdf'),
            patch('core.pdf_receipt.PalletReceiptPDF.print_pdf', return_value=True),
            patch('queue_app.utils.emit_queue_status'),
            patch('queue_app.utils.emit_receipt_done'),
        ):
            result = receipt_service.ze16_pdf_task_sync(
                pallet.pk,
                owner=None,
                sap_worker_session=sap_worker_session,
            )

        self.assertEqual(result['status'], 'ok')
        sap_client_class.assert_not_called()
        sap_worker_session.call.assert_called_once()


class QueueStatsCacheTests(TestCase):
    def tearDown(self):
        from queue_app.services.stats_service import invalidate_queue_stats_cache

        invalidate_queue_stats_cache()


    def test_stats_cache_is_invalidated_after_a_queue_mutation(self):
        from queue_app.services.stats_service import (
            calculate_queue_stats,
            invalidate_queue_stats_cache,
        )

        pallet = Pallet.objects.create(status=Pallet.STATUS_ACTIVE)
        item = HUItem.objects.create(hu_code='TH0000268197', pallet=pallet)
        invalidate_queue_stats_cache()
        self.assertEqual(calculate_queue_stats()['pending'], 1)

        item.status = HUItem.STATUS_OK
        item.save(update_fields=['status'])
        self.assertEqual(calculate_queue_stats()['pending'], 1)

        invalidate_queue_stats_cache()
        self.assertEqual(calculate_queue_stats()['pending'], 0)


class SAPSessionValidationTests(TestCase):
    def test_destination_storage_message_is_classified_as_already_separated(self):
        self.assertTrue(SAPClient._is_hu_already_in_destination_storage_location(
            '@5C@ HU Already In The Destination Storage Location'
        ))
        self.assertFalse(SAPClient._is_hu_already_in_destination_storage_location(
            '@5C@ HU is not allowed in the destination storage location'
        ))


    def test_phase2_material_types_message_returns_error_after_enter_button(self):
        class Field:
            Text = ''
            caretPosition = 0

        class Window:
            def sendVKey(self, _key):
                return None

        field = Field()
        enter_button = MagicMock()
        client = SAPClient()
        elements = {
            'wnd[0]/usr/txtGV_HU': field,
            'wnd[0]': Window(),
            'wnd[0]/usr/btnENTER': enter_button,
        }
        client._find = lambda element_id: elements.get(element_id)
        client._get_origin_timings = MagicMock(return_value=(0, 0, 0, 0))
        client._wait_idle = MagicMock(return_value=True)
        elements.update({
            'wnd[0]/usr/txtGV_MESSAGE01': SimpleNamespace(Text='Code C1017078829 HU'),
            'wnd[0]/usr/txtGV_MESSAGE02': SimpleNamespace(Text='contains different'),
            'wnd[0]/usr/txtGV_MESSAGE03': SimpleNamespace(Text='material types.'),
            'wnd[0]/usr/txtGV_MESSAGE04': SimpleNamespace(Text='Impossible to go'),
            'wnd[0]/usr/txtGV_MESSAGE05': SimpleNamespace(Text='on'),
        })
        client._get_sbar_text = MagicMock(return_value='')
        client._get_popup = MagicMock(return_value=None)

        result = client.process_hu_phase2('C1017078829', phase2_wait=0)

        self.assertEqual(result['status'], 'error')
        self.assertEqual(
            result['message'],
            'Code C1017078829 HU contains different material types. Impossible to go on',
        )
        self.assertEqual(result['phase2_ts'].year, 1)
        enter_button.press.assert_called_once()


    def test_phase2_message_field_returns_its_error_text(self):
        class Field:
            Text = ''
            caretPosition = 0

        class Window:
            def sendVKey(self, _key):
                return None

        field = Field()
        client = SAPClient()
        elements = {
            'wnd[0]/usr/txtGV_HU': field,
            'wnd[0]': Window(),
            'wnd[0]/usr/txtGV_MESSAGE01': SimpleNamespace(Text='HU bloqueada para separazione'),
        }
        client._find = lambda element_id: elements.get(element_id)
        client._get_origin_timings = MagicMock(return_value=(0, 0, 0, 0))
        client._wait_idle = MagicMock(return_value=True)
        client._get_sbar_text = MagicMock(return_value='')
        client._get_popup = MagicMock(return_value=None)

        result = client.process_hu_phase2('C1017078825', phase2_wait=0)

        self.assertEqual(result['status'], 'error')
        self.assertIn('HU bloqueada para separazione', result['message'])
        self.assertEqual(result['phase2_ts'].year, 1)


    def _make_sap_session(
        self,
        *,
        system='LUP',
        client='100',
        user='BOT1',
        transaction='SESSION_MANAGER',
        busy=False,
        modal=False,
        wnd=None,
    ):
        class Session:
            def __init__(self):
                self.Info = SimpleNamespace(
                    SystemName=system,
                    Client=client,
                    User=user,
                    Transaction=transaction,
                )
                self.ActiveWindow = SimpleNamespace(Text='SAP GUI for Windows 770')
                self.Busy = busy
                self._wnd = wnd or SimpleNamespace(Text='SAP GUI for Windows 770')
                self._modal = SimpleNamespace(Text='Modal SAP') if modal else None


            def findById(self, element_id):
                if element_id == 'wnd[0]':
                    return self._wnd
                if element_id == 'wnd[1]' and self._modal:
                    return self._modal
                if element_id == 'wnd[0]/sbar':
                    return SimpleNamespace(Text='')
                raise LookupError(element_id)


        return Session()


    def _make_sap_app(self, sessions, *, connection_name='LUP Production [Public]'):
        class Children:
            def __init__(self, values):
                self._values = values
                self.Count = len(values)


            def __call__(self, index):
                return self._values[index]


        class Connection:
            def __init__(self, values):
                self.Children = Children(values)
                self.Description = connection_name
                self.Name = connection_name


        class App:
            def __init__(self, values):
                self.Children = Children([Connection(values)])


        return App(sessions)


    def test_wait_for_sap_app_retries_until_scripting_engine_is_ready(self):
        class App:
            pass


        attempts = {'count': 0}


        def get_sap_app():
            attempts['count'] += 1
            if attempts['count'] == 1:
                raise RuntimeError('SAP GUI no listo')
            return App()


        with (
            patch.object(SAPClient, '_get_sap_app', side_effect=get_sap_app),
            patch('core.sap_client.time.sleep'),
        ):
            app = SAPClient._wait_for_sap_app(timeout=2)


        self.assertIsInstance(app, App)
        self.assertEqual(attempts['count'], 2)


    def test_session_is_alive_rejects_connection_reset_dialog(self):
        class Info:
            SystemName = 'LUP'
            User = 'TEST'


        class Window:
            Text = 'SAP GUI for Windows 770'


        class StatusBar:
            Text = 'WSAECONNRESET: Connection reset by peer'


        class Session:
            def __init__(self):
                self.Info = Info()
                self.ActiveWindow = Window()
                self.Busy = False


            def findById(self, element_id):
                if element_id == 'wnd[0]/sbar':
                    return StatusBar()
                return Window()


        self.assertFalse(SAPClient._session_is_alive(Session()))


    def test_find_ready_session_uses_expected_sap_user(self):
        other_user = self._make_sap_session(user='SUPERVISOR')
        expected_user = self._make_sap_session(user='BOT1')
        app = self._make_sap_app([other_user, expected_user])


        with patch.object(SAPClient, '_session_is_alive', return_value=True):
            session = SAPClient._find_ready_session(app, 'LUP', sap_user='BOT1')


        self.assertIs(session, expected_user)


    def test_find_ready_session_rejects_other_sap_users(self):
        app = self._make_sap_app([
            self._make_sap_session(user='SUPERVISOR'),
            self._make_sap_session(user='BOT2'),
        ])


        with patch.object(SAPClient, '_session_is_alive', return_value=True):
            session = SAPClient._find_ready_session(app, 'LUP', sap_user='BOT1')


        self.assertIsNone(session)


    def test_find_ready_session_can_require_client(self):
        wrong_client = self._make_sap_session(client='200', user='BOT1')
        expected = self._make_sap_session(client='100', user='BOT1')
        app = self._make_sap_app([wrong_client, expected])


        with patch.object(SAPClient, '_session_is_alive', return_value=True):
            session = SAPClient._find_ready_session(
                app,
                'LUP',
                sap_user='BOT1',
                sap_client='100',
            )


        self.assertIs(session, expected)


    def test_session_is_alive_rejects_busy_session(self):
        busy = self._make_sap_session(user='BOT1', busy=True)


        self.assertFalse(SAPClient._session_is_alive(busy))


    def test_find_ready_session_skips_busy_and_selects_free_session(self):
        busy = self._make_sap_session(user='BOT1', busy=True)
        free = self._make_sap_session(user='BOT1', busy=False)
        app = self._make_sap_app([busy, free])


        session = SAPClient._find_ready_session(app, 'LUP', sap_user='BOT1')


        self.assertIs(session, free)


    def test_find_ready_session_ignores_other_user_busy_and_selects_configured_user(self):
        other_busy = self._make_sap_session(user='JUAN.MORENO', busy=True)
        nexhus_free = self._make_sap_session(user='LOPEZHJC')
        app = self._make_sap_app([other_busy, nexhus_free])


        session = SAPClient._find_ready_session(app, 'LUP', sap_user='LOPEZHJC')


        self.assertIs(session, nexhus_free)
        self.assertEqual(SAPClient._last_session_selection_code, 'SAP_NO_READY_SESSION')


    def test_find_ready_session_skips_other_user_before_reading_slow_fields(self):
        calls = {
            'user': 0,
            'transaction': 0,
            'busy': 0,
            'find': 0,
            'system': 0,
            'client': 0,
        }


        class Info:
            @property
            def User(self):
                calls['user'] += 1
                return 'JUAN.MORENO'


            @property
            def Transaction(self):
                calls['transaction'] += 1
                raise AssertionError('No debe leer Transaction de sesiones de otro usuario')


            @property
            def SystemName(self):
                calls['system'] += 1
                raise AssertionError('No debe leer SystemName de sesiones de otro usuario')


            @property
            def Client(self):
                calls['client'] += 1
                raise AssertionError('No debe leer Client de sesiones de otro usuario')


        class OtherUserSession:
            def __init__(self):
                self.Info = Info()


            @property
            def Busy(self):
                calls['busy'] += 1
                raise AssertionError('No debe leer Busy de sesiones de otro usuario')


            def findById(self, element_id):
                calls['find'] += 1
                raise AssertionError(f'No debe tocar {element_id} en sesiones de otro usuario')


        app = self._make_sap_app([OtherUserSession()])


        with patch.object(SAPClient, '_probe_session_for_work') as probe:
            session = SAPClient._find_ready_session(
                app,
                'LUP',
                probe=True,
                sap_user='LOPEZHJC',
            )


        self.assertIsNone(session)
        self.assertEqual(calls['user'], 1)
        self.assertEqual(calls['transaction'], 0)
        self.assertEqual(calls['busy'], 0)
        self.assertEqual(calls['find'], 0)
        self.assertEqual(calls['system'], 0)
        self.assertEqual(calls['client'], 0)
        self.assertEqual(SAPClient._last_session_selection_code, 'SAP_NO_READY_SESSION')
        probe.assert_not_called()


    def test_find_ready_session_skips_unknown_user_before_reading_slow_fields(self):
        calls = {
            'user': 0,
            'transaction': 0,
            'busy': 0,
            'find': 0,
        }


        class Info:
            @property
            def User(self):
                calls['user'] += 1
                return ''


            @property
            def Transaction(self):
                calls['transaction'] += 1
                raise AssertionError('No debe leer Transaction si no hay usuario SAP')


        class UnknownUserSession:
            def __init__(self):
                self.Info = Info()


            @property
            def Busy(self):
                calls['busy'] += 1
                raise AssertionError('No debe leer Busy si no hay usuario SAP')


            def findById(self, element_id):
                calls['find'] += 1
                raise AssertionError(f'No debe tocar {element_id} si no hay usuario SAP')


        app = self._make_sap_app([UnknownUserSession()])


        with patch.object(SAPClient, '_probe_session_for_work') as probe:
            session = SAPClient._find_ready_session(
                app,
                'LUP',
                probe=True,
                sap_user='LOPEZHJC',
            )


        self.assertIsNone(session)
        self.assertEqual(calls['user'], 1)
        self.assertEqual(calls['transaction'], 0)
        self.assertEqual(calls['busy'], 0)
        self.assertEqual(calls['find'], 0)
        self.assertEqual(SAPClient._last_session_selection_code, 'SAP_NO_READY_SESSION')
        probe.assert_not_called()


    def test_connect_opens_new_login_when_only_other_user_is_busy(self):
        other_busy = self._make_sap_session(user='JUAN.MORENO', busy=True)
        nexhus_free = self._make_sap_session(user='LOPEZHJC')
        first_app = self._make_sap_app([other_busy])
        second_app = self._make_sap_app([other_busy, nexhus_free])
        client = SAPClient(sap_user='LOPEZHJC')


        with (
            patch.object(SAPClient, '_get_sap_app', side_effect=[first_app, second_app]),
            patch.object(SAPClient, 'close_sessions', return_value=0),
            patch.object(SAPClient, 'open_and_login', return_value=(True, 'LOPEZHJC', 'OK')) as login,
            patch('core.sap_client.SAP_PROBE_BEFORE_WORK', False),
        ):
            connected = client.connect(auto_login=True)


        self.assertTrue(connected)
        self.assertIs(client.session, nexhus_free)
        login.assert_called_once()


    def test_connect_force_new_login_skips_existing_session_scan(self):
        login_session = self._make_sap_session(user='LOPEZHJC')
        client = SAPClient(sap_user='LOPEZHJC')


        with (
            patch.object(SAPClient, '_get_sap_app') as get_app,
            patch.object(SAPClient, '_find_ready_session') as find_ready,
            patch.object(
                SAPClient,
                'open_and_login',
                return_value=(True, 'LOPEZHJC', 'OK', login_session),
            ) as login,
        ):
            connected = client.connect(auto_login=True, force_new_login=True)


        self.assertTrue(connected)
        self.assertIs(client.session, login_session)
        get_app.assert_not_called()
        find_ready.assert_not_called()
        login.assert_called_once()


    def test_connect_returns_busy_when_configured_user_session_is_busy(self):
        own_busy = self._make_sap_session(user='LOPEZHJC', busy=True)
        app = self._make_sap_app([own_busy])
        client = SAPClient(sap_user='LOPEZHJC')


        with (
            patch.object(SAPClient, '_get_sap_app', return_value=app),
            patch.object(SAPClient, 'open_and_login') as login,
        ):
            with self.assertRaises(SAPBusySessionError):
                client.connect(auto_login=True)


        login.assert_not_called()


    def test_find_ready_session_skips_modal_and_selects_free_session(self):
        modal = self._make_sap_session(user='BOT1', modal=True)
        free = self._make_sap_session(user='BOT1')
        app = self._make_sap_app([modal, free])


        session = SAPClient._find_ready_session(app, 'LUP', sap_user='BOT1')


        self.assertIs(session, free)


    def test_find_ready_session_all_busy_fails_fast(self):
        app = self._make_sap_app([
            self._make_sap_session(user='BOT1', busy=True),
            self._make_sap_session(user='BOT1', busy=True),
        ])


        with patch.object(SAPClient, '_probe_session_for_work') as probe:
            session = SAPClient._find_ready_session(app, 'LUP', probe=True, sap_user='BOT1')


        self.assertIsNone(session)
        self.assertEqual(SAPClient._last_session_selection_code, 'SAP_BUSY_SESSION')
        probe.assert_not_called()


    def test_find_ready_session_does_not_probe_busy_sessions(self):
        busy = self._make_sap_session(user='BOT1', busy=True)
        free = self._make_sap_session(user='BOT1')
        app = self._make_sap_app([busy, free])


        with patch.object(SAPClient, '_probe_session_for_work', return_value=True) as probe:
            session = SAPClient._find_ready_session(app, 'LUP', probe=True, sap_user='BOT1')


        self.assertIs(session, free)
        probe.assert_called_once_with(free)


    def test_close_sessions_closes_only_expected_identity(self):
        closed = []


        class Window:
            Text = 'SAP GUI for Windows 770'


            def __init__(self, user):
                self.user = user


            def Close(self):
                closed.append(self.user)


        bot_session = self._make_sap_session(user='BOT1', wnd=Window('BOT1'))
        other_session = self._make_sap_session(user='SUPERVISOR', wnd=Window('SUPERVISOR'))
        app = self._make_sap_app([bot_session, other_session])


        with (
            patch.object(SAPClient, '_get_sap_app', return_value=app),
            patch('core.sap_client.pythoncom.CoInitialize'),
            patch('core.sap_client.pythoncom.CoUninitialize'),
            patch('core.sap_client.time.sleep'),
        ):
            count = SAPClient.close_sessions(sistema='LUP', sap_user='BOT1')


        self.assertEqual(count, 1)
        self.assertEqual(closed, ['BOT1'])


    def test_close_sessions_skips_other_user_without_reading_slow_fields(self):
        calls = {
            'user': 0,
            'system': 0,
            'client': 0,
            'find': 0,
        }


        class Info:
            @property
            def User(self):
                calls['user'] += 1
                return 'JUAN.MORENO'


            @property
            def SystemName(self):
                calls['system'] += 1
                raise AssertionError('No debe leer SystemName al cerrar sesiones ajenas')


            @property
            def Client(self):
                calls['client'] += 1
                raise AssertionError('No debe leer Client al cerrar sesiones ajenas')


        class OtherUserSession:
            def __init__(self):
                self.Info = Info()


            def findById(self, element_id):
                calls['find'] += 1
                raise AssertionError(f'No debe tocar {element_id} al cerrar sesiones ajenas')


        app = self._make_sap_app([OtherUserSession()])


        with (
            patch.object(SAPClient, '_get_sap_app', return_value=app),
            patch('core.sap_client.pythoncom.CoInitialize'),
            patch('core.sap_client.pythoncom.CoUninitialize'),
        ):
            count = SAPClient.close_sessions(sistema='LUP', sap_user='LOPEZHJC')


        self.assertEqual(count, 0)
        self.assertEqual(calls['user'], 1)
        self.assertEqual(calls['system'], 0)
        self.assertEqual(calls['client'], 0)
        self.assertEqual(calls['find'], 0)


class AlreadySeparatedProcessingTests(TestCase):
    def test_phase1_destination_notice_still_runs_f2(self):
        from queue_app.services.hu_processing_service import _process_hu_item

        pallet = Pallet.objects.create(status=Pallet.STATUS_READY)
        item = HUItem.objects.create(
            hu_code='TH0000268197',
            pallet=pallet,
            status=HUItem.STATUS_PENDING,
        )
        sap = MagicMock()
        sap.process_hu_phase1.return_value = {
            'status': 'already_separated',
            'message': 'Ya se hizo el Acknowledge',
            'sbar': '',
        }
        sap.process_hu_phase2.return_value = {
            'status': 'ok',
            'message': 'Separazione OK',
            'duration_ms': 450,
            'phase2_ts': timezone.now(),
        }

        with (
            patch('core.sap_client.SAPClient', return_value=sap),
            patch('queue_app.utils.emit_item_update'),
            patch('queue_app.utils.emit_queue_status'),
            patch('queue_app.utils.emit_stats_update'),
            patch('pythoncom.CoInitialize'),
            patch('pythoncom.CoUninitialize'),
        ):
            result = _process_hu_item(
                item.pk,
                run_f1=True,
                run_f2=True,
                run_pallet_boundary=False,
            )

        self.assertEqual(result.status, HUItem.STATUS_OK)
        self.assertEqual(result.phase1_msg, 'Ya se hizo el Acknowledge')
        self.assertEqual(result.phase2_msg, 'Separazione OK')
        self.assertEqual(result.phase2_ms, 450)
        self.assertEqual(result.error_msg, '')
        sap.setup_phase2.assert_called_once()
        sap.process_hu_phase2.assert_called_once()


class QueueEventTests(TestCase):
    def test_queue_done_forces_running_state_off(self):
        sent_events = []


        with (
            patch('queue_app.utils.calculate_queue_stats', return_value={'is_running': True}),
            patch('queue_app.utils._send_group', side_effect=lambda _event, data: sent_events.append(data)),
        ):
            emit_queue_done({'status': 'error', 'message': 'Queue finished with 1 error(s)'})


        self.assertEqual(sent_events[0]['type'], 'queue_done')
        self.assertFalse(sent_events[0]['stats']['is_running'])


    def test_queue_status_carries_operational_feedback(self):
        sent_events = []


        with (
            patch('queue_app.utils.calculate_queue_stats', return_value={'is_running': True}),
            patch('queue_app.utils._send_group', side_effect=lambda _event, data: sent_events.append(data)),
        ):
            emit_queue_status(
                'Pallet P02 abierto con 5 HU(s). Cierra el pallet para continuar el proceso.',
                badge='EN ESPERA',
                mode='waiting',
                footer='Esperando cierre de P02.',
                active_pallet_id=2,
                active_hu_count=5,
                remaining_seconds=360,
            )


        self.assertEqual(sent_events[0]['type'], 'queue_status')
        self.assertEqual(sent_events[0]['mode'], 'waiting')
        self.assertEqual(sent_events[0]['active_pallet_id'], 2)
        self.assertEqual(sent_events[0]['stats']['is_running'], True)




class ClearQueueTests(TestCase):
    def test_clear_queue_resets_pallet_number_to_one(self):
        for idx in range(3):
            pallet = Pallet.objects.create(status=Pallet.STATUS_ACTIVE)
            HUItem.objects.create(
                hu_code=f'T10045916{idx:03d}',
                pallet=pallet,
                status=HUItem.STATUS_OK,
            )


        with patch('queue_app.views.is_queue_locked', return_value=False):
            response = self.client.post('/cola/limpiar/')


        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload['ok'])
        self.assertEqual(payload['pallet_id'], 1)
        self.assertEqual(payload['stats']['total'], 0)
        self.assertEqual(payload['stats']['pallets'], 1)
        self.assertEqual(HUItem.objects.count(), 0)
        self.assertEqual(Pallet.objects.count(), 1)
        self.assertEqual(Pallet.objects.get().pk, 1)


    def test_clear_queue_does_not_reset_while_processing(self):
        pallet = Pallet.objects.create(status=Pallet.STATUS_ACTIVE)
        HUItem.objects.create(
            hu_code='T10045916000',
            pallet=pallet,
            status=HUItem.STATUS_PROCESSING,
        )


        response = self.client.post('/cola/limpiar/')


        self.assertEqual(response.status_code, 409)
        self.assertEqual(Pallet.objects.get().pk, pallet.pk)


    def test_clear_queue_is_blocked_while_worker_lock_is_active(self):
        with patch('queue_app.views.is_queue_locked', return_value=True):
            response = self.client.post('/cola/limpiar/')


        self.assertEqual(response.status_code, 409)
        self.assertFalse(response.json()['ok'])




class ReprocessQueueTests(TestCase):
    def test_reprocess_pdf_errors_retries_receipt_without_resetting_hus(self):
        completed_at = timezone.now()
        pallet = Pallet.objects.create(
            status=Pallet.STATUS_READY,
            pdf_status=Pallet.PDF_STATUS_ERROR,
            pdf_msg='ZE16: sin Receipt IDs para pallet 7 tras 4 intento(s)',
            pdf_ms=1200,
            receipt_done_at=completed_at,
        )
        item = HUItem.objects.create(
            hu_code='T10045916001',
            pallet=pallet,
            status=HUItem.STATUS_OK,
            phase1_msg='Acknowledge OK',
            phase2_msg='Separazione OK',
            phase2_ms=450,
            receipt_done_at=completed_at,
            pdf_status=Pallet.PDF_STATUS_ERROR,
            pdf_msg=pallet.pdf_msg,
            pdf_ms=1200,
        )

        with (
            patch('queue_app.views.is_queue_locked', return_value=False),
            patch('queue_app.views._celery_start_blocker_response', return_value=None),
            patch('core.sap_client.SAPClient.ensure_session_ready', return_value=(True, 'TEST', 'OK')),
            patch('queue_app.views.process_queue_task.delay') as delay,
            patch('queue_app.views.emit_item_update'),
            patch('queue_app.views.emit_stats_update'),
        ):
            response = self.client.post(
                '/cola/reprocesar/',
                data='{"mode": "pdf_errors"}',
                content_type='application/json',
            )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload['mode'], 'pdf_errors')
        self.assertEqual(payload['count'], 1)
        self.assertEqual(payload['pallet_ids'], [pallet.pk])
        delay.assert_called_once()

        item.refresh_from_db()
        pallet.refresh_from_db()
        self.assertEqual(item.status, HUItem.STATUS_OK)
        self.assertEqual(item.phase1_msg, 'Acknowledge OK')
        self.assertEqual(item.phase2_msg, 'Separazione OK')
        self.assertEqual(item.phase2_ms, 450)
        self.assertEqual(item.pdf_status, '')
        self.assertIsNone(item.receipt_done_at)
        self.assertEqual(pallet.status, Pallet.STATUS_READY)
        self.assertEqual(pallet.pdf_status, '')
        self.assertEqual(pallet.pdf_msg, '')
        self.assertIsNone(pallet.receipt_done_at)
        self.assertIn(pallet, list(pallets_ready_for_pdf_queryset()))


    def test_reprocess_errors_only_marks_only_failed_hus_pending(self):
        pallet = Pallet.objects.create(status=Pallet.STATUS_ACTIVE)
        ok = HUItem.objects.create(hu_code='T10045916001', pallet=pallet, status=HUItem.STATUS_OK)
        failed = HUItem.objects.create(hu_code='T10045916002', pallet=pallet, status=HUItem.STATUS_ERROR)
        not_found = HUItem.objects.create(
            hu_code='T10045916003',
            pallet=pallet,
            status=HUItem.STATUS_HU_NOT_FOUND,
        )


        with (
            patch('queue_app.views.is_queue_locked', return_value=False),
            patch('queue_app.views._celery_start_blocker_response', return_value=None),
            patch('core.sap_client.SAPClient.ensure_session_ready', return_value=(True, 'TEST', 'OK')),
            patch('queue_app.views.process_queue_task.delay'),
            patch('queue_app.views.emit_item_update'),
            patch('queue_app.views.emit_stats_update'),
        ):
            response = self.client.post(
                '/cola/reprocesar/',
                data='{"mode": "errors"}',
                content_type='application/json',
            )


        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()['count'], 2)
        ok.refresh_from_db()
        failed.refresh_from_db()
        not_found.refresh_from_db()
        self.assertEqual(ok.status, HUItem.STATUS_OK)
        self.assertEqual(failed.status, HUItem.STATUS_PENDING)
        self.assertEqual(not_found.status, HUItem.STATUS_PENDING)


    def test_reprocess_all_marks_all_finished_hus_pending(self):
        pallet = Pallet.objects.create(status=Pallet.STATUS_ACTIVE)
        items = [
            HUItem.objects.create(hu_code='T10045916001', pallet=pallet, status=HUItem.STATUS_OK),
            HUItem.objects.create(hu_code='T10045916002', pallet=pallet, status=HUItem.STATUS_DUPLICATE),
            HUItem.objects.create(hu_code='T10045916003', pallet=pallet, status=HUItem.STATUS_ERROR),
        ]


        with (
            patch('queue_app.views.is_queue_locked', return_value=False),
            patch('queue_app.views._celery_start_blocker_response', return_value=None),
            patch('core.sap_client.SAPClient.ensure_session_ready', return_value=(True, 'TEST', 'OK')),
            patch('queue_app.views.process_queue_task.delay'),
            patch('queue_app.views.emit_item_update'),
            patch('queue_app.views.emit_stats_update'),
        ):
            response = self.client.post(
                '/cola/reprocesar/',
                data='{"mode": "all"}',
                content_type='application/json',
            )


        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()['count'], 3)
        for item in items:
            item.refresh_from_db()
            self.assertEqual(item.status, HUItem.STATUS_PENDING)


    def test_reprocess_is_blocked_without_sap_session(self):
        pallet = Pallet.objects.create(status=Pallet.STATUS_ACTIVE)
        HUItem.objects.create(hu_code='T10045916001', pallet=pallet, status=HUItem.STATUS_ERROR)


        with (
            patch('queue_app.views.is_queue_locked', return_value=False),
            patch('queue_app.views._celery_start_blocker_response', return_value=None),
            patch('core.sap_client.SAPClient.ensure_session_ready', return_value=(False, '', 'SAP no disponible')),
            patch('queue_app.views.process_queue_task.delay') as delay,
        ):
            response = self.client.post(
                '/cola/reprocesar/',
                data='{"mode": "errors"}',
                content_type='application/json',
            )


        self.assertEqual(response.status_code, 409)
        self.assertFalse(response.json()['ok'])
        delay.assert_not_called()
        pallet.refresh_from_db()
        self.assertEqual(pallet.status, Pallet.STATUS_ACTIVE)


    def test_reprocess_does_not_open_sap_without_target_hus(self):
        with (
            patch('queue_app.views.is_queue_locked', return_value=False),
            patch('core.sap_client.SAPClient.ensure_session_ready') as ensure_session_ready,
            patch('queue_app.views.process_queue_task.delay') as delay,
        ):
            response = self.client.post(
                '/cola/reprocesar/',
                data='{"mode": "all"}',
                content_type='application/json',
            )


        self.assertEqual(response.status_code, 400)
        self.assertFalse(response.json()['ok'])
        ensure_session_ready.assert_not_called()
        delay.assert_not_called()


    def test_reprocess_is_blocked_when_celery_has_no_workers(self):
        pallet = Pallet.objects.create(status=Pallet.STATUS_ACTIVE)
        HUItem.objects.create(hu_code='T10045916001', pallet=pallet, status=HUItem.STATUS_ERROR)


        blocker = {
            'message': 'No se puede iniciar proceso: Celery no esta activo. Redis esta conectado, pero ningun worker respondio.',
            'redis': {'ok': True},
            'celery': {'ok': False, 'workers': []},
        }
        with (
            patch('queue_app.views.is_queue_locked', return_value=False),
            patch('queue_app.views._celery_start_blocker', return_value=blocker),
            patch('core.sap_client.SAPClient.ensure_session_ready') as ensure_session_ready,
            patch('queue_app.views.process_queue_task.delay') as delay,
        ):
            response = self.client.post(
                '/cola/reprocesar/',
                data='{"mode": "errors"}',
                content_type='application/json',
            )


        self.assertEqual(response.status_code, 503)
        self.assertFalse(response.json()['ok'])
        self.assertIn('Celery no esta activo', response.json()['error'])
        ensure_session_ready.assert_not_called()
        delay.assert_not_called()


    def test_reprocess_closes_sap_if_celery_dispatch_fails(self):
        pallet = Pallet.objects.create(status=Pallet.STATUS_ACTIVE)
        HUItem.objects.create(hu_code='T10045916001', pallet=pallet, status=HUItem.STATUS_ERROR)


        with (
            patch('queue_app.views.is_queue_locked', return_value=False),
            patch('queue_app.views._celery_start_blocker_response', return_value=None),
            patch('core.sap_client.SAPClient.ensure_session_ready', return_value=(True, 'TEST', 'OK')),
            patch('queue_app.views.process_queue_task.delay', side_effect=RuntimeError('Redis down')),
            patch('core.sap_client.SAPClient.close_sessions') as close_sessions,
            patch('queue_app.views.emit_item_update'),
            patch('queue_app.views.emit_stats_update'),
        ):
            response = self.client.post(
                '/cola/reprocesar/',
                data='{"mode": "all"}',
                content_type='application/json',
            )


        self.assertEqual(response.status_code, 503)
        self.assertFalse(response.json()['ok'])
        close_sessions.assert_called_once()




class StartProcessingTests(TestCase):
    def test_start_processing_allows_direct_login_when_sap_status_probe_is_timeout(self):
        pallet = Pallet.objects.create(status=Pallet.STATUS_ACTIVE)
        HUItem.objects.create(hu_code='T10045916001', pallet=pallet, status=HUItem.STATUS_PENDING)


        with (
            patch('queue_app.views._celery_start_blocker_response', return_value=None),
            patch('queue_app.services.diagnostics_service.get_fast_sap_status', return_value={
                'status': 'timeout',
                'source': 'timeout',
                'reason': 'live_timeout',
                'code': 'SAP_STATUS_TIMEOUT',
            }),
            patch('core.sap_client.SAPClient.ensure_session_ready') as ensure_session_ready,
            patch('queue_app.views.process_queue_task.delay') as delay,
        ):
            response = self.client.post('/api/procesar/', data='{}', content_type='application/json')


        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json()['ok'])
        ensure_session_ready.assert_not_called()
        delay.assert_called_once()
        self.assertTrue(delay.call_args.kwargs['force_new_sap_login'])


    def test_start_processing_is_blocked_when_worker_lock_is_active(self):
        pallet = Pallet.objects.create(status=Pallet.STATUS_ACTIVE)
        HUItem.objects.create(hu_code='T10045916001', pallet=pallet, status=HUItem.STATUS_PENDING)


        with (
            patch('queue_app.views.is_queue_locked', return_value=True),
            patch('core.sap_client.SAPClient.ensure_session_ready') as ensure_session_ready,
            patch('queue_app.views.process_queue_task.delay') as delay,
        ):
            response = self.client.post('/api/procesar/', data='{}', content_type='application/json')


        self.assertEqual(response.status_code, 409)
        ensure_session_ready.assert_not_called()
        delay.assert_not_called()


    def test_start_processing_does_not_open_sap_without_work(self):
        Pallet.objects.create(status=Pallet.STATUS_ACTIVE)


        with (
            patch('queue_app.views.is_queue_locked', return_value=False),
            patch('core.sap_client.SAPClient.ensure_session_ready') as ensure_session_ready,
            patch('core.sap_client.SAPClient.close_sessions') as close_sessions,
            patch('queue_app.views.process_queue_task.delay') as delay,
        ):
            response = self.client.post('/api/procesar/', data='{}', content_type='application/json')


        self.assertEqual(response.status_code, 200)
        self.assertFalse(response.json()['ok'])
        ensure_session_ready.assert_not_called()
        close_sessions.assert_called_once()
        delay.assert_not_called()


    def test_start_processing_is_blocked_when_celery_has_no_workers(self):
        pallet = Pallet.objects.create(status=Pallet.STATUS_ACTIVE)
        HUItem.objects.create(hu_code='T10045916001', pallet=pallet, status=HUItem.STATUS_PENDING)


        blocker = {
            'message': 'No se puede iniciar proceso: Celery no esta activo. Redis esta conectado, pero ningun worker respondio.',
            'redis': {'ok': True},
            'celery': {'ok': False, 'workers': []},
        }
        with (
            patch('queue_app.views.is_queue_locked', return_value=False),
            patch('queue_app.views._celery_start_blocker', return_value=blocker),
            patch('core.sap_client.SAPClient.ensure_session_ready') as ensure_session_ready,
            patch('queue_app.views.process_queue_task.delay') as delay,
        ):
            response = self.client.post('/api/procesar/', data='{}', content_type='application/json')


        self.assertEqual(response.status_code, 503)
        self.assertFalse(response.json()['ok'])
        self.assertIn('Celery no esta activo', response.json()['error'])
        ensure_session_ready.assert_not_called()
        delay.assert_not_called()


    def test_sap_start_endpoint_does_not_open_sap_without_work(self):
        with (
            patch('core.sap_client.SAPClient.ensure_session_ready') as ensure_session_ready,
            patch('core.sap_client.SAPClient.close_sessions') as close_sessions,
        ):
            response = self.client.post('/api/sap/iniciar/')


        self.assertEqual(response.status_code, 400)
        self.assertFalse(response.json()['ok'])
        ensure_session_ready.assert_not_called()
        close_sessions.assert_called_once()


    def test_sap_start_endpoint_allows_active_hus(self):
        pallet = Pallet.objects.create(status=Pallet.STATUS_ACTIVE)
        HUItem.objects.create(hu_code='T10045916001', pallet=pallet, status=HUItem.STATUS_PENDING)


        with patch(
            'core.sap_client.SAPClient.ensure_session_ready',
            return_value=(True, 'TEST', 'OK'),
        ) as ensure_session_ready:
            response = self.client.post('/api/sap/iniciar/')


        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json()['ok'])
        ensure_session_ready.assert_called_once()


    def test_start_processing_closes_sap_if_celery_dispatch_fails(self):
        pallet = Pallet.objects.create(status=Pallet.STATUS_ACTIVE)
        HUItem.objects.create(hu_code='T10045916001', pallet=pallet, status=HUItem.STATUS_PENDING)


        with (
            patch('queue_app.views.is_queue_locked', return_value=False),
            patch('queue_app.views._celery_start_blocker_response', return_value=None),
            patch('queue_app.views._sap_start_blocker_response', return_value=None),
            patch('core.sap_client.SAPClient.ensure_session_ready', return_value=(True, 'TEST', 'OK')),
            patch('queue_app.views.process_queue_task.delay', side_effect=RuntimeError('Redis down')),
            patch('core.sap_client.SAPClient.close_sessions') as close_sessions,
        ):
            response = self.client.post('/api/procesar/', data='{}', content_type='application/json')


        self.assertEqual(response.status_code, 503)
        self.assertFalse(response.json()['ok'])
        close_sessions.assert_called_once()


    def test_start_processing_recovers_stale_lock_without_processing_hus(self):
        pallet = Pallet.objects.create(status=Pallet.STATUS_ACTIVE)
        HUItem.objects.create(hu_code='T10045916001', pallet=pallet, status=HUItem.STATUS_PENDING)
        queue_status = {
            'is_locked': True,
            'worker_stale': True,
            'processing_hus': 0,
            'stop_requested': False,
            'stop_request_age_seconds': None,
        }


        with (
            patch('queue_app.views._queue_diagnostic_snapshot', return_value=queue_status),
            patch('queue_app.views.clear_queue_runtime_state') as clear_runtime,
            patch('queue_app.views.emit_stats_update'),
            patch('queue_app.views.emit_queue_status'),
            patch('queue_app.views._celery_start_blocker_response', return_value=None),
            patch('queue_app.views._sap_start_blocker_response', return_value=None),
            patch('core.sap_client.SAPClient.ensure_session_ready', return_value=(True, 'TEST', 'OK')),
            patch('queue_app.views.process_queue_task.delay') as delay,
        ):
            response = self.client.post('/api/procesar/', data='{}', content_type='application/json')


        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json()['ok'])
        clear_runtime.assert_called_once()
        delay.assert_called_once()


    def test_stop_requests_worker_cancellation(self):
        with (
            patch('queue_app.views._queue_diagnostic_snapshot', return_value={'is_locked': True, 'worker_stale': False}),
            patch('queue_app.views._check_redis_status', return_value={'ok': True}),
            patch('queue_app.views._check_celery_status', return_value={'ok': True}),
            patch('queue_app.views.request_queue_stop', return_value=True) as stop,
        ):
            response = self.client.post('/cola/detener/')


        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json()['ok'])
        stop.assert_called_once()


    def test_stop_recovers_orphaned_runtime_when_celery_was_forced_closed(self):
        pallet = Pallet.objects.create(status=Pallet.STATUS_READY)
        item = HUItem.objects.create(
            hu_code='T10045916001',
            pallet=pallet,
            status=HUItem.STATUS_PROCESSING,
            processing_started_at=timezone.now() - timedelta(seconds=30),
        )
        queue_status = {
            'is_locked': True,
            'worker_stale': True,
        }
        celery_status = {
            'ok': False,
            'message': CELERY_NO_WORKER_MESSAGE,
        }


        with (
            patch('queue_app.views._queue_diagnostic_snapshot', return_value=queue_status),
            patch('queue_app.views._check_redis_status', return_value={'ok': True}),
            patch('queue_app.views._check_celery_status', return_value=celery_status),
            patch('queue_app.views.clear_queue_runtime_state') as clear_runtime,
            patch('queue_app.views.emit_item_update') as emit_item,
            patch('queue_app.views.emit_stats_update'),
            patch('queue_app.views.emit_queue_done') as emit_done,
            patch('queue_app.views.request_queue_stop') as stop,
        ):
            response = self.client.post('/cola/detener/')


        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload['ok'])
        self.assertTrue(payload['recovered'])
        clear_runtime.assert_called_once()
        stop.assert_not_called()
        emit_item.assert_called_once()
        emit_done.assert_called_once()
        item.refresh_from_db()
        self.assertEqual(item.status, HUItem.STATUS_ERROR)
        self.assertIn('Celery', item.error_msg)


    def test_stop_recovers_when_safe_stop_waits_too_long(self):
        pallet = Pallet.objects.create(status=Pallet.STATUS_READY)
        item = HUItem.objects.create(
            hu_code='T10045916001',
            pallet=pallet,
            status=HUItem.STATUS_PROCESSING,
            processing_started_at=timezone.now() - timedelta(seconds=60),
        )
        queue_status = {
            'is_locked': True,
            'worker_stale': False,
            'stop_requested': True,
            'stop_request_age_seconds': 45,
        }


        with (
            patch('queue_app.views._queue_diagnostic_snapshot', return_value=queue_status),
            patch('queue_app.views._check_redis_status', return_value={'ok': True}),
            patch('queue_app.views._check_celery_status', return_value={'ok': True}),
            patch('queue_app.views.clear_queue_runtime_state') as clear_runtime,
            patch('queue_app.views.emit_item_update') as emit_item,
            patch('queue_app.views.emit_stats_update'),
            patch('queue_app.views.emit_queue_done') as emit_done,
            patch('queue_app.views.request_queue_stop') as stop,
        ):
            response = self.client.post(
                '/cola/detener/',
                data='{"force": true}',
                content_type='application/json',
            )


        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload['ok'])
        self.assertTrue(payload['recovered'])
        clear_runtime.assert_called_once()
        stop.assert_not_called()
        emit_item.assert_called_once()
        emit_done.assert_called_once()
        item.refresh_from_db()
        self.assertEqual(item.status, HUItem.STATUS_ERROR)


    def test_start_processing_allows_pdf_pending_pallet_without_pending_hus(self):
        pallet = Pallet.objects.create(status=Pallet.STATUS_ACTIVE)
        HUItem.objects.create(hu_code='T10045916001', pallet=pallet, status=HUItem.STATUS_OK)


        with (
            patch('queue_app.views.is_queue_locked', return_value=False),
            patch('queue_app.views._celery_start_blocker_response', return_value=None),
            patch('queue_app.views._sap_start_blocker_response', return_value=None),
            patch('core.sap_client.SAPClient.ensure_session_ready', return_value=(True, 'TEST', 'OK')),
            patch('queue_app.views.process_queue_task.delay') as delay,
        ):
            response = self.client.post('/api/procesar/', data='{}', content_type='application/json')


        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload['ok'])
        self.assertEqual(payload['count'], 0)
        self.assertEqual(payload['pdf_count'], 1)
        delay.assert_called_once()




class ScanHuTests(TestCase):
    def test_scan_database_error_returns_retryable_json(self):
        with patch(
            'queue_app.views._get_or_create_active_pallet',
            side_effect=DatabaseError('database locked'),
        ):
            response = self.client.post(
                '/scan/',
                data='{"code": "TH0000268197"}',
                content_type='application/json',
            )


        self.assertEqual(response.status_code, 503)
        payload = response.json()
        self.assertFalse(payload['ok'])
        self.assertTrue(payload['retryable'])
        self.assertEqual(payload['type'], 'database_error')
        self.assertEqual(HUItem.objects.count(), 0)


    def test_scan_auto_start_failure_does_not_drop_saved_hu(self):
        with (
            patch('queue_app.views._auto_start_queue_after_pallet_close', side_effect=RuntimeError('celery down')),
            patch('queue_app.views.emit_item_update'),
            patch('queue_app.views.emit_stats_update'),
            patch('queue_app.views.emit_current_queue_status_if_available'),
        ):
            response = self.client.post(
                '/scan/',
                data='{"code": "2972200143"}',
                content_type='application/json',
            )


        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload['ok'])
        self.assertFalse(payload['auto_started'])
        self.assertEqual(payload['auto_start_reason'], 'auto_start_failed')
        self.assertEqual(HUItem.objects.filter(hu_code='2972200143').count(), 1)


    def test_scan_response_includes_updated_stats_for_current_ui_state(self):
        with (
            patch('queue_app.views.is_queue_locked', return_value=False),
            patch('queue_app.views.emit_item_update'),
            patch('queue_app.views.emit_stats_update'),
        ):
            response = self.client.post(
                '/scan/',
                data='{"code": "TH0000268197"}',
                content_type='application/json',
            )


        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload['ok'])
        self.assertIn('stats', payload)
        self.assertEqual(payload['stats']['total'], 1)
        self.assertEqual(payload['stats']['pending'], 1)
        self.assertEqual(payload['stats']['pallets'], 1)


    def test_scan_batch_processes_pending_browser_outbox(self):
        with (
            patch('queue_app.views.is_queue_locked', return_value=False),
            patch('queue_app.views.emit_item_update') as emit_item_update,
            patch('queue_app.views.emit_stats_update') as emit_stats_update,
            patch('queue_app.views.emit_current_queue_status_if_available') as emit_queue_status,
        ):
            response = self.client.post(
                '/scan/batch/',
                data=(
                    '{"items": ['
                    '{"id": "a", "code": "TH0000268197", "options": {}},'
                    '{"id": "b", "code": "TH0000268256", "options": {}}'
                    ']}'
                ),
                content_type='application/json',
            )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload['ok'])
        self.assertEqual(payload['saved'], 2)
        self.assertEqual(payload['retryable'], 0)
        self.assertEqual(HUItem.objects.count(), 2)
        self.assertEqual(payload['stats']['pending'], 2)
        self.assertEqual(emit_item_update.call_count, 2)
        emit_stats_update.assert_called_once()
        emit_queue_status.assert_called_once()


    def test_scan_is_allowed_when_worker_lock_is_active(self):
        with (
            patch('queue_app.views.is_queue_locked', return_value=True),
            patch('queue_app.views.get_processing_pallet_ids', return_value=set()),
            patch('queue_app.views.emit_item_update'),
            patch('queue_app.views.emit_stats_update'),
        ):
            response = self.client.post(
                '/scan/',
                data='{"code": "TH0000268197"}',
                content_type='application/json',
            )


        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json()['ok'])
        self.assertEqual(HUItem.objects.count(), 1)


    def test_scan_during_processing_starts_new_pallet_when_active_pallet_is_in_current_run(self):
        pallet = Pallet.objects.create(status=Pallet.STATUS_ACTIVE, origin_code='THA')
        HUItem.objects.create(hu_code='TH0000268000', pallet=pallet, status=HUItem.STATUS_PENDING)


        with (
            patch('queue_app.views.is_queue_locked', return_value=True),
            patch('queue_app.views.get_processing_pallet_ids', return_value={pallet.pk}),
            patch('queue_app.views.emit_item_update'),
            patch('queue_app.views.emit_stats_update'),
        ):
            response = self.client.post(
                '/scan/',
                data='{"code": "TH0000268197"}',
                content_type='application/json',
            )


        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json()['ok'])
        self.assertEqual(Pallet.objects.count(), 2)
        self.assertEqual(HUItem.objects.get(hu_code='TH0000268197').pallet_id, 2)


    def test_scan_during_processing_uses_latest_intake_pallet_not_in_current_run(self):
        processing_pallet = Pallet.objects.create(status=Pallet.STATUS_ACTIVE, origin_code='THA')
        intake_pallet = Pallet.objects.create(status=Pallet.STATUS_ACTIVE, origin_code='THA')
        HUItem.objects.create(
            hu_code='TH0000268000',
            pallet=processing_pallet,
            status=HUItem.STATUS_PENDING,
        )
        HUItem.objects.create(
            hu_code='TH0000268001',
            pallet=intake_pallet,
            status=HUItem.STATUS_PENDING,
        )


        with (
            patch('queue_app.views.is_queue_locked', return_value=True),
            patch('queue_app.views.get_processing_pallet_ids', return_value={processing_pallet.pk}),
            patch('queue_app.views.emit_item_update'),
            patch('queue_app.views.emit_stats_update'),
        ):
            response = self.client.post(
                '/scan/',
                data='{"code": "TH0000268197"}',
                content_type='application/json',
            )


        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json()['ok'])
        self.assertEqual(Pallet.objects.count(), 2)
        self.assertEqual(HUItem.objects.get(hu_code='TH0000268197').pallet_id, intake_pallet.pk)




class DeleteHuTests(TestCase):
    def test_delete_failed_hu_resets_pdf_status_when_remaining_hus_are_ok(self):
        pallet = Pallet.objects.create(
            status=Pallet.STATUS_ACTIVE,
            pdf_status=Pallet.PDF_STATUS_ERROR,
            pdf_msg='PDF omitido: HU con error',
            pdf_ms=1200,
        )
        HUItem.objects.create(hu_code='T10045916001', pallet=pallet, status=HUItem.STATUS_OK)
        HUItem.objects.create(hu_code='T10045916002', pallet=pallet, status=HUItem.STATUS_ERROR)


        with (
            patch('queue_app.views.is_queue_locked', return_value=False),
            patch('queue_app.views.emit_item_update'),
            patch('queue_app.views.emit_stats_update'),
        ):
            response = self.client.post('/hu/T10045916002/borrar/')


        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload['ok'])
        self.assertTrue(payload['pdf_reset'])
        self.assertEqual(payload['stats']['pdf_pending'], 1)


        pallet.refresh_from_db()
        self.assertEqual(pallet.pdf_status, '')
        self.assertEqual(pallet.pdf_msg, '')
        self.assertEqual(pallet.pdf_ms, 0)
        self.assertEqual(pallet.status, Pallet.STATUS_READY)


    def test_delete_hu_is_blocked_when_worker_lock_is_active(self):
        pallet = Pallet.objects.create(status=Pallet.STATUS_ACTIVE)
        HUItem.objects.create(hu_code='T10045916002', pallet=pallet, status=HUItem.STATUS_ERROR)


        with patch('queue_app.views.is_queue_locked', return_value=True):
            response = self.client.post('/hu/T10045916002/borrar/')


        self.assertEqual(response.status_code, 409)
        self.assertFalse(response.json()['ok'])
        self.assertEqual(HUItem.objects.count(), 1)




class NewPalletTests(TestCase):
    def test_new_pallet_returns_updated_stats(self):
        pallet = Pallet.objects.create(status=Pallet.STATUS_ACTIVE)
        HUItem.objects.create(hu_code='T10045916001', pallet=pallet, status=HUItem.STATUS_PENDING)


        response = self.client.post('/pallet/nuevo/')


        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload['ok'])
        self.assertIn('stats', payload)
        self.assertEqual(payload['stats']['pallets'], 2)


    def test_new_pallet_is_allowed_when_worker_lock_is_active(self):
        pallet = Pallet.objects.create(status=Pallet.STATUS_ACTIVE)
        HUItem.objects.create(hu_code='T10045916001', pallet=pallet, status=HUItem.STATUS_PENDING)


        with patch('queue_app.views.is_queue_locked', return_value=True):
            response = self.client.post('/pallet/nuevo/')


        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json()['ok'])
        self.assertEqual(Pallet.objects.count(), 2)


    def test_new_pallet_reactivates_worker_when_continuous_mode_is_armed(self):
        pallet = Pallet.objects.create(status=Pallet.STATUS_ACTIVE)
        HUItem.objects.create(hu_code='T10045916001', pallet=pallet, status=HUItem.STATUS_PENDING)


        with (
            patch('queue_app.views.is_continuous_queue_armed', return_value=True),
            patch('queue_app.views.is_queue_locked', return_value=False),
            patch('queue_app.views._celery_start_blocker', return_value=None),
            patch('core.sap_client.SAPClient.ensure_session_ready', return_value=(True, 'TEST', 'OK')),
            patch('queue_app.views.process_queue_task.delay') as delay,
            patch('queue_app.views.arm_continuous_queue') as arm,
        ):
            response = self.client.post(
                '/pallet/nuevo/',
                data='{"run_f1": false, "run_f2": true, "run_pdf": true}',
                content_type='application/json',
            )


        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload['ok'])
        self.assertTrue(payload['auto_started'])
        pallet.refresh_from_db()
        self.assertEqual(pallet.status, Pallet.STATUS_READY)
        delay.assert_called_once_with(
            run_f1=False,
            run_f2=True,
            run_pdf=True,
            continuous=True,
            idle_timeout=QUEUE_CONTINUOUS_IDLE_TIMEOUT_SECONDS,
        )
        arm.assert_called_once()


    def test_new_pallet_does_not_reactivate_worker_when_celery_is_down(self):
        pallet = Pallet.objects.create(status=Pallet.STATUS_ACTIVE)
        HUItem.objects.create(hu_code='T10045916001', pallet=pallet, status=HUItem.STATUS_PENDING)


        blocker = {
            'message': 'No se puede iniciar proceso: Celery no esta activo. Redis esta conectado, pero ningun worker respondio.',
            'redis': {'ok': True},
            'celery': {'ok': False, 'workers': []},
        }
        with (
            patch('queue_app.views.is_continuous_queue_armed', return_value=True),
            patch('queue_app.views.is_queue_locked', return_value=False),
            patch('queue_app.views._celery_start_blocker', return_value=blocker),
            patch('core.sap_client.SAPClient.ensure_session_ready') as ensure_session_ready,
            patch('queue_app.views.process_queue_task.delay') as delay,
        ):
            response = self.client.post('/pallet/nuevo/')


        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload['ok'])
        self.assertFalse(payload['auto_started'])
        self.assertEqual(payload['auto_start_reason'], 'celery_unavailable')
        self.assertIn('Celery no esta activo', payload['auto_start_error'])
        ensure_session_ready.assert_not_called()
        delay.assert_not_called()


    def test_new_pallet_does_not_reactivate_worker_before_manual_start(self):
        pallet = Pallet.objects.create(status=Pallet.STATUS_ACTIVE)
        HUItem.objects.create(hu_code='T10045916001', pallet=pallet, status=HUItem.STATUS_PENDING)


        with (
            patch('queue_app.views.is_continuous_queue_armed', return_value=False),
            patch('queue_app.views.process_queue_task.delay') as delay,
        ):
            response = self.client.post('/pallet/nuevo/')


        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload['ok'])
        self.assertFalse(payload['auto_started'])
        self.assertEqual(payload['auto_start_reason'], 'not_armed')
        delay.assert_not_called()




class DeletePalletTests(TestCase):
    def test_delete_pallet_is_blocked_when_worker_lock_is_active(self):
        pallet = Pallet.objects.create(status=Pallet.STATUS_ACTIVE)
        HUItem.objects.create(hu_code='T10045916001', pallet=pallet, status=HUItem.STATUS_ERROR)


        with patch('queue_app.views.is_queue_locked', return_value=True):
            response = self.client.post(f'/pallet/{pallet.pk}/borrar/')


        self.assertEqual(response.status_code, 409)
        self.assertFalse(response.json()['ok'])
        self.assertEqual(Pallet.objects.count(), 1)
        self.assertEqual(HUItem.objects.count(), 1)




class QueueConsumerTests(TestCase):
    def test_stats_update_includes_pdf_pending(self):
        consumer = QueueConsumer()
        sent_payloads = []


        async def fake_send_json(payload):
            sent_payloads.append(payload)


        consumer._send_json = fake_send_json


        async_to_sync(consumer.stats_update)({
            'type': 'stats_update',
            'total': 2,
            'ok': 1,
            'errors': 0,
            'pending': 1,
            'pallets': 1,
            'pdf_pending': 3,
            'is_running': True,
        })


        self.assertEqual(sent_payloads[0]['pdf_pending'], 3)
        self.assertTrue(sent_payloads[0]['is_running'])


    def test_queue_done_includes_fresh_stats(self):
        consumer = QueueConsumer()
        sent_payloads = []


        async def fake_send_json(payload):
            sent_payloads.append(payload)


        consumer._send_json = fake_send_json


        async_to_sync(consumer.queue_done)({
            'type': 'queue_done',
            'status': 'ok',
            'message': 'done',
            'pallets_processed': 1,
            'hus_processed': 2,
            'errors': 0,
            'stats': {'pending': 0, 'pdf_pending': 0},
        })


        self.assertEqual(sent_payloads[0]['type'], 'queue_done')
        self.assertEqual(sent_payloads[0]['stats']['pending'], 0)
        self.assertEqual(sent_payloads[0]['stats']['pdf_pending'], 0)


    def test_queue_status_forwards_operational_message(self):
        consumer = QueueConsumer()
        sent_payloads = []


        async def fake_send_json(payload):
            sent_payloads.append(payload)


        consumer._send_json = fake_send_json


        async_to_sync(consumer.queue_status)({
            'type': 'queue_status',
            'message': 'Esperando cierre de P02.',
            'badge': 'EN ESPERA',
            'mode': 'waiting',
            'footer': 'Usa Nuevo pallet.',
            'active_pallet_id': 2,
            'active_hu_count': 5,
            'remaining_seconds': 360,
            'stats': {'is_running': True},
        })


        self.assertEqual(sent_payloads[0]['type'], 'queue_status')
        self.assertEqual(sent_payloads[0]['mode'], 'waiting')
        self.assertEqual(sent_payloads[0]['active_hu_count'], 5)


    def test_delete_and_clear_events_forward_sync_payloads(self):
        consumer = QueueConsumer()
        sent_payloads = []


        async def fake_send_json(payload):
            sent_payloads.append(payload)


        consumer._send_json = fake_send_json


        async_to_sync(consumer.hu_deleted)({
            'type': 'hu_deleted',
            'hu_code': 'TH0000268197',
            'pallet_id': 1,
            'pallet_deleted': False,
            'stats': {'total': 0},
        })
        async_to_sync(consumer.pallet_deleted)({
            'type': 'pallet_deleted',
            'pallet_id': 1,
            'stats': {'total': 0},
        })
        async_to_sync(consumer.queue_cleared)({
            'type': 'queue_cleared',
            'pallet_id': 1,
            'stats': {'total': 0},
        })


        self.assertEqual([payload['type'] for payload in sent_payloads], [
            'hu_deleted',
            'pallet_deleted',
            'queue_cleared',
        ])




class SequentialQueueTaskTests(TestCase):
    def test_queue_task_finishes_pallet_print_before_next_pallet(self):
        p1 = Pallet.objects.create(status=Pallet.STATUS_READY)
        p2 = Pallet.objects.create(status=Pallet.STATUS_READY)
        hu1 = HUItem.objects.create(hu_code='T10045916001', pallet=p1)
        hu2 = HUItem.objects.create(hu_code='T10045916002', pallet=p1)
        hu3 = HUItem.objects.create(hu_code='T10045916003', pallet=p2)
        events = []
        sap_sessions = []
        boundary_sessions = []


        def process_item(item_id, **kwargs):
            item = HUItem.objects.get(pk=item_id)
            events.append(('hu', item.pallet_id, item.hu_code))
            sap_sessions.append(kwargs.get('sap_session'))
            item.status = HUItem.STATUS_OK
            item.save(update_fields=['status'])
            return item


        def print_pallet(pallet_id, **kwargs):
            events.append(('print', pallet_id))
            boundary_sessions.append(kwargs.get('sap_worker_session'))
            Pallet.objects.filter(pk=pallet_id).update(status=Pallet.STATUS_DONE)
            return {'status': 'ok', 'message': 'printed', 'marked': 1}


        with (
            patch('queue_app.tasks._acquire_queue_lock', return_value=True),
            patch('queue_app.tasks._release_queue_lock'),
            patch('queue_app.tasks._stop_requested', return_value=False),
            patch('queue_app.tasks._touch_queue_worker_heartbeat'),
            patch('queue_app.tasks._process_hu_item', side_effect=process_item),
            patch('queue_app.tasks._run_pallet_boundary', side_effect=print_pallet),
            patch('queue_app.utils.emit_queue_done'),
        ):
            result = process_queue_task.run()


        self.assertEqual(result['status'], 'ok')
        self.assertEqual(events, [
            ('hu', p1.pk, hu1.hu_code),
            ('hu', p1.pk, hu2.hu_code),
            ('print', p1.pk),
            ('hu', p2.pk, hu3.hu_code),
            ('print', p2.pk),
        ])
        self.assertTrue(all(session is sap_sessions[0] for session in sap_sessions))
        self.assertTrue(all(session is sap_sessions[0] for session in boundary_sessions))


    def test_queue_task_continues_after_pallet_with_hu_errors(self):
        p1 = Pallet.objects.create(status=Pallet.STATUS_READY)
        p2 = Pallet.objects.create(status=Pallet.STATUS_READY)
        hu1 = HUItem.objects.create(hu_code='T10045916001', pallet=p1)
        hu2 = HUItem.objects.create(hu_code='T10045916002', pallet=p2)
        events = []


        def process_item(item_id, **kwargs):
            item = HUItem.objects.get(pk=item_id)
            events.append(('hu', item.pallet_id))
            item.status = HUItem.STATUS_ERROR if item.pallet_id == p1.pk else HUItem.STATUS_OK
            item.save(update_fields=['status'])
            return item


        def print_pallet(pallet_id, **kwargs):
            events.append(('print', pallet_id))
            Pallet.objects.filter(pk=pallet_id).update(status=Pallet.STATUS_DONE)
            return {'status': 'ok', 'message': 'printed', 'marked': 1}


        with (
            patch('queue_app.tasks._acquire_queue_lock', return_value=True),
            patch('queue_app.tasks._release_queue_lock'),
            patch('queue_app.tasks._stop_requested', return_value=False),
            patch('queue_app.tasks._touch_queue_worker_heartbeat'),
            patch('queue_app.tasks._process_hu_item', side_effect=process_item),
            patch('queue_app.tasks._run_pallet_boundary', side_effect=print_pallet),
            patch('queue_app.utils.emit_queue_done'),
        ):
            result = process_queue_task.run()


        self.assertEqual(result['status'], 'error')
        self.assertEqual(result['errors'], 1)
        self.assertEqual(events, [('hu', p1.pk), ('hu', p2.pk), ('print', p2.pk)])


    def test_queue_task_prints_pallet_ready_for_pdf_after_error_hu_deleted(self):
        pallet = Pallet.objects.create(status=Pallet.STATUS_READY)
        HUItem.objects.create(hu_code='T10045916001', pallet=pallet, status=HUItem.STATUS_OK)
        events = []


        def print_pallet(pallet_id, **kwargs):
            events.append(('print', pallet_id))
            Pallet.objects.filter(pk=pallet_id).update(status=Pallet.STATUS_DONE)
            return {'status': 'ok', 'message': 'printed', 'marked': 1}


        with (
            patch('queue_app.tasks._acquire_queue_lock', return_value=True),
            patch('queue_app.tasks._release_queue_lock'),
            patch('queue_app.tasks._stop_requested', return_value=False),
            patch('queue_app.tasks._touch_queue_worker_heartbeat'),
            patch('queue_app.tasks._process_hu_item') as process_item,
            patch('queue_app.tasks._run_pallet_boundary', side_effect=print_pallet),
            patch('queue_app.utils.emit_queue_done'),
        ):
            result = process_queue_task.run()


        self.assertEqual(result['status'], 'ok')
        self.assertEqual(events, [('print', pallet.pk)])
        process_item.assert_not_called()


    def test_continuous_queue_picks_ready_pallet_added_during_processing(self):
        first = Pallet.objects.create(status=Pallet.STATUS_READY)
        first_hu = HUItem.objects.create(hu_code='T10045916001', pallet=first)
        second_holder = {}
        events = []


        def process_item(item_id, **kwargs):
            item = HUItem.objects.get(pk=item_id)
            events.append(('hu', item.pallet_id, item.hu_code))
            item.status = HUItem.STATUS_OK
            item.save(update_fields=['status'])


            if item.pk == first_hu.pk:
                second = Pallet.objects.create(status=Pallet.STATUS_READY)
                second_holder['pallet'] = second
                HUItem.objects.create(hu_code='T10045916002', pallet=second)


            return item


        def print_pallet(pallet_id, **kwargs):
            events.append(('print', pallet_id))
            Pallet.objects.filter(pk=pallet_id).update(status=Pallet.STATUS_DONE)
            return {'status': 'ok', 'message': 'printed', 'marked': 1}


        with (
            patch('queue_app.tasks._acquire_queue_lock', return_value=True),
            patch('queue_app.tasks._release_queue_lock'),
            patch('queue_app.tasks._stop_requested', return_value=False),
            patch('queue_app.tasks._touch_queue_worker_heartbeat'),
            patch('queue_app.tasks._process_hu_item', side_effect=process_item),
            patch('queue_app.tasks._run_pallet_boundary', side_effect=print_pallet),
            patch('queue_app.utils.emit_queue_done'),
        ):
            result = process_queue_task.run(continuous=True, idle_timeout=0)


        second = second_holder['pallet']
        self.assertEqual(result['status'], 'ok')
        self.assertEqual(events, [
            ('hu', first.pk, 'T10045916001'),
            ('print', first.pk),
            ('hu', second.pk, 'T10045916002'),
            ('print', second.pk),
        ])




class ZE16NormalizationTests(TestCase):
    def test_italy_hu_is_left_padded_for_ze16(self):
        client = ZE16Client(session=None)


        self.assertEqual(
            client._normalize_hu('2972200143'),
            '00000000002972200143',
        )
        self.assertEqual(
            client._normalize_hu('00000000002972200143'),
            '00000000002972200143',
        )


    def test_non_italy_hu_is_not_left_padded(self):
        client = ZE16Client(session=None)


        self.assertEqual(client._normalize_hu('TH0000267998'), 'TH0000267998')
        self.assertEqual(client._normalize_hu('ELPS1234567'), 'ELPS1234567')


class ZE16ReceiptRetryTests(TestCase):
    @override_settings(
        SAP_ZE16_RECEIPT_MAX_ATTEMPTS=3,
        SAP_ZE16_RECEIPT_RETRY_DELAY_SECONDS=0,
    )
    def test_empty_receipts_stop_after_configured_attempts(self):
        class EmptyZE16:
            def __init__(self):
                self.calls = 0

            def get_receipts_for_pallet(self, hu_codes):
                self.calls += 1
                return {}

        ze16 = EmptyZE16()

        receipts, attempts = _get_ze16_receipts_with_retries(
            ze16,
            ['C1016958421'],
            pallet_id=2,
            hu_count=1,
        )

        self.assertEqual(receipts, {})
        self.assertEqual(attempts, 3)
        self.assertEqual(ze16.calls, 3)


    def test_pdf_error_pallet_is_not_selected_again_until_reprocessed(self):
        pallet = Pallet.objects.create(
            status=Pallet.STATUS_READY,
            pdf_status=Pallet.PDF_STATUS_ERROR,
            pdf_msg='ZE16: sin Receipt IDs para pallet 2 tras 4 intento(s)',
        )
        HUItem.objects.create(
            hu_code='C1016958421',
            pallet=pallet,
            origin_code='BRA/ATL',
            status=HUItem.STATUS_OK,
        )

        self.assertNotIn(pallet, list(pallets_ready_for_pdf_queryset()))
