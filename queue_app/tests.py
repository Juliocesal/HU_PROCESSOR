from asgiref.sync import async_to_sync
from django.test import TestCase
from unittest.mock import patch

from core.ze16_client import ZE16Client
from queue_app.consumers import QueueConsumer
from queue_app.models import HUItem, Pallet
from queue_app.tasks import process_queue_task


class ClearQueueTests(TestCase):
    def test_clear_queue_resets_pallet_number_to_one(self):
        for idx in range(3):
            pallet = Pallet.objects.create(status=Pallet.STATUS_ACTIVE)
            HUItem.objects.create(
                hu_code=f'T10045916{idx:03d}',
                pallet=pallet,
                status=HUItem.STATUS_OK,
            )

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


class StartProcessingTests(TestCase):
    def test_start_processing_is_blocked_without_sap_session(self):
        pallet = Pallet.objects.create(status=Pallet.STATUS_ACTIVE)
        HUItem.objects.create(hu_code='T10045916001', pallet=pallet, status=HUItem.STATUS_PENDING)

        with (
            patch('core.sap_client.SAPClient.ensure_session_ready', return_value=(False, '', 'SAP no disponible')),
            patch('queue_app.views.process_queue_task.delay') as delay,
        ):
            response = self.client.post('/api/procesar/', data='{}', content_type='application/json')

        self.assertEqual(response.status_code, 409)
        self.assertFalse(response.json()['ok'])
        delay.assert_not_called()

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

    def test_stop_requests_worker_cancellation(self):
        with patch('queue_app.views.request_queue_stop', return_value=True) as stop:
            response = self.client.post('/cola/detener/')

        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json()['ok'])
        stop.assert_called_once()

    def test_start_processing_allows_pdf_pending_pallet_without_pending_hus(self):
        pallet = Pallet.objects.create(status=Pallet.STATUS_ACTIVE)
        HUItem.objects.create(hu_code='T10045916001', pallet=pallet, status=HUItem.STATUS_OK)

        with (
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
    def test_scan_response_includes_updated_stats_for_current_ui_state(self):
        with (
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
            idle_timeout=30,
        )
        arm.assert_called_once()

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

        def process_item(item_id, **kwargs):
            item = HUItem.objects.get(pk=item_id)
            events.append(('hu', item.pallet_id, item.hu_code))
            item.status = HUItem.STATUS_OK
            item.save(update_fields=['status'])
            return item

        def print_pallet(pallet_id, **kwargs):
            events.append(('print', pallet_id))
            Pallet.objects.filter(pk=pallet_id).update(status=Pallet.STATUS_DONE)
            return {'status': 'ok', 'message': 'printed', 'marked': 1}

        with (
            patch('queue_app.tasks._acquire_queue_lock', return_value=True),
            patch('queue_app.tasks._release_queue_lock'),
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
