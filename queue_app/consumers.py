import json
import logging

from channels.db import database_sync_to_async
from channels.generic.websocket import AsyncWebsocketConsumer

from queue_app.ws.events import QUEUE_UPDATES_GROUP

log = logging.getLogger(__name__)


class QueueConsumer(AsyncWebsocketConsumer):
    """Puente WebSocket entre eventos de backend y la UI del navegador."""

    GROUP_NAME = QUEUE_UPDATES_GROUP

    async def connect(self):
        await self.channel_layer.group_add(self.GROUP_NAME, self.channel_name)
        await self.accept()
        log.info("ws_connected channel=%s", self.channel_name)
        await self._send_initial_state()

    async def disconnect(self, close_code):
        await self.channel_layer.group_discard(self.GROUP_NAME, self.channel_name)
        log.info("ws_disconnected channel=%s code=%s", self.channel_name, close_code)

    async def receive(self, text_data):
        """Atiende comandos simples del navegador que no requieren estado HTTP."""
        try:
            data = json.loads(text_data)
        except json.JSONDecodeError as exc:
            log.warning("ws_receive_invalid_json error=%s", exc)
            return

        if data.get('action') == 'ping':
            await self._send_json({'type': 'pong'})

    async def item_update(self, event):
        await self._send_json({
            'type': 'item_update',
            'hu_code': event['hu_code'],
            'status': event['status'],
            'f1_display': event['f1_display'],
            'f2_display': event['f2_display'],
            'phase1_msg': event['phase1_msg'],
            'phase2_msg': event['phase2_msg'],
            'phase2_ms': event['phase2_ms'],
            'pallet_id': event['pallet_id'],
            'origin_code': event.get('origin_code', '?'),
            'pdf_status': event.get('pdf_status', ''),
            'pdf_display': event.get('pdf_display', ''),
            'pdf_msg': event.get('pdf_msg', ''),
            'pdf_ms': event.get('pdf_ms', 0),
            'processing_time_display': event.get('processing_time_display', ''),
            'processing_started_at': event.get('processing_started_at', ''),
            'processing_finished_at': event.get('processing_finished_at', ''),
            'receipt_done_at': event.get('receipt_done_at', ''),
        })

    async def stats_update(self, event):
        await self._send_json({
            'type': 'stats_update',
            'total': event['total'],
            'ok': event['ok'],
            'errors': event['errors'],
            'pending': event['pending'],
            'pallets': event['pallets'],
            'pallets_total': event.get('pallets_total', 0),
            'pallets_done': event.get('pallets_done', 0),
            'pallets_processing_seconds': event.get('pallets_processing_seconds', 0),
            'pallets_processing_display': event.get('pallets_processing_display', ''),
            'pdf_pending': event.get('pdf_pending', 0),
            'is_running': event.get('is_running', False),
        })

    async def receipt_done(self, event):
        await self._send_json({
            'type': 'receipt_done',
            'pallet_id': event['pallet_id'],
            'status': event['status'],
            'message': event['message'],
            'marked': event['marked'],
            'pdf_status': event.get('pdf_status', event['status']),
            'pdf_display': event.get('pdf_display', ''),
            'pdf_msg': event.get('pdf_msg', event['message']),
            'pdf_ms': event.get('pdf_ms', 0),
            'processing_time_display': event.get('processing_time_display', ''),
            'processing_started_at': event.get('processing_started_at', ''),
            'processing_finished_at': event.get('processing_finished_at', ''),
            'receipt_done_at': event.get('receipt_done_at', ''),
            'stats': event.get('stats', {}),
        })

    async def queue_status(self, event):
        await self._send_json({
            'type': 'queue_status',
            'message': event.get('message', ''),
            'badge': event.get('badge', 'INFO'),
            'mode': event.get('mode', 'running'),
            'footer': event.get('footer', ''),
            'active_pallet_id': event.get('active_pallet_id'),
            'active_hu_count': event.get('active_hu_count', 0),
            'remaining_seconds': event.get('remaining_seconds'),
            'stats': event.get('stats', {}),
        })

    async def queue_done(self, event):
        await self._send_json({
            'type': 'queue_done',
            'status': event['status'],
            'message': event['message'],
            'pallets_processed': event['pallets_processed'],
            'hus_processed': event['hus_processed'],
            'errors': event['errors'],
            'stats': event.get('stats', {}),
        })

    async def pallet_done(self, event):
        await self._send_json({
            'type': 'pallet_done',
            'pallet_id': event['pallet_id'],
            'hu_count': event['hu_count'],
            'processing_time_display': event.get('processing_time_display', ''),
            'processing_started_at': event.get('processing_started_at', ''),
            'processing_finished_at': event.get('processing_finished_at', ''),
            'receipt_done_at': event.get('receipt_done_at', ''),
            'stats': event.get('stats', {}),
        })

    async def pallet_created(self, event):
        await self._send_json({
            'type': 'pallet_created',
            'pallet_id': event['pallet_id'],
            'origin_code': event.get('origin_code', ''),
            'stats': event.get('stats', {}),
        })

    async def hu_deleted(self, event):
        await self._send_json({
            'type': 'hu_deleted',
            'hu_code': event['hu_code'],
            'pallet_id': event['pallet_id'],
            'pallet_deleted': event.get('pallet_deleted', False),
            'stats': event.get('stats', {}),
        })

    async def pallet_deleted(self, event):
        await self._send_json({
            'type': 'pallet_deleted',
            'pallet_id': event['pallet_id'],
            'stats': event.get('stats', {}),
        })

    async def queue_cleared(self, event):
        await self._send_json({
            'type': 'queue_cleared',
            'pallet_id': event['pallet_id'],
            'stats': event.get('stats', {}),
        })

    async def error_message(self, event):
        await self._send_json({
            'type': 'error',
            'message': event['message'],
        })

    async def _send_json(self, payload: dict) -> None:
        await self.send(json.dumps(payload))

    async def _send_initial_state(self):
        """Envia el estado actual de la cola cuando el navegador abre la pagina."""
        state = await self._get_initial_state()
        await self._send_json({
            'type': 'initial_state',
            'items': state['items'],
            'stats': state['stats'],
            'queue_status': state['queue_status'],
        })

    @database_sync_to_async
    def _get_initial_state(self):
        from queue_app.services.snapshot_service import build_queue_snapshot_payload

        return build_queue_snapshot_payload()
