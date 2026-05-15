import json
import logging
from channels.generic.websocket import AsyncWebsocketConsumer

log = logging.getLogger(__name__)


class QueueConsumer(AsyncWebsocketConsumer):
    """
    Canal WebSocket por sesión de usuario.
    Reemplaza el Bridge(QObject) + pyqtSignal de queue_window.py.

    Cada browser conectado entra al grupo 'queue_updates'
    y recibe todos los eventos que antes eran señales Qt.
    """

    GROUP_NAME = 'queue_updates'

    # ── Conexión / desconexión ────────────────────────────────────────────────

    async def connect(self):
        await self.channel_layer.group_add(
            self.GROUP_NAME,
            self.channel_name,
        )
        await self.accept()
        log.info(f"ws_connected channel={self.channel_name}")

        # Mandar estado actual al conectarse (equivale a pintar la UI al abrir la app)
        await self._send_initial_state()

    async def disconnect(self, close_code):
        await self.channel_layer.group_discard(
            self.GROUP_NAME,
            self.channel_name,
        )
        log.info(f"ws_disconnected channel={self.channel_name} code={close_code}")

    # ── Mensajes entrantes desde el browser ──────────────────────────────────

    async def receive(self, text_data):
        """
        El browser puede mandar comandos simples vía WebSocket.
        La mayoría de acciones van por HTTP POST (scan, new_pallet, etc.)
        pero pings o acks pueden venir por aquí.
        """
        try:
            data = json.loads(text_data)
            action = data.get('action')
            if action == 'ping':
                await self.send(json.dumps({'type': 'pong'}))
        except Exception as e:
            log.warning(f"ws_receive_error: {e}")

    # ── Handlers de grupo — equivalentes a los slots Qt ──────────────────────
    # Cada método recibe un evento del channel layer y lo reenvía al browser.
    # Nombre del método = tipo del evento (con _ en lugar de .)

    async def item_update(self, event):
        """Equivale a _on_item() en queue_window.py"""
        await self.send(json.dumps({
            'type':       'item_update',
            'hu_code':    event['hu_code'],
            'status':     event['status'],
            'f1_display': event['f1_display'],
            'f2_display': event['f2_display'],
            'phase1_msg': event['phase1_msg'],
            'phase2_msg': event['phase2_msg'],
            'phase2_ms':  event['phase2_ms'],
            'pallet_id':  event['pallet_id'],
        }))

    async def stats_update(self, event):
        """Equivale a _on_stats() en queue_window.py"""
        await self.send(json.dumps({
            'type':    'stats_update',
            'total':   event['total'],
            'ok':      event['ok'],
            'errors':  event['errors'],
            'pending': event['pending'],
            'pallets': event['pallets'],
        }))

    async def sp01_done(self, event):
        """Equivale a _on_sp01() en queue_window.py"""
        await self.send(json.dumps({
            'type':      'sp01_done',
            'pallet_id': event['pallet_id'],
            'status':    event['status'],
            'message':   event['message'],
            'marked':    event['marked'],
        }))

    async def pallet_done(self, event):
        """Equivale a _on_pallet_done() en queue_window.py"""
        await self.send(json.dumps({
            'type':      'pallet_done',
            'pallet_id': event['pallet_id'],
            'hu_count':  event['hu_count'],
        }))

    async def error_message(self, event):
        """Equivale a _on_err() en queue_window.py"""
        await self.send(json.dumps({
            'type':    'error',
            'message': event['message'],
        }))

    # ── Estado inicial al conectar ────────────────────────────────────────────

    async def _send_initial_state(self):
        """
        Cuando el browser abre la página manda el estado actual de la cola.
        Equivale a que la ventana Qt pinte el estado al arrancar.
        """
        from channels.db import database_sync_to_async
        from queue_app.models import HUItem, Pallet

        @database_sync_to_async
        def get_state():
            items = list(HUItem.objects.select_related('pallet').order_by('added_at'))
            pallets = list(Pallet.objects.filter(status=Pallet.STATUS_ACTIVE))

            total   = len(items)
            ok      = sum(1 for i in items if i.status in ('ok', 'duplicate'))
            errors  = sum(1 for i in items if i.status == 'error')
            pending = sum(1 for i in items if i.status == 'pending')

            return {
                'items': [{
                    'hu_code':    i.hu_code,
                    'status':     i.status,
                    'f1_display': i.f1_display,
                    'f2_display': i.f2_display,
                    'phase1_msg': i.phase1_msg,
                    'phase2_msg': i.phase2_msg,
                    'phase2_ms':  i.phase2_ms,
                    'pallet_id':  i.pallet_id,
                    'origin_code': i.origin_code,
                } for i in items],
                'stats': {
                    'total': total, 'ok': ok,
                    'errors': errors, 'pending': pending,
                    'pallets': len(pallets),
                },
            }

        state = await get_state()
        await self.send(json.dumps({
            'type':  'initial_state',
            'items': state['items'],
            'stats': state['stats'],
        }))