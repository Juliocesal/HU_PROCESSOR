from django.urls import path
from . import consumers

websocket_urlpatterns = [
    path('ws/queue/', consumers.QueueConsumer.as_asgi()),
]