import json
import secrets

from django.conf import settings
from django.http import JsonResponse


SERVICE_CONTROL_SESSION_KEY = 'nexhus_service_control_authenticated'


def service_control_is_authenticated(request) -> bool:
    return bool(request.session.get(SERVICE_CONTROL_SESSION_KEY))


def service_control_auth_response(
    request,
    *,
    enabled_func,
    not_configured_message_func,
    authenticated_func=service_control_is_authenticated,
) -> JsonResponse | None:
    if not enabled_func():
        return JsonResponse({
            'ok': False,
            'authenticated': False,
            'enabled': False,
            'error': not_configured_message_func(),
        }, status=403)

    if not authenticated_func(request):
        return JsonResponse({
            'ok': False,
            'authenticated': False,
            'enabled': True,
            'error': 'Login de soporte requerido.',
        }, status=401)

    return None


def service_control_login_response(
    request,
    *,
    enabled_func,
    not_configured_message_func,
    get_services_status_func,
    logger,
) -> JsonResponse:
    """Autentica el panel local de soporte sin mezclarlo con las vistas de cola."""
    if not enabled_func():
        return JsonResponse({
            'ok': False,
            'authenticated': False,
            'enabled': False,
            'error': not_configured_message_func(),
        }, status=403)

    try:
        payload = json.loads(request.body.decode('utf-8') or '{}')
    except (json.JSONDecodeError, UnicodeDecodeError):
        payload = {}

    password = str(payload.get('password', ''))
    expected = str(getattr(settings, 'NEXHUS_SERVICE_CONTROL_PASSWORD', ''))
    if not secrets.compare_digest(password, expected):
        logger.warning("service_control_login_failed remote=%s", request.META.get('REMOTE_ADDR'))
        return JsonResponse({
            'ok': False,
            'authenticated': False,
            'enabled': True,
            'error': 'Clave de soporte incorrecta.',
        }, status=403)

    request.session[SERVICE_CONTROL_SESSION_KEY] = True
    request.session.set_expiry(60 * 30)
    return JsonResponse({
        'ok': True,
        'authenticated': True,
        'enabled': True,
        'services': get_services_status_func(),
    })


def service_control_logout_response(request) -> JsonResponse:
    request.session.pop(SERVICE_CONTROL_SESSION_KEY, None)
    return JsonResponse({'ok': True, 'authenticated': False})


def service_control_status_response(
    request,
    *,
    auth_response_func,
    get_services_status_func,
) -> JsonResponse:
    auth_error = auth_response_func(request)
    if auth_error:
        return auth_error

    return JsonResponse({
        'ok': True,
        'authenticated': True,
        'enabled': True,
        'services': get_services_status_func(),
    })


def service_control_action_response(
    request,
    service: str,
    action: str,
    *,
    auth_response_func,
    execute_service_action_func,
    get_services_status_func,
    logger,
) -> JsonResponse:
    auth_error = auth_response_func(request)
    if auth_error:
        return auth_error

    result = execute_service_action_func(service, action)
    logger.warning(
        "service_control_action service=%s action=%s ok=%s message=%s",
        service,
        action,
        result.ok,
        result.message,
    )
    return JsonResponse({
        **result.as_dict(),
        'authenticated': True,
        'enabled': True,
        'services': get_services_status_func(),
    }, status=200 if result.ok else 400)
