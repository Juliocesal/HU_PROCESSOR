// Utilidades compartidas por la UI de cola. Mantener este archivo sin estado de negocio.
(function () {
  const nativeFetch = window.fetch.bind(window);
  let diagnosticLogger = null;

  function setDiagnosticLogger(logger) {
    diagnosticLogger = typeof logger === 'function' ? logger : null;
  }

  function logDiagnostic(level, source, message, detail = '') {
    if (diagnosticLogger) diagnosticLogger(level, source, message, detail);
  }

  function iconHTML(name, className = 'ui-icon') {
    return `<i data-lucide="${name}" class="${className}" aria-hidden="true"></i>`;
  }

  function escapeHTML(value) {
    return String(value || '').replace(/[&<>"']/g, char => ({
      '&': '&amp;',
      '<': '&lt;',
      '>': '&gt;',
      '"': '&quot;',
      "'": '&#39;',
    }[char]));
  }

  async function fetchWithTimeout(resource, init = {}, timeoutMs = 10000, label = 'Solicitud HTTP') {
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), timeoutMs);
    const started = performance.now();
    const url = typeof resource === 'string' ? resource : resource?.url || label;

    try {
      const response = await nativeFetch(resource, { ...init, signal: controller.signal });
      if (!response.ok) {
        logDiagnostic(
          response.status >= 500 ? 'error' : 'warn',
          'HTTP',
          `${response.status} ${response.statusText || ''}`,
          url
        );
      }
      return response;
    } catch (error) {
      if (error.name === 'AbortError') {
        const seconds = Math.round(timeoutMs / 1000);
        const message = `${label} tardo mas de ${seconds}s. Backend/Daphne no respondio a tiempo.`;
        logDiagnostic('error', 'HTTP_TIMEOUT', message, url);
        throw new Error(message);
      }
      logDiagnostic('error', 'HTTP', 'Fallo de conexion HTTP.', `${url} ${error.message}`);
      throw error;
    } finally {
      clearTimeout(timeoutId);
      const elapsed = performance.now() - started;
      if (elapsed > 5000) {
        logDiagnostic('warn', 'HTTP', `Solicitud lenta (${Math.round(elapsed)} ms).`, url);
      }
    }
  }

  function getCookie(name) {
    let cookieValue = null;
    if (document.cookie && document.cookie !== '') {
      for (const cookie of document.cookie.split(';')) {
        const c = cookie.trim();
        if (c.startsWith(name + '=')) {
          cookieValue = decodeURIComponent(c.slice(name.length + 1));
          break;
        }
      }
    }
    return cookieValue;
  }

  function csrfHeaders(extra = {}) {
    const token = getCookie('csrftoken');
    return token ? { ...extra, 'X-CSRFToken': token } : extra;
  }

  async function readJsonResponse(res) {
    const contentType = res.headers.get('content-type') || '';
    if (contentType.includes('application/json')) {
      const data = await res.json();
      if (!res.ok || data.ok === false) {
        logDiagnostic(
          res.status >= 500 ? 'error' : 'warn',
          'HTTP',
          `Respuesta JSON con alerta (${res.status}).`,
          data.error || data.message || res.url
        );
      }
      return data;
    }

    const text = await res.text();
    const titleMatch = text.match(/<title>(.*)<\/title>/i);
    const title = titleMatch ? titleMatch[1].replace(/\s+/g, ' ').trim() : '';
    logDiagnostic(
      'error',
      'HTTP',
      'Respuesta no JSON cuando se esperaba JSON.',
      `${res.status} ${title || res.url}`
    );
    return {
      ok: false,
      error: title || `Error HTTP ${res.status}`,
      status: res.status,
    };
  }

  window.NexhusQueueShared = {
    nativeFetch,
    setDiagnosticLogger,
    iconHTML,
    escapeHTML,
    fetchWithTimeout,
    getCookie,
    csrfHeaders,
    readJsonResponse,
  };
})();
