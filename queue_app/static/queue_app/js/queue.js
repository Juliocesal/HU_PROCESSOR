  // -- Mapa de orígenes (igual que hu_origins.py) -------------------------------
  const ORIGIN_MAP = {
    'TH':   { code: 'THA',     label: 'Tailandia (THA)',   cls: 'origin-THA'     },
    'T':    { code: 'CNA',     label: 'China (CNA)',        cls: 'origin-CNA'     },
    'C10':  { code: 'BRA/ATL', label: 'ATL (BRA/ATL)',     cls: 'origin-BRA_ATL' },
    '29':   { code: 'ITA',     label: 'Italia (ITA)',       cls: 'origin-ITA',  auto_pallet: true },
    'ELPS': { code: 'FHR',     label: 'Foothill Ranch (ELPS)', cls: 'origin-FHR', auto_pallet: true },
  };

  function detectOrigin(hu_code) {
    const code = hu_code.trim().toUpperCase();
    const prefixes = ['ELPS', 'C10', 'TH', 'T', '29']; // más específico primero
    for (const prefix of prefixes) {
      if (code.startsWith(prefix)) return ORIGIN_MAP[prefix];
    }
    return { code: 'UNK', label: 'Desconocido', cls: 'origin-UNK', auto_pallet: false };
  }

  function isPalletSeparator(val) {
    return val.trim().toUpperCase() === 'PALLET';
  }

  // -- Estado local -------------------------------------------------------------
  let isRunning   = false;
  let flashTimer  = null;
  let stopRequested = false;
  let stopRequestedAt = 0;
  let palletSepRows = {}; // pallet_id -> tr element
  const palletTimers = new Map();
  let palletTimerFrame = null;
  let sessionCloseCountdownTimer = null;
  let websocketConnectionLost = false;
  let lastSapConnected = null;
  let sapStatusCheckRunning = false;
  let sapStatusFailureCount = 0;
  let sapStatusPollTimer = null;
  let sapStatusPollDelayMs = 5000;
  let lastSapStatus = '';
  const _visibleSeps = new Set();
  let _stickyObserver = null;
  const tableWrap = document.getElementById('table-wrap');
  const stickyPallet = document.getElementById('sticky-pallet');
  const QUEUE_TABLE_COLSPAN = 6;
  const EMPTY_QUEUE_MESSAGE = 'Sin HUs en cola - escanea el primer código';
  const SAFE_STOP_GRACE_MS = 30000;

  const initialStats = JSON.parse(
    document.getElementById('initial-stats').textContent || '{}'
  );
  isRunning = Boolean(initialStats.is_running || false);

  function markStopRequested() {
    stopRequested = true;
    if (!stopRequestedAt) stopRequestedAt = Date.now();
  }

  function clearStopRequestState() {
    stopRequested = false;
    stopRequestedAt = 0;
  }

  let stats = {
    total:   Number(initialStats.total || 0),
    ok:      Number(initialStats.ok || 0),
    errors:  Number(initialStats.errors || 0),
    pending: Number(initialStats.pending || 0),
    pallets: Number(initialStats.pallets || 0),
    pallets_total: Number(initialStats.pallets_total || 0),
    pallets_done: Number(initialStats.pallets_done || 0),
    pallets_processing_seconds: Number(initialStats.pallets_processing_seconds || 0),
    pallets_processing_display: initialStats.pallets_processing_display || '',
    pdf_pending: Number(initialStats.pdf_pending || 0)
  };

  function emptyQueueRowHTML() {
    return `
      <tr class="empty-row" id="empty-row">
        <td colspan="${QUEUE_TABLE_COLSPAN}">
          <div class="empty-state">
            ${iconHTML('scan-line')}
            <span>${EMPTY_QUEUE_MESSAGE}</span>
          </div>
        </td>
      </tr>
    `;
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

  // -- Consola de diagnostico ---------------------------------------------------
  const DIAG_MAX_LINES = 500;
  const diagnosticLogs = [];
  let diagnosticAutoscroll = true;
  let diagnosticStatusTimer = null;
  const diagnosticStatusLevels = {};
  let lastSystemStatus = null;
  let serviceControlAuthenticated = false;
  let serviceControlTimer = null;
  let serviceControlBusy = false;
  const SERVICE_ACTION_LABELS = {
    start: 'Prender',
    stop: 'Apagar',
    pause: 'Pausar',
    resume: 'Reanudar',
    restart: 'Reiniciar',
  };
  let lastRelevantWsEventAt = Date.now();
  let lastRelevantWsMode = '';
  let lastNoProgressWarningAt = 0;
  let lastWebSocketCloseLogAt = 0;
  let iconRefreshFrame = null;
  const nativeFetch = window.fetch.bind(window);
  const FETCH_TIMEOUTS = {
    startProcess: 15000,
    reprocessQueue: 15000,
    clearQueue: 10000,
    stopProcess: 10000,
    scanPastedLine: 10000,
    queueSnapshot: 8000,
    checkSAP: 4000,
    serviceControl: 8000,
  };
  const NO_PROGRESS_WARNING_MS = 45000;
  const NO_PROGRESS_WARNING_COOLDOWN_MS = 60000;
  const SAP_STATUS_BASE_POLL_MS = 5000;
  const SAP_STATUS_MAX_POLL_MS = 60000;
  const SCAN_OUTBOX_STORAGE_KEY = 'nexhus.scanOutbox.v1';
  const UI_STATUS_STORAGE_KEY = 'nexhus.uiStatus.v1';
  const SCAN_OUTBOX_MAX_ITEMS = 1000;
  const SCAN_BATCH_MAX_ITEMS = 100;
  const SCAN_RETRY_BASE_MS = 2000;
  const SCAN_RETRY_MAX_MS = 30000;
  const RELEVANT_WS_EVENTS = new Set([
    'item_update',
    'queue_status',
    'receipt_done',
    'queue_done',
    'error',
  ]);
  let wsResyncPending = false;
  let snapshotRequestRunning = false;
  let scanOutbox = loadScanOutbox();
  let scanOutboxProcessing = false;
  let scanOutboxTimer = null;
  let scanOutboxHadFailures = false;

  function sanitizeDiagnosticText(value) {
    return String(value ?? '')
      .replace(/((password|passwd|pwd|pass|bcode|token|secret)\s*[:=]\s*)[^&\s,;}"']+/ig, '$1***')
      .slice(0, 700);
  }

  function diagnosticTimestamp() {
    return new Date().toLocaleTimeString('es-MX', { hour12: false });
  }

  async function fetchWithTimeout(resource, init = {}, timeoutMs = 10000, label = 'Solicitud HTTP') {
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), timeoutMs);
    const started = performance.now();
    const url = typeof resource === 'string' ? resource : resource?.url || label;

    try {
      const response = await nativeFetch(resource, { ...init, signal: controller.signal });
      if (!response.ok) {
        addDiagnosticLog(
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
        const message = `${label} tardó más de ${seconds}s. Backend/Daphne no respondió a tiempo.`;
        addDiagnosticLog('error', 'HTTP_TIMEOUT', message, url);
        throw new Error(message);
      }
      addDiagnosticLog('error', 'HTTP', 'Fallo de conexión HTTP.', `${url} ${error.message}`);
      throw error;
    } finally {
      clearTimeout(timeoutId);
      const elapsed = performance.now() - started;
      if (elapsed > 5000) {
        addDiagnosticLog('warn', 'HTTP', `Solicitud lenta (${Math.round(elapsed)} ms).`, url);
      }
    }
  }

  function addDiagnosticLog(level, source, message, detail = '') {
    const now = Date.now();
    const entry = {
      ts: diagnosticTimestamp(),
      at: now,
      level: String(level || 'info').toLowerCase(),
      source: sanitizeDiagnosticText(source || 'UI'),
      message: sanitizeDiagnosticText(message),
      detail: sanitizeDiagnosticText(detail),
    };

    const previous = diagnosticLogs[diagnosticLogs.length - 1];
    if (
      previous &&
      previous.level === entry.level &&
      previous.source === entry.source &&
      previous.message === entry.message &&
      previous.detail === entry.detail &&
      now - (previous.at || 0) < 15000
    ) {
      return;
    }

    diagnosticLogs.push(entry);
    if (diagnosticLogs.length > DIAG_MAX_LINES) diagnosticLogs.shift();
    renderDiagnosticConsole();
  }

  function renderDiagnosticConsole() {
    const consoleEl = document.getElementById('diag-console');
    if (!consoleEl) return;

    consoleEl.innerHTML = diagnosticLogs.map(entry => {
      const level = ['ok', 'warn', 'error', 'info'].includes(entry.level) ? entry.level : 'info';
      const detail = entry.detail ? ` ${escapeHTML(entry.detail)}` : '';
      return (
        `<span class="diag-line-time">[${escapeHTML(entry.ts)}]</span> ` +
        `<span class="diag-line-${level}">${escapeHTML(level.toUpperCase()).padEnd(5, ' ')}</span> ` +
        `<span class="diag-line-source">${escapeHTML(entry.source)}</span> ` +
        `${escapeHTML(entry.message)}${detail}`
      );
    }).join('\n');

    if (diagnosticAutoscroll) {
      consoleEl.scrollTop = consoleEl.scrollHeight;
    }
  }

  function setDiagnosticMenuState(level = 'info', title = 'Estado de diagnóstico pendiente') {
    const dot = document.getElementById('diag-menu-dot');
    if (!dot) return;

    dot.classList.remove('ok', 'warn', 'error', 'info');
    dot.classList.add(['ok', 'warn', 'error', 'info'].includes(level) ? level : 'info');
    dot.title = title;
  }

  function worstDiagnosticLevel(levels) {
    const rank = { error: 3, warn: 2, ok: 1, info: 0 };
    return levels.reduce((worst, level) => (
      (rank[level] || 0) > (rank[worst] || 0) ? level : worst
    ), 'info');
  }

  function serviceLevel(status, warnWhenUnavailable = false) {
    if (!status) return 'info';
    if (['ok', 'warn', 'error', 'info'].includes(status.level)) return status.level;
    if (status.ok || status.connected) return 'ok';
    return warnWhenUnavailable ? 'warn' : 'error';
  }

  function diagnosticLevelLabel(level) {
    return {
      ok: 'OK',
      warn: 'WARN',
      error: 'ERROR',
      info: 'INFO',
    }[level] || 'INFO';
  }

  function diagnosticStatusItems(status) {
    const queue = status?.queue || {};
    const queueLevel = hasOrphanedQueueRuntime(status)
      ? 'error'
      : (queue.is_locked ? 'warn' : 'ok');
    const workerAge = queue.worker_heartbeat_age_seconds;

    return [
      {
        key: 'django',
        label: 'Django/Daphne',
        level: serviceLevel(status?.django),
        message: status?.django?.message || 'Sin consulta del backend.',
        detail: status?.django?.action || '',
      },
      {
        key: 'redis',
        label: 'Redis',
        level: serviceLevel(status?.redis),
        message: status?.redis?.message || 'Sin estado Redis.',
        detail: status?.redis?.error || status?.redis?.action || '',
      },
      {
        key: 'celery',
        label: 'Celery',
        level: serviceLevel(status?.celery),
        message: status?.celery?.message || 'Sin estado Celery.',
        detail: (status?.celery?.workers || []).join(', ') || status?.celery?.action || '',
      },
      {
        key: 'database',
        label: 'Base de datos',
        level: serviceLevel(status?.database),
        message: status?.database?.message || 'Sin estado DB.',
        detail: status?.database?.error || status?.database?.action || '',
      },
      {
        key: 'sap',
        label: 'SAP',
        level: serviceLevel(status?.sap, true),
        message: status?.sap?.message || 'Sin estado SAP.',
        detail: status?.sap?.user ? `user=${status.sap.user}` : (status?.sap?.error || status?.sap?.action || ''),
      },
      {
        key: 'websocket',
        label: 'WebSocket',
        level: websocketConnectionLost ? 'error' : 'ok',
        message: websocketConnectionLost ? 'Desconectado o reintentando.' : 'Canal de eventos activo.',
        detail: websocketConnectionLost ? 'Revisar Daphne/ASGI/puerto.' : '',
      },
      {
        key: 'queue',
        label: 'Cola',
        level: queueLevel,
        message: `Lock ${queue.is_locked ? 'activo' : 'libre'} · Pendientes ${queue.pending_hus || 0} · Processing ${queue.processing_hus || 0}`,
        detail: [
          queue.ready_pallets ? `ready=${queue.ready_pallets}` : '',
          queue.pdf_pending ? `pdf=${queue.pdf_pending}` : '',
          workerAge !== null && workerAge !== undefined ? `heartbeat=${workerAge}s` : '',
        ].filter(Boolean).join(' · '),
      },
    ];
  }

  function logDiagnosticStatusChanges(status, { manual = false } = {}) {
    const items = diagnosticStatusItems(status);

    items.forEach(item => {
      const previousLevel = diagnosticStatusLevels[item.key];
      diagnosticStatusLevels[item.key] = item.level;

      if (!previousLevel && ['warn', 'error'].includes(item.level)) {
        addDiagnosticLog(item.level, item.label.toUpperCase(), item.message, item.detail);
        return;
      }

      if (previousLevel && previousLevel !== item.level) {
        addDiagnosticLog(
          item.level,
          item.label.toUpperCase(),
          `Estado cambio de ${diagnosticLevelLabel(previousLevel)} a ${diagnosticLevelLabel(item.level)}.`,
          item.message
        );
      }
    });

    if (manual) {
      const health = diagnosticHealthFromStatus(status);
      addDiagnosticLog('info', 'SYSTEM', 'Estado actualizado manualmente.', health.title);
    }
  }

  function isSystemStatusPayload(data) {
    return Boolean(
      data &&
      data.django &&
      data.redis &&
      data.celery &&
      data.database &&
      data.sap &&
      data.queue
    );
  }

  function hasOrphanedQueueRuntime(status = lastSystemStatus) {
    return Boolean(
      status?.queue?.is_locked &&
      status.queue.worker_stale &&
      status.celery &&
      !status.celery.ok
    );
  }

  function diagnosticHealthFromStatus(status) {
    if (!status) {
      return {
        level: websocketConnectionLost ? 'error' : 'info',
        title: websocketConnectionLost
          ? 'WebSocket desconectado. Revisar Daphne/ASGI/puerto.'
          : 'Diagnostico pendiente.',
      };
    }

    const levels = [
      serviceLevel(status.django),
      serviceLevel(status.redis),
      serviceLevel(status.celery),
      serviceLevel(status.database),
      serviceLevel(status.sap, true),
      websocketConnectionLost ? 'error' : 'ok',
    ];
    const level = worstDiagnosticLevel(levels);
    const titleByLevel = {
      ok: 'Diagnostico OK.',
      warn: 'Diagnostico con advertencias. Revisar SAP/cola.',
      error: 'Diagnostico con errores criticos. Revisar consola.',
      info: 'Diagnostico pendiente.',
    };
    return { level, title: titleByLevel[level] || titleByLevel.info };
  }

  function updateDiagnosticMenuFromStatus(status = lastSystemStatus) {
    const health = diagnosticHealthFromStatus(status);
    setDiagnosticMenuState(health.level, health.title);
  }

  function setDiagnosticSummary(status) {
    const summary = document.getElementById('diag-summary');
    const statusGrid = document.getElementById('diag-status-grid');
    const updated = document.getElementById('diag-status-updated');
    if (!summary) return;

    const pill = (label, level = 'info') => {
      const cls = ['ok', 'warn', 'error', 'info'].includes(level) ? level : 'info';
      return `<span class="diag-pill ${cls}">${escapeHTML(label)}</span>`;
    };

    const health = diagnosticHealthFromStatus(status);
    const queue = status.queue || {};
    summary.innerHTML = [
      pill(`General: ${diagnosticLevelLabel(health.level)}`, health.level),
      pill(`Lock: ${queue.is_locked ? 'activo' : 'libre'}`, queue.is_locked ? 'warn' : 'ok'),
      pill(`Pendientes: ${Number(queue.pending_hus || 0)}`, 'ok'),
      pill(`Processing: ${Number(queue.processing_hus || 0)}`, queue.processing_hus ? 'warn' : 'ok'),
    ].join('');

    if (statusGrid) {
      statusGrid.innerHTML = diagnosticStatusItems(status).map(item => {
        const level = ['ok', 'warn', 'error', 'info'].includes(item.level) ? item.level : 'info';
        const detail = item.detail ? `<small title="${escapeHTML(item.detail)}">${escapeHTML(item.detail)}</small>` : '';
        return (
          `<div class="diag-status-card ${level}">` +
          `<span class="diag-status-name">${escapeHTML(item.label)}</span>` +
          `<strong>${diagnosticLevelLabel(level)}</strong>` +
          `<small title="${escapeHTML(item.message)}">${escapeHTML(item.message)}</small>` +
          detail +
          `</div>`
        );
      }).join('');
    }

    if (updated) {
      updated.textContent = `Actualizado ${diagnosticTimestamp()}`;
    }

    updateDiagnosticMenuFromStatus(status);
  }

  async function refreshSystemStatus(options = {}) {
    const manual = Boolean(options.manual);

    try {
      const res = await nativeFetch('/api/system-status/', {
        headers: csrfHeaders({ 'Accept': 'application/json' }),
      });
      const data = await readJsonResponse(res);

      if (!isSystemStatusPayload(data)) {
        addDiagnosticLog(
          'error',
          'SYSTEM',
          data.error || 'No se recibió un payload válido de /api/system-status/.',
          'No se actualizan estados de servicios para evitar información falsa.'
        );
        setDiagnosticMenuState('error', 'system-status no disponible. Revisar Django/Daphne/puerto.');
        return;
      }

      lastSystemStatus = data;
      setDiagnosticSummary(data);
      logDiagnosticStatusChanges(data, { manual });

      const queue = data.queue || {};
      if (manual && queue.last_operational_status?.message) {
        addDiagnosticLog('info', 'QUEUE_STATUS', queue.last_operational_status.message);
      }

      if (queue.stop_requested && isRunning) {
        stopRequested = true;
        if (!stopRequestedAt) {
          const ageMs = Number(queue.stop_request_age_seconds || 0) * 1000;
          stopRequestedAt = Date.now() - ageMs;
        }
        const age = Number(queue.stop_request_age_seconds || 0);
        if (age >= SAFE_STOP_GRACE_MS / 1000) {
          setFooterStatus('La detención segura no confirmó cierre. Presiona Detener para liberar la cola.', 'error');
        }
        updateButtons();
      }

      if (hasOrphanedQueueRuntime(data)) {
        if (!diagnosticStatusLevels.queue_orphan_alerted) {
          addDiagnosticLog(
            'error',
            'QUEUE',
            'Lock de cola huérfano: Celery no responde y el heartbeat del worker venció.',
            'Presiona Detener para liberar la UI y revisar HUs en processing.'
          );
          diagnosticStatusLevels.queue_orphan_alerted = true;
        }
        clearStopRequestState();
        setProgBadge('CELERY', 'error');
        setProgStatus('Celery se detuvo o fue cerrado mientras la cola estaba activa.', 'error');
        setFooterStatus('Presiona Detener para liberar el lock y revisar la cola.', 'error');
        document.getElementById('prog-bar').classList.remove('animated', 'waiting', 'done');
        document.getElementById('prog-bar').classList.add('error');
        updateButtons();
      } else {
        diagnosticStatusLevels.queue_orphan_alerted = false;
      }
    } catch (error) {
      addDiagnosticLog('error', 'SYSTEM', 'No se pudo consultar /api/system-status/.', error.message);
      setDiagnosticMenuState('error', 'No se pudo consultar Django/Daphne. Revisar backend/puerto.');
    }

    refreshIcons();
  }

  function openDiagnosticConsole() {
    const panel = document.getElementById('diag-panel');
    if (!panel) return;
    panel.classList.add('open');
    panel.setAttribute('aria-hidden', 'false');
    addDiagnosticLog('info', 'UI', 'Consola de diagnóstico abierta.');
    refreshSystemStatus();
    clearInterval(diagnosticStatusTimer);
    diagnosticStatusTimer = setInterval(() => refreshSystemStatus({ silent: true }), 15000);
    refreshIcons();
  }

  function closeDiagnosticConsole() {
    const panel = document.getElementById('diag-panel');
    if (!panel) return;
    panel.classList.remove('open');
    panel.setAttribute('aria-hidden', 'true');
    const servicePanel = document.getElementById('svc-panel');
    const windowEl = panel.querySelector('.diag-window');
    if (servicePanel) servicePanel.hidden = true;
    if (windowEl) windowEl.classList.remove('services-open');
    clearInterval(diagnosticStatusTimer);
    diagnosticStatusTimer = null;
    clearInterval(serviceControlTimer);
    serviceControlTimer = null;
  }

  function clearDiagnosticConsole() {
    diagnosticLogs.length = 0;
    renderDiagnosticConsole();
    addDiagnosticLog('info', 'UI', 'Consola limpiada.');
  }

  function toggleDiagnosticAutoscroll() {
    diagnosticAutoscroll = !diagnosticAutoscroll;
    const btn = document.getElementById('diag-pause');
    if (btn) {
      btn.querySelector('span').textContent = diagnosticAutoscroll ? 'Pausar' : 'Seguir';
    }
    addDiagnosticLog('info', 'UI', diagnosticAutoscroll ? 'Auto-scroll activado.' : 'Auto-scroll pausado.');
  }

  async function copyDiagnosticLogs() {
    const text = buildSupportReport();

    try {
      await navigator.clipboard.writeText(text);
      addDiagnosticLog('ok', 'UI', 'Reporte de soporte copiado al portapapeles.');
      flash('Reporte de soporte copiado.', 'green');
    } catch (error) {
      addDiagnosticLog('error', 'UI', 'No se pudieron copiar los logs.', error.message);
      flash('No se pudieron copiar los logs.', 'orange');
    }
  }

  function formatServiceForReport(label, service, extra = '') {
    if (!service) return `${label}: SIN DATOS`;
    const level = serviceLevel(service).toUpperCase();
    const state = level === 'INFO' ? ((service.ok || service.connected) ? 'OK' : 'SIN DATOS') : level;
    const parts = [
      `${label}: ${state}`,
      service.message || '',
      service.action ? `accion=${service.action}` : '',
      service.error ? `error=${service.error}` : '',
      extra,
    ].filter(Boolean);
    return parts.join(' | ');
  }

  function buildSupportReport() {
    const status = lastSystemStatus || {};
    const queue = status.queue || {};
    const recentLogs = diagnosticLogs.slice(-80).map(entry => (
      `[${entry.ts}] ${entry.level.toUpperCase()} ${entry.source} ${entry.message}${entry.detail ? ' ' + entry.detail : ''}`
    ));

    return [
      'NEXHUS - REPORTE DE SOPORTE',
      `Generado: ${new Date().toLocaleString('es-MX', { hour12: false })}`,
      `Ultimo system-status: ${status.timestamp || 'sin consulta'}`,
      formatServiceForReport('Django/Daphne', status.django),
      formatServiceForReport('Redis', status.redis),
      formatServiceForReport('Celery', status.celery, status.celery?.workers?.length ? `workers=${status.celery.workers.join(',')}` : ''),
      formatServiceForReport('DB', status.database, status.database?.engine ? `engine=${status.database.engine}` : ''),
      formatServiceForReport('SAP', status.sap, status.sap?.user ? `user=${status.sap.user}` : ''),
      `WebSocket: ${websocketConnectionLost ? 'DESCONECTADO' : 'OK'}`,
      `Queue: lock=${queue.is_locked ? 'activo' : 'libre'} pending=${queue.pending_hus || 0} processing=${queue.processing_hus || 0} ready_pallets=${queue.ready_pallets || 0} pdf_pending=${queue.pdf_pending || 0}`,
      `UI: isRunning=${isRunning} stopRequested=${stopRequested} lastWsMode=${lastRelevantWsMode || 'n/a'} lastWsEvent=${Math.round((Date.now() - lastRelevantWsEventAt) / 1000)}s`,
      '',
      'Ultimos logs visibles:',
      recentLogs.length ? recentLogs.join('\n') : 'Sin logs visibles.',
    ].join('\n');
  }

  function toggleServiceControlPanel() {
    const panel = document.getElementById('svc-panel');
    const windowEl = document.querySelector('.diag-window');
    if (!panel) return;

    const willOpen = panel.hidden;
    panel.hidden = !willOpen;
    if (windowEl) windowEl.classList.toggle('services-open', willOpen);
    if (willOpen) {
      addDiagnosticLog('info', 'SERVICES', 'Panel de servicios abierto.');
      refreshServiceControlStatus({ silentAuth: true });
      serviceControlTimer = setInterval(
        () => refreshServiceControlStatus({ silent: true, silentAuth: true }),
        10000
      );
    } else {
      clearInterval(serviceControlTimer);
      serviceControlTimer = null;
    }
    refreshIcons();
  }

  function renderServiceControlAuth(message = '') {
    const login = document.getElementById('svc-login');
    const dashboard = document.getElementById('svc-dashboard');
    const grid = document.getElementById('svc-grid');
    const updated = document.getElementById('svc-updated');

    serviceControlAuthenticated = false;
    if (login) login.hidden = false;
    if (dashboard) dashboard.hidden = true;
    if (grid) grid.innerHTML = '';
    if (updated) updated.textContent = 'Sin consulta';
    if (message) addDiagnosticLog('warn', 'SERVICES', message);
    refreshIcons();
  }

  function renderServiceControlDashboard(services = {}) {
    const login = document.getElementById('svc-login');
    const dashboard = document.getElementById('svc-dashboard');
    const grid = document.getElementById('svc-grid');
    const updated = document.getElementById('svc-updated');

    serviceControlAuthenticated = true;
    if (login) login.hidden = true;
    if (dashboard) dashboard.hidden = false;
    if (updated) updated.textContent = `Actualizado ${diagnosticTimestamp()}`;
    if (!grid) return;

    const ordered = ['daphne', 'redis', 'celery']
      .map(key => services[key])
      .filter(Boolean);

    grid.innerHTML = ordered.map(service => {
      const level = ['ok', 'warn', 'error'].includes(service.level) ? service.level : 'info';
      const actions = service.actions || {};
      const buttons = ['start', 'stop', 'pause', 'resume', 'restart'].filter(action => (
        Boolean(actions[action])
      )).map(action => (
        `<button type="button" class="svc-action-btn" ` +
        `onclick="serviceControlAction('${escapeHTML(service.key)}','${action}')">` +
        `${SERVICE_ACTION_LABELS[action] || action}</button>`
      )).join('');

      const actionHTML = buttons || '<span class="svc-no-actions">Sin acciones disponibles</span>';

      return `
        <article class="svc-card ${level}">
          <div class="svc-card-title">
            <strong>${escapeHTML(service.label || service.key)}</strong>
            <span class="svc-state">${escapeHTML(service.state || 'unknown')}</span>
          </div>
          <p>${escapeHTML(service.message || 'Sin estado.')}</p>
          <small title="${escapeHTML(service.detail || '')}">${escapeHTML(service.detail || '')}</small>
          <div class="svc-card-actions">${actionHTML}</div>
        </article>
      `;
    }).join('');

    refreshIcons();
  }

  async function loginServiceControl() {
    const input = document.getElementById('svc-password');
    const password = input ? input.value : '';
    if (!password) {
      flash('Ingresa la clave de soporte.', 'orange');
      return;
    }

    try {
      const res = await fetchWithTimeout('/api/service-control/login/', {
        method: 'POST',
        headers: csrfHeaders({ 'Content-Type': 'application/json' }),
        body: JSON.stringify({ password }),
      }, FETCH_TIMEOUTS.serviceControl, 'Login servicios');
      const data = await readJsonResponse(res);

      if (!data.ok) {
        renderServiceControlAuth(data.error || 'No se pudo autenticar.');
        flash(data.error || 'Login de soporte rechazado.', 'orange');
        return;
      }

      if (input) input.value = '';
      addDiagnosticLog('ok', 'SERVICES', 'Login de soporte aceptado.');
      renderServiceControlDashboard(data.services || {});
    } catch (error) {
      renderServiceControlAuth('No se pudo conectar al control de servicios.');
      flash(error.message, 'red');
    }
  }

  async function logoutServiceControl() {
    try {
      await fetchWithTimeout('/api/service-control/logout/', {
        method: 'POST',
        headers: csrfHeaders(),
      }, FETCH_TIMEOUTS.serviceControl, 'Logout servicios');
    } catch (error) {
      addDiagnosticLog('warn', 'SERVICES', 'Logout local con error HTTP.', error.message);
    }
    renderServiceControlAuth();
  }

  async function refreshServiceControlStatus(options = {}) {
    const panel = document.getElementById('svc-panel');
    if (!panel || panel.hidden || serviceControlBusy) return;

    try {
      const res = await fetchWithTimeout('/api/service-control/status/', {
        headers: csrfHeaders({ 'Accept': 'application/json' }),
      }, FETCH_TIMEOUTS.serviceControl, 'Estado servicios');
      const data = await readJsonResponse(res);

      if (!data.ok) {
        if (!options.silentAuth) {
          renderServiceControlAuth(data.error || 'Login de soporte requerido.');
        }
        return;
      }

      renderServiceControlDashboard(data.services || {});
      if (!options.silent) addDiagnosticLog('info', 'SERVICES', 'Estado de servicios actualizado.');
    } catch (error) {
      addDiagnosticLog('error', 'SERVICES', 'No se pudo consultar servicios.', error.message);
    }
  }

  async function serviceControlAction(service, action) {
    if (serviceControlBusy) return;
    serviceControlBusy = true;
    addDiagnosticLog('warn', 'SERVICES', `Accion solicitada: ${service}.${action}`);

    try {
      const res = await fetchWithTimeout(`/api/service-control/${service}/${action}/`, {
        method: 'POST',
        headers: csrfHeaders({ 'Content-Type': 'application/json' }),
        body: JSON.stringify({}),
      }, FETCH_TIMEOUTS.serviceControl, 'Accion servicio');
      const data = await readJsonResponse(res);

      if (data.services) renderServiceControlDashboard(data.services);
      addDiagnosticLog(data.ok ? 'ok' : 'warn', 'SERVICES', data.message || 'Accion finalizada.', data.action || '');
      flash(data.message || 'Accion de servicio finalizada.', data.ok ? 'green' : 'orange');
    } catch (error) {
      addDiagnosticLog('error', 'SERVICES', 'Accion de servicio fallida.', error.message);
      flash(error.message, 'red');
    } finally {
      serviceControlBusy = false;
      setTimeout(() => refreshServiceControlStatus({ silent: true }), 2500);
    }
  }

  window.fetch = async (resource, init) => {
    const started = performance.now();
    const url = typeof resource === 'string' ? resource : resource?.url || 'fetch';

    try {
      const response = await nativeFetch(resource, init);
      if (!response.ok) {
        addDiagnosticLog(
          response.status >= 500 ? 'error' : 'warn',
          'HTTP',
          `${response.status} ${response.statusText || ''}`,
          url
        );
      }
      return response;
    } catch (error) {
        addDiagnosticLog('error', 'HTTP', 'Fallo de conexión HTTP.', `${url} ${error.message}`);
      throw error;
    } finally {
      const elapsed = performance.now() - started;
      if (elapsed > 5000) {
        addDiagnosticLog('warn', 'HTTP', `Solicitud lenta (${Math.round(elapsed)} ms).`, url);
      }
    }
  };

  function refreshIcons() {
    if (!window.lucide) return;
    if (!document.querySelector('[data-lucide]')) return;
    if (iconRefreshFrame) return;

    iconRefreshFrame = requestAnimationFrame(() => {
      iconRefreshFrame = null;
      if (!window.lucide || !document.querySelector('[data-lucide]')) return;
      window.lucide.createIcons({ attrs: { 'stroke-width': 2 } });
    });
  }

  function palletHeaderHTML(palletId, originCode, count, processingTime = '') {
    const pid = String(palletId).padStart(2, '0');
    const origin = originCode || 'Sin origen';
    const huLabel = `${count} HU${count !== 1 ? 's' : ''}`;
    const timeHTML = processingTime
      ? `<span class="pallet-time" data-pallet-timer-label="${palletId}">${processingTime}</span>`
      : '';

    return `
      <div class="pallet-header">
        <span class="pallet-title">Pallet P${pid}</span>
        <span class="origin-badge pallet-origin">${origin}</span>
        <span class="pallet-hu-count">${huLabel}</span>
        ${timeHTML}
      </div>
    `;
  }

  function parseServerDate(value) {
    if (!value) return null;
    const normalized = String(value).includes('T')
      ? String(value)
      : String(value).replace(' ', 'T');
    const date = new Date(normalized);
    return Number.isNaN(date.getTime()) ? null : date;
  }

  function formatElapsedFromMs(ms) {
    const totalSeconds = Math.max(0, Math.floor(ms / 1000));
    if (totalSeconds < 60) return `${totalSeconds}s`;

    const minutes = Math.floor(totalSeconds / 60);
    const seconds = totalSeconds % 60;
    return seconds === 0 ? `${minutes}min` : `${minutes}min ${seconds}s`;
  }

  function formatCountdown(seconds) {
    const safeSeconds = Math.max(0, Math.ceil(Number(seconds || 0)));
    const minutes = Math.floor(safeSeconds / 60);
    const rest = safeSeconds % 60;
    return `${String(minutes).padStart(2, '0')}:${String(rest).padStart(2, '0')}`;
  }

  function clearSessionCloseCountdown() {
    if (!sessionCloseCountdownTimer) return;
    clearInterval(sessionCloseCountdownTimer);
    sessionCloseCountdownTimer = null;
  }

  function startSessionCloseCountdown(msg) {
    clearSessionCloseCountdown();
    const initialSeconds = Number(msg.remaining_seconds || 0);
    if (!initialSeconds || initialSeconds <= 0) return;

    const endsAt = Date.now() + initialSeconds * 1000;
    const baseMessage = msg.message || 'Worker en espera operativa.';
    const baseFooter = msg.footer || 'Esperando siguiente pallet.';

    const render = () => {
      const remaining = Math.max(0, Math.ceil((endsAt - Date.now()) / 1000));
      const countdown = formatCountdown(remaining);
      setProgStatus(`${baseMessage} SAP se cerrara en ${countdown} si no cierras otro pallet.`, 'waiting');
      setFooterStatus(`${baseFooter} Cierre automático de SAP en ${countdown}.`, 'waiting');
      if (remaining <= 0) clearSessionCloseCountdown();
    };

    render();
    sessionCloseCountdownTimer = setInterval(render, 1000);
  }

  function ensurePalletTimerLoop() {
    if (palletTimerFrame) return;

    const tick = () => {
      palletTimerFrame = null;
      const now = Date.now();

      palletTimers.forEach((timer, palletId) => {
        const endMs = timer.endMs || now;
        const text = formatElapsedFromMs(endMs - timer.startMs);
        if (text === timer.lastText) return;

        timer.lastText = text;
        document
          .querySelectorAll(`[data-pallet-timer-label="${palletId}"]`)
          .forEach(el => { el.textContent = text; });

        const sep = palletSepRows[palletId] ||
          document.querySelector(`tr.pallet-sep[data-pallet-id="${palletId}"]`);
        if (sep) sep.dataset.processingTime = text;
      });

      if ([...palletTimers.values()].some(timer => !timer.endMs)) {
        palletTimerFrame = requestAnimationFrame(tick);
      }
    };

    palletTimerFrame = requestAnimationFrame(tick);
  }

  function stopPalletTimerLoopIfIdle() {
    if ([...palletTimers.values()].some(timer => !timer.endMs)) return;
    if (!palletTimerFrame) return;

    cancelAnimationFrame(palletTimerFrame);
    palletTimerFrame = null;
  }

  function removePalletTimer(palletId) {
    palletTimers.delete(String(palletId));
    stopPalletTimerLoopIfIdle();
  }

  function clearPalletTimers() {
    palletTimers.clear();
    if (palletTimerFrame) {
      cancelAnimationFrame(palletTimerFrame);
      palletTimerFrame = null;
    }
  }

  function syncPalletTimer(palletId, startedAt, receiptDoneAt, displayValue = '') {
    const start = parseServerDate(startedAt);
    if (!start) return;

    const done = parseServerDate(receiptDoneAt);
    palletTimers.set(String(palletId), {
      startMs: start.getTime(),
      endMs: done ? done.getTime() : null,
      lastText: '',
    });

    setPalletProcessingTime(palletId, displayValue || formatElapsedFromMs((done || new Date()) - start));
    ensurePalletTimerLoop();
  }

  // -- Reloj ---------------------------------------------------------------------
  function tickClock() {
    document.getElementById('clock').textContent =
      new Date().toTimeString().slice(0, 8);
  }
  setInterval(tickClock, 1000);
  tickClock();

  // -- WebSocket con reconexión automática ---------------------------------------
  let ws;
  let wsRetry = 0;

  function logWebSocketDiagnostic(msg) {
    if (!msg || !msg.type) return;

    if (msg.type === 'queue_status') {
      const level = msg.mode === 'error' ? 'error' : (msg.mode === 'waiting' ? 'warn' : 'info');
      addDiagnosticLog(level, msg.badge || 'QUEUE', msg.message || 'Actualización de cola.', msg.footer || '');
      return;
    }

    if (msg.type === 'item_update') {
      if (msg.status === 'processing') {
        addDiagnosticLog('info', 'HU', `Procesando ${msg.hu_code}.`, `pallet=P${String(msg.pallet_id).padStart(2, '0')}`);
      } else if (msg.status === 'error' || msg.status === 'hu_not_found') {
        addDiagnosticLog('error', 'HU', `Error en ${msg.hu_code}.`, msg.phase1_msg || msg.phase2_msg || '');
      }
      return;
    }

    if (msg.type === 'receipt_done') {
      addDiagnosticLog(
        msg.status === 'ok' ? 'ok' : 'error',
        'PDF',
        `Pallet P${String(msg.pallet_id).padStart(2, '0')}: ${msg.message || 'receipt_done'}`,
        msg.pdf_ms ? `${msg.pdf_ms}ms` : ''
      );
      return;
    }

    if (msg.type === 'queue_done') {
      addDiagnosticLog(
        msg.status === 'ok' ? 'ok' : (msg.status === 'stopped' ? 'warn' : 'error'),
        'QUEUE',
        msg.message || 'Cola finalizada.',
        `hus=${msg.hus_processed || 0} errors=${msg.errors || 0}`
      );
      return;
    }

    if (msg.type === 'error') {
      addDiagnosticLog('error', 'BACKEND', msg.message || 'Error recibido por WebSocket.');
    }
  }

  function markRelevantWebSocketEvent(msg) {
    if (!msg || !RELEVANT_WS_EVENTS.has(msg.type)) return;

    lastRelevantWsEventAt = Date.now();
    lastRelevantWsMode = msg.type === 'queue_status' ? String(msg.mode || '') : '';
  }

  function checkProcessingProgressSilence() {
    const controlledWaiting = lastRelevantWsMode === 'waiting' && !hasOrphanedQueueRuntime();
    if (!isRunning || controlledWaiting) return;

    const elapsed = Date.now() - lastRelevantWsEventAt;
    if (elapsed < NO_PROGRESS_WARNING_MS) return;

    if (Date.now() - lastNoProgressWarningAt < NO_PROGRESS_WARNING_COOLDOWN_MS) return;
    lastNoProgressWarningAt = Date.now();

    addDiagnosticLog(
      'warn',
      'WATCHDOG',
      'No hay eventos nuevos desde hace 45s.',
      'Posible Celery detenido, SAP bloqueado o WebSocket sin eventos.'
    );
    setDiagnosticMenuState('warn', 'Proceso sin eventos recientes. Revisar consola de diagnóstico.');
    resyncQueueUI({ source: 'watchdog', silent: true, notify: false });
  }

  function connectWS() {
    const proto = location.protocol === 'https:' ? 'wss' : 'ws';
    ws = new WebSocket(`${proto}://${location.host}/ws/queue/`);

    ws.onopen = () => {
      const wasReconnecting = websocketConnectionLost || wsRetry > 0;
      console.log('WS conectado');
      websocketConnectionLost = false;
      wsRetry = 0;
      addDiagnosticLog('ok', 'WEBSOCKET', 'Conectado a /ws/queue/.');
      updateDiagnosticMenuFromStatus();
      if (wasReconnecting) {
        wsResyncPending = true;
        setFooterStatus('Conexion en vivo restablecida.', isRunning ? 'running' : 'done');
        addDiagnosticLog('info', 'SYNC', 'WebSocket reconectado. Esperando snapshot inicial.');
        setTimeout(() => {
          if (wsResyncPending) {
            resyncQueueUI({ source: 'ws_reconnect_fallback', notify: true });
          }
        }, 2500);
      }
    };

    ws.onclose = () => {
      websocketConnectionLost = true;
      const delay = Math.min(1000 * 2 ** wsRetry, 30000); // max 30s
      wsRetry++;
      console.warn(`WS cerrado. Reintentando en ${delay/1000}s...`);
      const now = Date.now();
      if (wsRetry <= 2 || now - lastWebSocketCloseLogAt > 30000) {
        addDiagnosticLog('error', 'WEBSOCKET', 'Conexión cerrada. Reintentando...', `delay=${delay / 1000}s. Revisar Daphne/ASGI/puerto.`);
        lastWebSocketCloseLogAt = now;
      }
      setDiagnosticMenuState('error', 'WebSocket desconectado. Revisar Daphne/ASGI/puerto.');
      setFooterStatus(`Actualizaciones en vivo desconectadas. Reintentando en ${delay / 1000}s.`, 'waiting');
      setTimeout(connectWS, delay);
    };

    ws.onerror = e => {
      console.error('WS error', e);
      addDiagnosticLog('error', 'WEBSOCKET', 'Error de WebSocket.', 'Revisar Daphne/ASGI/puerto.');
      setDiagnosticMenuState('error', 'WebSocket con error. Revisar Daphne/ASGI/puerto.');
      setFooterStatus('Error de WebSocket. Esperando reconexión automática.', 'waiting');
    };

    ws.onmessage = (e) => {
      let msg;
      try {
        msg = JSON.parse(e.data);
      } catch (error) {
        addDiagnosticLog('error', 'WEBSOCKET', 'Mensaje WebSocket inválido.', error.message);
        return;
      }
      markRelevantWebSocketEvent(msg);
      logWebSocketDiagnostic(msg);
      switch (msg.type) {
        case 'initial_state': handleInitialState(msg); break;
        case 'item_update':   handleItemUpdate(msg);   break;
        case 'stats_update':  handleStatsUpdate(msg);  break;
        case 'receipt_done':  handleReceiptDone(msg);  break;
        case 'pallet_created': handlePalletCreated(msg); break;
        case 'hu_deleted':    handleHuDeleted(msg);    break;
        case 'pallet_deleted': handlePalletDeleted(msg); break;
        case 'queue_cleared': handleQueueCleared(msg); break;
        case 'queue_status':  handleQueueStatus(msg);  break;
        case 'queue_done':    handleQueueDone(msg);    break;
        case 'pallet_done':   handlePalletDone(msg);   break;
        case 'error':         handleError(msg);         break;
      }
    };
  }

  connectWS();
  setInterval(checkProcessingProgressSilence, 5000);

  // -- Handlers WebSocket --------------------------------------------------------

  function snapshotImpliesRunning(snapshot) {
    const snapshotStats = snapshot?.stats || {};
    if (Object.prototype.hasOwnProperty.call(snapshotStats, 'is_running')) {
      return Boolean(snapshotStats.is_running);
    }

    const queueStatus = snapshot?.queue_status || {};
    const mode = String(queueStatus.mode || '');
    return Boolean(mode && !['done', 'idle', 'error', 'stopped'].includes(mode));
  }

  function clearTableForSnapshot() {
    const tbody = document.getElementById('queue-tbody');
    tbody.innerHTML = '';
    palletSepRows = {};
    _visibleSeps.clear();
    palletTimers.clear();
    if (stickyPallet) stickyPallet.classList.remove('visible');
  }

  function applyIdleSnapshotStatus(snapshotStats = {}) {
    clearSessionCloseCountdown();
    clearStopRequestState();

    if (scanOutbox.length) {
      updateScanOutboxStatus();
      return;
    }

    if (applyLocalUiStatus()) return;

    const total = Number(snapshotStats.total || 0);
    const pending = Number(snapshotStats.pending || 0);
    const pdfPending = Number(snapshotStats.pdf_pending || 0);
    const errors = Number(snapshotStats.errors || 0);
    const ok = Number(snapshotStats.ok || 0);
    const bar = document.getElementById('prog-bar');

    bar.classList.remove('animated', 'waiting', 'done', 'error');

    if (!total) {
      setProgBadge('EN ESPERA', '');
      setProgStatus('En espera.', '');
      setFooterStatus('En espera.', '');
      return;
    }

    if (pending || pdfPending) {
      setProgBadge('EN ESPERA', 'waiting');
      setProgStatus('Hay trabajo pendiente listo para procesar.', 'waiting');
      setFooterStatus('UI sincronizada. Puedes iniciar el proceso.', 'waiting');
      bar.classList.add('waiting');
      return;
    }

    if (errors) {
      setProgBadge('Completado con errores', 'error');
      setProgStatus('Cola finalizada con errores.', 'error');
      setFooterStatus('UI sincronizada con backend.', 'error');
      bar.classList.add('error');
      return;
    }

    if (ok) {
      setProgBadge('Completado', 'done');
      setProgStatus('Cola finalizada correctamente.', 'done');
      setFooterStatus('UI sincronizada con backend.', 'done');
      bar.classList.add('done');
    }
  }

  function applyQueueSnapshot(snapshot, options = {}) {
    const source = options.source || 'snapshot';
    const items = Array.isArray(snapshot?.items) ? snapshot.items : [];
    const snapshotStats = snapshot?.stats || {};
    const queueStatus = snapshot?.queue_status || null;
    const snapshotRunning = snapshotImpliesRunning(snapshot);

    if (!snapshotRunning) {
      clearTableForSnapshot();
    } else {
      const empty = document.getElementById('empty-row');
      if (empty && items.length) empty.remove();
    }

    const seenHuCodes = new Set();
    items.forEach(item => {
      if (!item || !item.hu_code) return;
      seenHuCodes.add(String(item.hu_code));
      updateOrCreateRow(item);
    });

    if (!snapshotRunning) {
      document.querySelectorAll('#queue-tbody tr[data-hu]').forEach(row => {
        if (!seenHuCodes.has(String(row.dataset.hu || ''))) row.remove();
      });
      document.querySelectorAll('tr.pallet-sep').forEach(sep => {
        const palletId = sep.dataset.palletId;
        if (!document.querySelector(`tr[data-pallet="${palletId}"]`)) {
          sep.remove();
          delete palletSepRows[palletId];
          removePalletTimer(palletId);
        }
      });
    }

    if (!items.length) ensureEmptyQueueMessage();

    handleStatsUpdate(snapshotStats);
    if (queueStatus) {
      handleQueueStatus({ ...queueStatus, stats: snapshotStats });
    } else if (!snapshotRunning) {
      isRunning = false;
      applyIdleSnapshotStatus(snapshotStats);
      updateButtons();
    } else {
      isRunning = true;
      showRunningMode('Proceso activo. UI sincronizada con backend.');
      updateButtons();
    }

    updateStickyPallet();
    refreshIcons();
    lastRelevantWsEventAt = Date.now();

    addDiagnosticLog('ok', 'SYNC', 'Snapshot aplicado correctamente.', `source=${source} items=${items.length}`);
    if (options.notify) {
      flash('UI resincronizada desde backend.', 'green');
    }
  }

  async function resyncQueueUI(options = {}) {
    if (snapshotRequestRunning) return;
    snapshotRequestRunning = true;

    const source = options.source || 'manual';
    addDiagnosticLog('info', 'SYNC', 'Snapshot solicitado.', `source=${source}`);
    if (!options.silent) {
      setFooterStatus('Resincronizando UI desde backend...', 'waiting');
      rememberLocalUiStatus({
        badge: 'SYNC',
        mode: 'waiting',
        message: 'Resincronizando UI desde backend...',
        footer: 'Consultando estado actual de la cola.',
        ttlMs: 15000,
      });
    }

    try {
      const res = await fetchWithTimeout(
        '/api/queue-snapshot/',
        {},
        FETCH_TIMEOUTS.queueSnapshot,
        'Snapshot de cola'
      );
      const data = await readJsonResponse(res);
      if (data.ok === false) {
        throw new Error(data.error || 'Snapshot rechazado por backend.');
      }
      applyQueueSnapshot(data, {
        source,
        notify: options.notify !== false && !options.silent,
      });
      if (!scanOutbox.length) clearLocalUiStatus();
    } catch (error) {
      addDiagnosticLog('error', 'SYNC', 'Snapshot fallido.', error.message);
      if (!options.silent) {
        setFooterStatus('No se pudo resincronizar la UI.', 'error');
        flash(`Error al resincronizar: ${error.message}`, 'red');
      }
    } finally {
      snapshotRequestRunning = false;
      updateButtons();
    }
  }

  async function manualResyncQueueUI() {
    await resyncQueueUI({ source: 'manual', notify: true });
  }

  function handleInitialState(msg) {
    const wasReconnectSnapshot = wsResyncPending;
    wsResyncPending = false;
    applyQueueSnapshot(msg, {
      source: wasReconnectSnapshot ? 'ws_reconnect' : 'ws_initial',
      notify: wasReconnectSnapshot,
    });
  }

  function handleItemUpdate(msg) {
    clearSessionCloseCountdown();
    updateOrCreateRow(msg);
    updateProgress();
    refreshIcons();

    if (msg.status === 'processing') {
      setProgBadge('PROCESANDO', 'running');
      setProgStatus(`Procesando HU ${msg.hu_code} en Pallet P${String(msg.pallet_id).padStart(2, '0')}`, 'running');
      setFooterStatus(`Procesando HU ${msg.hu_code}...`, 'running');
    } else if ((msg.status === 'ok' || msg.status === 'duplicate') && isRunning) {
      setProgStatus(`HU ${msg.hu_code} completada. Continuando con el pallet.`, 'running');
      setFooterStatus(`HU ${msg.hu_code} OK.`, 'running');
    } else if (msg.status === 'error' || msg.status === 'hu_not_found') {
      const detail = (msg.phase1_msg || msg.phase2_msg || 'Sin detalles').slice(0, 60);
      setProgStatus(`ERROR en HU ${msg.hu_code}: ${detail}`, 'error');
      setFooterStatus(`Error:  Error en ${msg.hu_code}`, 'error');
    }
  }

  function handleStatsUpdate(msg) {
    if (Object.prototype.hasOwnProperty.call(msg, 'is_running')) {
      isRunning = Boolean(msg.is_running);
      if (!isRunning) clearStopRequestState();
    }

    stats = {
      total:   Number(msg.total || 0),
      ok:      Number(msg.ok || 0),
      errors:  Number(msg.errors || 0),
      pending: Number(msg.pending || 0),
      pallets: Number(msg.pallets || 0),
      pallets_total: Number(msg.pallets_total || 0),
      pallets_done: Number(msg.pallets_done || 0),
      pallets_processing_seconds: Number(msg.pallets_processing_seconds || 0),
      pallets_processing_display: msg.pallets_processing_display || '',
      pdf_pending: Number(msg.pdf_pending || 0)
    };

    const kpiTotal = document.getElementById('kpi-total');
    const kpiOk    = document.getElementById('kpi-ok');
    const kpiErr   = document.getElementById('kpi-err');
    const kpiPend  = document.getElementById('kpi-pend');

    if (kpiTotal) kpiTotal.textContent = stats.total;
    if (kpiOk)    kpiOk.textContent    = stats.ok;
    if (kpiErr)   kpiErr.textContent   = stats.errors;
    if (kpiPend)  kpiPend.textContent  = stats.pending;

    const palLbl = document.getElementById('pal-lbl');
    if (palLbl) palLbl.textContent = palletProgressText(stats);

    const errCard = document.querySelector('.kpi-card.kpi-err');
    if (errCard) errCard.classList.toggle('has-errors', stats.errors > 0);

    updateProgress();
    updateButtons();
  }

  function showRunningMode(message) {
    clearSessionCloseCountdown();
    setProgBadge('PROCESANDO', 'running');
    setProgStatus(message || 'Proceso activo.', 'running');
    setFooterStatus('Procesando...', 'running');
    const bar = document.getElementById('prog-bar');
    bar.classList.add('animated');
    bar.classList.remove('done', 'error', 'waiting');
  }

  function showSapLoginMode(message) {
    clearSessionCloseCountdown();
    setProgBadge('SAP', 'running');
    setProgStatus(message || 'Validando sesión SAP e iniciando sesión si es necesario...', 'running');
    setFooterStatus('Validando SAP. Si la sesión está cerrada, se abrirá automáticamente.', 'running');
    const bar = document.getElementById('prog-bar');
    bar.classList.add('animated');
    bar.classList.remove('done', 'error', 'waiting');
  }

  function handleAutoStartResult(data) {
    if (data.auto_started) {
      clearStopRequestState();
      isRunning = true;
      showRunningMode('Pallet cerrado. Proceso reactivado automáticamente.');
      setFooterStatus('El worker continuo tomara el nuevo pallet listo.', 'running');
      updateButtons();
      flash('Proceso reactivado automáticamente.', 'green');
      return;
    }

    if (data.auto_start_error) {
      setProgBadge('SAP', 'error');
      setProgStatus(`Pallet listo, pero no se pudo reactivar: ${data.auto_start_error}`, 'error');
      setFooterStatus('Corrige SAP y vuelve a iniciar el proceso.', 'error');
      flash(`Pallet listo. No se pudo reactivar: ${data.auto_start_error}`, 'orange');
    }
  }

  function handleReceiptDone(msg) {
    updatePalletPdfCells(msg);
    if (msg.stats) handleStatsUpdate(msg.stats);
    if (msg.status !== 'ok') {
      setProgBadge('REVISION', 'error');
      setProgStatus(`ZE16/PDF: ${msg.message}`, 'error');
      setFooterStatus(`ZE16/PDF: ${msg.message}`, 'error');
    } else if (isRunning) {
      setProgBadge('PDF OK', 'done');
      setProgStatus(`Recibo de Pallet P${String(msg.pallet_id).padStart(2, '0')} impreso. Buscando siguiente pallet.`, 'done');
      setFooterStatus(`Pallet P${String(msg.pallet_id).padStart(2, '0')} impreso correctamente.`, 'done');
    }
    updateButtons();
  }

  function handlePalletCreated(msg) {
    ensurePalletSep(msg.pallet_id, msg.origin_code || 'Sin origen');
    updatePalletSep(msg.pallet_id);
    if (msg.stats) handleStatsUpdate(msg.stats);
    updateStickyPallet();
    refreshIcons();
  }

  function handleHuDeleted(msg) {
    const row = document.getElementById(`row-${msg.hu_code}`);
    if (row) row.remove();

    if (msg.pallet_deleted) {
      removePalletRows(msg.pallet_id);
    } else {
      updatePalletSep(msg.pallet_id);
    }

    if (msg.stats) handleStatsUpdate(msg.stats);
    updateStickyPallet();
    ensureEmptyQueueMessage();
    updateButtons();
  }

  function handlePalletDeleted(msg) {
    removePalletRows(msg.pallet_id);
    if (msg.stats) handleStatsUpdate(msg.stats);
    updateStickyPallet();
    ensureEmptyQueueMessage();
    updateButtons();
  }

  function handleQueueCleared(msg) {
    clearSessionCloseCountdown();
    clearStopRequestState();
    const tbody = document.getElementById('queue-tbody');
    tbody.innerHTML = emptyQueueRowHTML();
    Object.keys(palletSepRows).forEach(k => delete palletSepRows[k]);
    _visibleSeps.clear();
    clearPalletTimers();
    clearLocalUiStatus();
    if (stickyPallet) stickyPallet.classList.remove('visible');
    isRunning = false;
    handleStatsUpdate(msg.stats || {
      total:0, ok:0, errors:0, pending:0, pallets:0,
      pallets_total:0, pallets_done:0,
      pallets_processing_seconds:0, pallets_processing_display:'0s',
      pdf_pending:0,
    });
    setProgBadge('EN ESPERA', '');
    setProgStatus('Cola limpiada. Listo para escanear.', '');
    setFooterStatus('En espera.', '');
    document.getElementById('prog-bar').classList.remove('animated','done','error','waiting');
    refreshIcons();
  }

  function handleQueueStatus(msg) {
    if (msg.stats) handleStatsUpdate(msg.stats);
    const mode = msg.mode || 'running';
    if (msg.stats && Object.prototype.hasOwnProperty.call(msg.stats, 'is_running')) {
      isRunning = Boolean(msg.stats.is_running);
    } else {
      isRunning = !['done', 'idle', 'error', 'stopped'].includes(mode);
    }
    setProgBadge(msg.badge || 'INFO', mode);
    setProgStatus(msg.message || 'Proceso activo.', mode);
    setFooterStatus(msg.footer || msg.message || 'Proceso activo.', mode);

    const bar = document.getElementById('prog-bar');
    bar.classList.remove('done', 'error', 'waiting');
    if (mode === 'waiting') {
      bar.classList.remove('animated');
      bar.classList.add('waiting');
      startSessionCloseCountdown(msg);
    } else if (mode === 'error') {
      clearSessionCloseCountdown();
      bar.classList.remove('animated');
      bar.classList.add('error');
    } else {
      clearSessionCloseCountdown();
      bar.classList.add('animated');
    }
    updateButtons();
  }

  function handleQueueDone(msg) {
    clearSessionCloseCountdown();
    clearStopRequestState();
    if (msg.stats) {
      handleStatsUpdate({ ...msg.stats, is_running: false });
    } else {
      stats.pending = 0;
      stats.pdf_pending = 0;
    }
    isRunning = false;

    if (msg.status === 'stopped') {
      setProgStatus(msg.message, '');
      setProgBadge('Detenido', '');
      document.getElementById('prog-bar').classList.remove('animated', 'waiting');
      setFooterStatus(msg.message, '');
      updateButtons();
      flash(msg.message, 'orange');
      return;
    }
    const hasErrors = msg.status !== 'ok' || Number(msg.errors || stats.errors || 0) > 0;
    setProgStatus(msg.message, hasErrors ? 'error' : 'done');
    setProgBadge(hasErrors ? 'Completado con errores' : 'Completado', hasErrors ? 'error' : 'done');
    document.getElementById('prog-bar').classList.remove('animated', 'waiting');
    document.getElementById('prog-bar').classList.toggle('done', !hasErrors);
    document.getElementById('prog-bar').classList.toggle('error', hasErrors);
    setFooterStatus(msg.message, hasErrors ? 'error' : 'done');
    updateButtons();
    flash(msg.message, hasErrors ? 'orange' : 'green');
  }

  function handlePalletDone(msg) {
    if (msg.processing_time_display || msg.processing_started_at) {
      syncPalletTimer(
        msg.pallet_id,
        msg.processing_started_at,
        msg.processing_finished_at || msg.receipt_done_at,
        msg.processing_time_display
      );
    }
    updatePalletSep(msg.pallet_id);
    if (msg.stats) handleStatsUpdate(msg.stats);
    if (isRunning) {
      const timerText = msg.processing_time_display ? ` en ${msg.processing_time_display}` : '';
      setProgStatus(`Pallet P${String(msg.pallet_id).padStart(2, '0')} terminado${timerText}. Preparando siguiente paso.`, 'running');
      setFooterStatus(`${palletProgressText()} · Pallet P${String(msg.pallet_id).padStart(2, '0')} terminado${timerText}.`, 'running');
    }
  }

  function handleError(msg) {
    clearSessionCloseCountdown();
    clearStopRequestState();
    isRunning = false;
    setProgBadge('Error: ERROR', 'error');
    document.getElementById('prog-bar').classList.remove('animated', 'waiting');
    document.getElementById('prog-bar').classList.add('error');
    setFooterStatus('Error:  Error en el proceso.', 'error');
    updateButtons();
    flash(`Error SAP: ${msg.message}`, 'red');
  }

  // -- Tabla ---------------------------------------------------------------------

  function originBadgeHTML(origin_code) {
    const safe = (origin_code || 'UNK').replace('/', '_');
    const cls  = `origin-${safe}`;
    return `<span class="origin-badge ${cls}">${origin_code || ''}</span>`;
  }

  function canDeleteStatus(status) {
    return !isRunning && (status === 'pending' || status === 'error' || status === 'hu_not_found');
  }

  function statusCellHTML(status, f1_display, f2_display, phase2_msg, col) {
    const display = col === 'f1' ? f1_display : f2_display;
    const labels = { pending:'Pendiente', processing:'Procesando...' };
    const statusIcon = {
      pending: 'clock-3',
      processing: 'loader-circle',
      ok: 'circle-check',
      duplicate: 'badge-check',
      error: 'triangle-alert',
      hu_not_found: 'circle-help',
    }[status] || 'circle';

    if (col === 'f1') {
      if (status === 'ok' || status === 'duplicate')
        return `<span class="ok-cell">${iconHTML(statusIcon)}${display || 'OK'}</span>`;
      if (status === 'error' || status === 'hu_not_found')
        return `<span class="error-cell">${iconHTML(statusIcon)}${display || 'Error'}</span>`;
      return `<span class="cell-status status-${status}">${iconHTML(statusIcon)}${display || labels[status] || status}</span>`;
    }

    // F2
    if (status === 'ok')
      return `<span class="ok-cell">${iconHTML('circle-check')}${display || 'OK'}</span>`;
    if (status === 'error' && !phase2_msg)
      return `<span class="muted-cell">${iconHTML('circle-minus')}omitido</span>`;
    if (status === 'error')
      return `<span class="error-cell">${iconHTML('triangle-alert')}${display || 'Error'}</span>`;
    return `<span class="cell-status status-${status}">${iconHTML(statusIcon)}${display || labels[status] || 'Pendiente'}</span>`;
  }

  function pdfCellHTML(status, pdfStatus = '', pdfDisplay = '', pdfMsg = '') {
    if (pdfStatus === 'ok') {
      return `<span class="ok-cell">${iconHTML('circle-check')}${escapeHTML(pdfDisplay || 'OK')}</span>`;
    }

    if (pdfStatus === 'error') {
      return `<span class="error-cell">${iconHTML('triangle-alert')}${escapeHTML(pdfMsg || pdfDisplay || 'Error PDF')}</span>`;
    }

    if (status === 'error' || status === 'hu_not_found') {
      return `<span class="muted-cell">${iconHTML('circle-minus')}omitido</span>`;
    }

    return `<span class="cell-status status-pending">${iconHTML('clock-3')}Pendiente</span>`;
  }

  function placeRowUnderPallet(row, palletId) {
    const tbody = document.getElementById('queue-tbody');
    const normalizedPalletId = String(palletId);
    const palletRows = Array
      .from(tbody.querySelectorAll(`tr[data-pallet="${normalizedPalletId}"]`))
      .filter(candidate => candidate !== row);

    if (palletRows.length) {
      palletRows[palletRows.length - 1].after(row);
      return;
    }

    const sep = palletSepRows[normalizedPalletId] ||
      document.querySelector(`tr.pallet-sep[data-pallet-id="${normalizedPalletId}"]`);
    if (sep) sep.after(row);
    else tbody.appendChild(row);
  }

  function updateOrCreateRow(item) {
    const existing = document.getElementById(`row-${item.hu_code}`);
    const normalizedPalletId = String(item.pallet_id);

    if (!existing) {
      // Quitar fila vacía
      const empty = document.getElementById('empty-row');
      if (empty) empty.remove();

      // Asegurar separador de pallet
      ensurePalletSep(item.pallet_id, item.origin_code);

      const tr = document.createElement('tr');
      tr.id = `row-${item.hu_code}`;
      tr.dataset.hu     = item.hu_code;
      tr.dataset.pallet = item.pallet_id;
      tr.dataset.status = item.status;
      tr.innerHTML = buildRowHTML(item);
      document.getElementById('queue-tbody').appendChild(tr);
      placeRowUnderPallet(tr, normalizedPalletId);
      tr.scrollIntoView({ block: 'nearest' });
    } else {
      const previousPalletId = String(existing.dataset.pallet || '');
      existing.dataset.status = item.status;
      existing.dataset.pallet = item.pallet_id;
      if (existing.cells[0]) {
        existing.cells[0].textContent = `P${String(item.pallet_id).padStart(2, '0')}`;
      }
      existing.cells[1].innerHTML = originBadgeHTML(item.origin_code);
      existing.cells[3].innerHTML = statusCellHTML(item.status, item.f1_display, item.f2_display, item.phase2_msg, 'f1');
      existing.cells[4].innerHTML = statusCellHTML(item.status, item.f1_display, item.f2_display, item.phase2_msg, 'f2');
      existing.cells[5].innerHTML = pdfCellHTML(item.status, item.pdf_status, item.pdf_display, item.pdf_msg);
      if (previousPalletId !== normalizedPalletId) {
        ensurePalletSep(item.pallet_id, item.origin_code);
        placeRowUnderPallet(existing, normalizedPalletId);
        updatePalletSep(previousPalletId);
      }
    }

    syncPalletTimer(
      item.pallet_id,
      item.processing_started_at,
      item.processing_finished_at || item.receipt_done_at,
      item.processing_time_display
    );
    updatePalletSep(item.pallet_id);
    refreshIcons();
  }

  function setPalletProcessingTime(palletId, processingTime) {
    if (!processingTime) return;

    const sep = palletSepRows[palletId] ||
      document.querySelector(`tr.pallet-sep[data-pallet-id="${palletId}"]`);
    if (sep) sep.dataset.processingTime = processingTime;
  }

  function updatePalletPdfCells(msg) {
    const rows = document.querySelectorAll(`tr[data-pallet="${msg.pallet_id}"]`);
    rows.forEach(row => {
      if (!row.cells[5]) return;
      row.cells[5].innerHTML = pdfCellHTML(
        row.dataset.status || '',
        msg.pdf_status || msg.status,
        msg.pdf_display || '',
        msg.pdf_msg || msg.message || ''
      );
    });
    if (msg.processing_time_display || msg.processing_started_at) {
      syncPalletTimer(
        msg.pallet_id,
        msg.processing_started_at,
        msg.processing_finished_at || msg.receipt_done_at,
        msg.processing_time_display
      );
      updatePalletSep(msg.pallet_id);
    }
    refreshIcons();
  }

  function ensurePalletSep(palletId, originCode) {
    if (palletSepRows[palletId]) return;
    const tbody = document.getElementById('queue-tbody');
    const sep = document.createElement('tr');
    sep.className = 'pallet-sep';
    sep.dataset.palletId = palletId;
    sep.dataset.origin = originCode || 'Sin origen';
    sep.dataset.processingTime = '';
    sep.innerHTML = `<td colspan="${QUEUE_TABLE_COLSPAN}" id="sep-${palletId}">${palletHeaderHTML(palletId, originCode || 'Sin origen', 0)}</td>`;
    palletSepRows[palletId] = sep;
    tbody.appendChild(sep);
    _observeSep(sep);
    refreshIcons();
  }

  function updatePalletSep(palletId) {
    const sep = palletSepRows[palletId] ||
      document.querySelector(`tr.pallet-sep[data-pallet-id="${palletId}"]`);
    if (!sep) return;
    if (!palletSepRows[palletId]) palletSepRows[palletId] = sep;

    const rows = document.querySelectorAll(`tr[data-pallet="${palletId}"]`);
    const count = rows.length;
    const td = document.getElementById(`sep-${palletId}`) || sep.querySelector('td');
    if (!td) return;

    // Recoger info de origin del primer row
    let originCode = sep.dataset.origin || 'Sin origen';
    if (rows.length > 0) {
      const firstBadge = rows[0].querySelector('.origin-badge');
      if (firstBadge) originCode = firstBadge.textContent.trim();
      sep.dataset.origin = originCode;
    }

    const processingTime = sep.dataset.processingTime || '';
    td.innerHTML = palletHeaderHTML(palletId, originCode, count, processingTime);
    refreshIcons();
  }

  // -- Sticky pallet header (igual que PyQt _update_sticky_pallet) ---------------
  if (tableWrap) tableWrap.addEventListener('scroll', updateStickyPallet);

  function updateStickyPallet() {
    if (!tableWrap || !stickyPallet) return;
    const wrapTop = tableWrap.getBoundingClientRect().top;
    let lastHiddenSep = null;

    for (const [pid, sep] of Object.entries(palletSepRows)) {
      const rect = sep.getBoundingClientRect();
      if (rect.top < wrapTop) {
        lastHiddenSep = { pid: parseInt(pid), sep };
      }
    }

    // También verificar separadores renderizados en Django
    document.querySelectorAll('tr.pallet-sep').forEach(sep => {
      const pid = parseInt(sep.dataset.palletId);
      const rect = sep.getBoundingClientRect();
      if (rect.top < wrapTop) {
        if (!lastHiddenSep || pid > lastHiddenSep.pid) {
          lastHiddenSep = { pid, sep };
        }
      }
      if (!palletSepRows[pid]) palletSepRows[pid] = sep;
    });

    if (lastHiddenSep) {
      const rows = document.querySelectorAll(`tr[data-pallet="${lastHiddenSep.pid}"]`);
      const count = rows.length;
      const originCode = lastHiddenSep.sep.dataset.origin || 'Sin origen';
      const processingTime = lastHiddenSep.sep.dataset.processingTime || '';
      stickyPallet.innerHTML = palletHeaderHTML(lastHiddenSep.pid, originCode, count, processingTime);
      stickyPallet.classList.add('visible');
      refreshIcons();
    } else {
      stickyPallet.classList.remove('visible');
    }
  }

  //  Progress ------------------------------------------------------------------
  function updateProgress() {
    const done  = stats.ok + stats.errors;
    const total = stats.total;
    const pct   = total > 0 ? Math.round(done / total * 100) : 0;
    const bar   = document.getElementById('prog-bar');
    bar.style.width = `${pct}%`;
    document.getElementById('prog-pct').textContent = `${pct}%`;
    if (total === 0) bar.classList.remove('animated', 'done', 'error', 'waiting');
  }

  function setProgBadge(text, mode) {
    const b = document.getElementById('prog-badge');
    b.textContent = text;
    b.className = 'prog-badge ' + (mode || '');
  }

  function setProgStatus(text, mode) {
    const s = document.getElementById('prog-status');
    s.textContent = text;
    const colors = {
      running: 'var(--blue)',
      waiting: 'var(--orange)',
      error: 'var(--red)',
      done: 'var(--green-text)',
    };
    s.style.color      = colors[mode] || 'var(--muted)';
    s.style.fontWeight = mode ? 'bold' : 'normal';
  }

  function setFooterStatus(text, mode) {
    const el = document.getElementById('footer-status');
    el.textContent = text;
    el.className = 'footer-status ' + (mode || '');
  }

  function palletProgressText(currentStats = stats) {
    const done = Number(currentStats.pallets_done || 0);
    const total = Number(currentStats.pallets_total || 0);
    const duration = currentStats.pallets_processing_display || formatElapsedFromMs(
      Number(currentStats.pallets_processing_seconds || 0) * 1000
    );
    return `Pallets: ${done}/${total} · Tiempo total: ${duration}`;
  }

  function rememberLocalUiStatus(payload = {}) {
    try {
      localStorage.setItem(UI_STATUS_STORAGE_KEY, JSON.stringify({
        badge: payload.badge || 'INFO',
        mode: payload.mode || '',
        message: payload.message || '',
        footer: payload.footer || payload.message || '',
        expiresAt: Date.now() + Number(payload.ttlMs || 30000),
      }));
    } catch (error) {
      // El estado visual local es auxiliar; si falla no debe afectar escaneo.
    }
  }

  function clearLocalUiStatus() {
    try {
      localStorage.removeItem(UI_STATUS_STORAGE_KEY);
    } catch (error) {
      // Sin accion: limpiar este estado es mejor esfuerzo.
    }
  }

  function applyLocalUiStatus() {
    try {
      const payload = JSON.parse(localStorage.getItem(UI_STATUS_STORAGE_KEY) || '{}');
      if (!payload.message || Date.now() > Number(payload.expiresAt || 0)) {
        clearLocalUiStatus();
        return false;
      }
      setProgBadge(payload.badge || 'INFO', payload.mode || '');
      setProgStatus(payload.message, payload.mode || '');
      setFooterStatus(payload.footer || payload.message, payload.mode || '');
      return true;
    } catch (error) {
      clearLocalUiStatus();
      return false;
    }
  }

  let confirmToastResolve = null;

  function closeConfirmToast(accepted) {
    const toast = document.getElementById('confirm-toast');
    if (toast) {
      toast.classList.remove('show');
      toast.setAttribute('aria-hidden', 'true');
    }
    if (confirmToastResolve) {
      confirmToastResolve(Boolean(accepted));
      confirmToastResolve = null;
    }
  }

  function showConfirmToast({ title, message, confirmText = 'Aceptar', cancelText = 'Cancelar' }) {
    const toast = document.getElementById('confirm-toast');
    if (!toast) return Promise.resolve(window.confirm(message || title || 'Confirmar'));

    document.getElementById('confirm-toast-title').textContent = title || 'Confirmar accion';
    document.getElementById('confirm-toast-message').textContent = message || 'Deseas continuar?';
    document.getElementById('confirm-toast-accept').textContent = confirmText;
    document.getElementById('confirm-toast-cancel').textContent = cancelText;
    toast.classList.add('show');
    toast.setAttribute('aria-hidden', 'false');
    refreshIcons();

    if (confirmToastResolve) confirmToastResolve(false);
    return new Promise(resolve => {
      confirmToastResolve = resolve;
    });
  }

  // -- Botones -------------------------------------------------------------------
  function updateButtons() {
    const hasPending   = stats.pending > 0;
    const hasPdfWork   = Number(stats.pdf_pending || 0) > 0;
    const hasProcessed = stats.ok > 0 || stats.errors > 0;
    document.getElementById('btn-start').disabled     = isRunning || (!hasPending && !hasPdfWork);
    document.getElementById('btn-stop').disabled      = !isRunning;
    document.getElementById('btn-clear').disabled     = isRunning;
    document.getElementById('btn-reprocess').disabled = isRunning || hasPending || !hasProcessed;
    const newPalletBtn = document.getElementById('btn-new-pallet');
    if (newPalletBtn) newPalletBtn.disabled = false;

    // Deshabilitar checkboxes durante ejecución
    ['chk-f1','chk-f2','chk-auto'].forEach(id => {
      const el = document.getElementById(id);
      if (el) el.disabled = isRunning;
    });
    updatePhaseCards();
  }

  function ensureEmptyQueueMessage() {
    const tbody = document.getElementById('queue-tbody');
    if (!tbody.querySelector('tr[data-hu]') && !tbody.querySelector('#empty-row')) {
      tbody.innerHTML = emptyQueueRowHTML();
      refreshIcons();
    }
  }

  function removePalletRows(palletId) {
    const normalizedPalletId = String(palletId);
    const sep = document.querySelector(`tr.pallet-sep[data-pallet-id="${normalizedPalletId}"]`);
    document.querySelectorAll(`tr[data-pallet="${normalizedPalletId}"]`).forEach(row => row.remove());
    if (sep) sep.remove();
    delete palletSepRows[normalizedPalletId];
    removePalletTimer(normalizedPalletId);
  }

  // -- Scan no-loss --------------------------------------------------------------
  const scanInputForPaste = document.getElementById('scan-input');
  let ctrlVPastePending = false;
  let bulkPasteRunning = false;

  function loadScanOutbox() {
    try {
      const raw = localStorage.getItem(SCAN_OUTBOX_STORAGE_KEY);
      const parsed = JSON.parse(raw || '[]');
      if (!Array.isArray(parsed)) return [];
      return parsed
        .filter(entry => entry && typeof entry.code === 'string' && entry.code.trim())
        .slice(0, SCAN_OUTBOX_MAX_ITEMS);
    } catch (error) {
      return [];
    }
  }

  function saveScanOutbox() {
    try {
      localStorage.setItem(
        SCAN_OUTBOX_STORAGE_KEY,
        JSON.stringify(scanOutbox.slice(0, SCAN_OUTBOX_MAX_ITEMS))
      );
    } catch (error) {
      addDiagnosticLog('error', 'SCAN', 'No se pudo guardar la cola local de escaneo.', error.message);
    }
  }

  function getCurrentScanOptions() {
    return {
      run_f1: Boolean(document.getElementById('chk-f1')?.checked),
      run_f2: Boolean(document.getElementById('chk-f2')?.checked),
      run_pdf: true,
    };
  }

  function validateScannedCode(raw) {
    if (isPalletSeparator(raw)) return { ok: true };
    if (raw.length < 10 || raw.length > 15) {
      return {
        ok: false,
        message: `Código inválido: '${raw}' tiene ${raw.length} chars. Rango: 10-15`,
      };
    }
    return { ok: true };
  }

  function enqueueScanCode(rawCode, source = 'scan', options = {}) {
    const raw = String(rawCode || '').trim();
    if (!raw) return false;

    const validation = validateScannedCode(raw);
    if (!validation.ok) {
      if (!options.silent) {
        setHint(`Error: ${validation.message}`, 'warn');
        flash(`Error: Código HU inválido: ${raw.length} caracteres`, 'orange');
      }
      return false;
    }

    if (scanOutbox.length >= SCAN_OUTBOX_MAX_ITEMS) {
      setHint('Cola local llena. Espera a que se sincronicen los HUs pendientes.', 'error', 0);
      flash('Cola local de escaneo llena. Revisa conexión.', 'red');
      addDiagnosticLog('error', 'SCAN', 'Cola local de escaneo llena.', `max=${SCAN_OUTBOX_MAX_ITEMS}`);
      return false;
    }

    scanOutbox.push({
      id: `${Date.now()}-${Math.random().toString(16).slice(2)}`,
      code: raw,
      source,
      options: getCurrentScanOptions(),
      attempts: 0,
      createdAt: Date.now(),
      nextAttemptAt: 0,
      lastError: '',
    });
    saveScanOutbox();

    if (!options.silent) {
      setHint(`Capturado ${raw}. Guardando en servidor...`, 'idle', 0);
    }
    updateScanOutboxStatus();
    scheduleScanOutboxProcessing(0);
    return true;
  }

  function clearScanSavingFeedback() {
    const savingText = 'lectura(s) de escaneo';
    const mode = isRunning ? 'running' : '';
    const message = isRunning ? 'Escaneo guardado. Proceso activo.' : 'Escaneo guardado. En espera.';
    const prog = document.getElementById('prog-status');
    const footer = document.getElementById('footer-status');

    if (prog?.textContent.includes(savingText)) setProgStatus(message, mode);
    if (footer?.textContent.includes(savingText)) setFooterStatus(message, mode);
  }

  function updateScanOutboxStatus() {
    const pending = scanOutbox.length;
    if (!pending) {
      if (scanOutboxHadFailures) {
        setHint('Todos los HUs pendientes fueron guardados.', 'ok');
        setFooterStatus('Escaneo sincronizado.', isRunning ? 'running' : 'done');
        addDiagnosticLog('ok', 'SCAN', 'Cola local de escaneo sincronizada.');
      } else {
        clearScanSavingFeedback();
      }
      scanOutboxHadFailures = false;
      clearLocalUiStatus();
      return;
    }

    const hasFailures = scanOutbox.some(entry => entry.lastError);
    const first = scanOutbox[0];
    const message = hasFailures
      ? `${pending} HU(s) pendientes por guardar. Reintentando...`
      : `Guardando ${pending} lectura(s) de escaneo...`;
    setHint(message, hasFailures ? 'warn' : 'idle', 0);
    setFooterStatus(
      hasFailures ? `${message} Último error: ${first.lastError || 'sin detalle'}` : message,
      hasFailures ? 'waiting' : (isRunning ? 'running' : 'waiting')
    );
    rememberLocalUiStatus({
      badge: hasFailures ? 'PENDIENTE' : 'SCAN',
      mode: hasFailures ? 'waiting' : (isRunning ? 'running' : 'waiting'),
      message,
      footer: hasFailures ? `${message} Ultimo error: ${first.lastError || 'sin detalle'}` : message,
      ttlMs: 60000,
    });
  }

  function scheduleScanOutboxProcessing(delayMs = 0) {
    clearTimeout(scanOutboxTimer);
    scanOutboxTimer = setTimeout(processScanOutbox, Math.max(0, delayMs));
  }

  function nextScanRetryDelay(attempts) {
    return Math.min(SCAN_RETRY_MAX_MS, SCAN_RETRY_BASE_MS * (2 ** Math.min(attempts, 4)));
  }

  function isScanRetryableFailure(data) {
    if (!data) return true;
    if (data.retryable === true) return true;
    const status = Number(data.status || data.status_code || 0);
    return status >= 500 || status === 0 || status === 408 || status === 429;
  }

  function maybeAutoStartAfterScan(data) {
    if (!data?.ok || isRunning || !document.getElementById('chk-auto')?.checked) return;
    if (!(stats.pending > 0 || data.ok)) return;
    if (window._autoStarted) return;

    window._autoStarted = true;
    setTimeout(() => {
      startProcess();
      setTimeout(() => { window._autoStarted = false; }, 2000);
    }, 500);
  }

  function handleScanServerResult(entry, data) {
    const code = entry.code;
    if (data.resolved_duplicate) {
      setHint(`HU ${code} ya estaba registrado. Cola local sincronizada.`, 'ok');
      if (entry.source !== 'paste') {
        flash(`HU ${code} confirmado en servidor.`, 'green');
      }
      if (data.stats) handleStatsUpdate(data.stats);
      return;
    }

    if (data.ok) {
      const okMessage = isPalletSeparator(code)
        ? (data.message || `Pallet ${data.pallet_id} listo.`)
        : `OK  ${code}  ->  Pallet ${data.pallet_id}  [${data.origin || 'OK'}]`;
      setHint(okMessage, 'ok');
      if (entry.source !== 'paste') {
        flash(okMessage, 'green');
      }
      if (data.stats) handleStatsUpdate(data.stats);
      handleAutoStartResult(data);
      if (data.needs_resync) {
        resyncQueueUI({ source: 'scan_response', silent: true, notify: false });
      }
      maybeAutoStartAfterScan(data);
      return;
    }

    const message = data.error || data.message || 'Escaneo rechazado por el servidor.';
    const color = data.type === 'duplicate' ? 'warn' : 'error';
    setHint(`Error: ${message}`, color);
    if (entry.source !== 'paste' || data.type !== 'duplicate') {
      flash(`Error: ${message}`, data.type === 'duplicate' ? 'orange' : 'red');
    }
  }

  async function postScannedEntry(entry) {
    const body = {
      code: entry.code,
      ...(entry.options || getCurrentScanOptions()),
    };
    const res = await fetchWithTimeout('/scan/', {
      method: 'POST',
      headers: csrfHeaders({ 'Content-Type': 'application/json' }),
      body: JSON.stringify(body),
    }, FETCH_TIMEOUTS.scanPastedLine, 'Escaneo HU');
    return await readJsonResponse(res);
  }

  async function postScannedBatch(entries) {
    const res = await fetchWithTimeout('/scan/batch/', {
      method: 'POST',
      headers: csrfHeaders({ 'Content-Type': 'application/json' }),
      body: JSON.stringify({
        items: entries.map(entry => ({
          id: entry.id,
          code: entry.code,
          options: entry.options || getCurrentScanOptions(),
        })),
      }),
    }, FETCH_TIMEOUTS.scanPastedLine, 'Escaneo HU batch');
    return await readJsonResponse(res);
  }

  function markScanEntryForRetry(entry, errorMessage) {
    entry.attempts = Number(entry.attempts || 0) + 1;
    entry.lastError = errorMessage || 'Error de conexion';
    entry.nextAttemptAt = Date.now() + nextScanRetryDelay(entry.attempts);
    scanOutboxHadFailures = true;

    if (entry.attempts === 1 || entry.attempts % 5 === 0) {
      flash(`HU pendiente por guardar: ${entry.code}. Reintentando.`, 'orange');
      addDiagnosticLog(
        'warn',
        'SCAN',
        'Escaneo pendiente por fallo temporal.',
        `${entry.code} attempts=${entry.attempts} error=${entry.lastError}`
      );
    }
  }

  function dueScanOutboxEntries() {
    const now = Date.now();
    const due = [];
    for (const entry of scanOutbox) {
      if (Number(entry.nextAttemptAt || 0) > now) break;
      due.push(entry);
      if (due.length >= SCAN_BATCH_MAX_ITEMS) break;
    }
    return due;
  }

  function applyScanBatchResult(entries, data) {
    const results = Array.isArray(data?.results) ? data.results : [];
    const resultById = new Map(results.map(result => [String(result.id || ''), result]));
    const consumedIds = new Set();
    let savedOrResolved = 0;

    for (const entry of entries) {
      const result = resultById.get(String(entry.id || ''));
      if (!result) {
        markScanEntryForRetry(entry, 'Respuesta batch incompleta');
        continue;
      }

      if (result.ok || !isScanRetryableFailure(result)) {
        consumedIds.add(entry.id);
        savedOrResolved += 1;
        handleScanServerResult(
          { ...entry, source: 'paste' },
          result.type === 'duplicate' ? { ...result, resolved_duplicate: true } : result
        );
      } else {
        markScanEntryForRetry(entry, result.error || result.message || `HTTP ${result.status_code || 500}`);
      }
    }

    if (consumedIds.size) {
      scanOutbox = scanOutbox.filter(entry => !consumedIds.has(entry.id));
    }
    if (data?.stats) handleStatsUpdate(data.stats);
    if (savedOrResolved) {
      setHint(`${savedOrResolved} HU(s) pendientes guardados en servidor.`, 'ok');
      setFooterStatus('Escaneo sincronizado con servidor.', isRunning ? 'running' : 'done');
      resyncQueueUI({ source: 'scan_batch', silent: true, notify: false });
    }
  }

  async function processScanOutbox() {
    if (scanOutboxProcessing || !scanOutbox.length) return;

    const dueEntries = dueScanOutboxEntries();
    if (!dueEntries.length) {
      updateScanOutboxStatus();
      const nextWait = Math.max(0, Number(scanOutbox[0].nextAttemptAt || 0) - Date.now());
      scheduleScanOutboxProcessing(nextWait);
      return;
    }

    if (dueEntries.length > 1) {
      scanOutboxProcessing = true;
      try {
        const data = await postScannedBatch(dueEntries);
        if (!data?.ok || !Array.isArray(data.results)) {
          throw new Error(data?.error || data?.message || 'Respuesta batch invalida');
        }
        applyScanBatchResult(dueEntries, data);
        saveScanOutbox();
      } catch (error) {
        dueEntries.forEach(entry => markScanEntryForRetry(entry, error.message || 'Error de conexion'));
        saveScanOutbox();
      } finally {
        scanOutboxProcessing = false;
        updateScanOutboxStatus();
        if (scanOutbox.length) {
          const nextWait = Math.max(0, Number(scanOutbox[0].nextAttemptAt || 0) - Date.now());
          scheduleScanOutboxProcessing(nextWait);
        }
      }
      return;
    }

    const entry = dueEntries[0];
    const waitMs = Math.max(0, Number(entry.nextAttemptAt || 0) - Date.now());
    if (waitMs > 0) {
      updateScanOutboxStatus();
      scheduleScanOutboxProcessing(waitMs);
      return;
    }

    scanOutboxProcessing = true;
    try {
      const data = await postScannedEntry(entry);
      const resolvedDuplicate = data?.type === 'duplicate' && Number(entry.attempts || 0) > 0;
      if (data.ok || resolvedDuplicate || !isScanRetryableFailure(data)) {
        scanOutbox.shift();
        saveScanOutbox();
        handleScanServerResult(entry, resolvedDuplicate ? { ...data, resolved_duplicate: true } : data);
      } else {
        throw new Error(data.error || data.message || `HTTP ${data.status || 500}`);
      }
    } catch (error) {
      entry.attempts = Number(entry.attempts || 0) + 1;
      entry.lastError = error.message || 'Error de conexión';
      entry.nextAttemptAt = Date.now() + nextScanRetryDelay(entry.attempts);
      scanOutboxHadFailures = true;
      saveScanOutbox();

      if (entry.attempts === 1 || entry.attempts % 5 === 0) {
        flash(`HU pendiente por guardar: ${entry.code}. Reintentando.`, 'orange');
        addDiagnosticLog(
          'warn',
          'SCAN',
          'Escaneo pendiente por fallo temporal.',
          `${entry.code} attempts=${entry.attempts} error=${entry.lastError}`
        );
      }
    } finally {
      scanOutboxProcessing = false;
      updateScanOutboxStatus();
      if (scanOutbox.length) {
        const nextWait = Math.max(0, Number(scanOutbox[0].nextAttemptAt || 0) - Date.now());
        scheduleScanOutboxProcessing(nextWait);
      }
    }
  }

  document.getElementById('scan-input').addEventListener('keydown', (e) => {
    if (e.key !== 'Enter') return;
    const raw = e.target.value.trim();
    e.target.value = '';
    enqueueScanCode(raw, 'scan');
  });

  // -- Pegado multiple por Ctrl+V ------------------------------------------------
  function getPastedLines(text) {
    return String(text || '')
      .replace(/\r/g, '\n')
      .split('\n')
      .map(line => line.trim())
      .filter(Boolean);
  }

  async function scanPastedLines(codes) {
    if (bulkPasteRunning) return;

    bulkPasteRunning = true;
    scanInputForPaste.value = '';
    addDiagnosticLog('info', 'SCAN', 'Pegado múltiple detectado.', `lines=${codes.length}`);

    let queuedCount = 0;
    let invalidCount = 0;
    for (const code of codes) {
      if (enqueueScanCode(code, 'paste', { silent: true })) {
        queuedCount++;
      } else {
        invalidCount++;
      }
    }

    bulkPasteRunning = false;
    scanInputForPaste.focus();
    updateButtons();

    if (queuedCount) {
      setHint(`${queuedCount} lectura(s) agregadas a sincronización local.`, 'idle', 0);
      setFooterStatus(`${queuedCount} lectura(s) listas para guardar en servidor.`, isRunning ? 'running' : 'waiting');
      flash(`${queuedCount} lectura(s) capturadas`, 'green');
      scheduleScanOutboxProcessing(0);
    }

    if (invalidCount) {
      flash(`${invalidCount} línea(s) omitidas por formato inválido`, queuedCount ? 'orange' : 'red');
      addDiagnosticLog('warn', 'SCAN', 'Pegado con líneas inválidas.', `invalid=${invalidCount}`);
    }
  }

  scanInputForPaste.addEventListener('keydown', (e) => {
    if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === 'v') {
      ctrlVPastePending = true;
      setTimeout(() => { ctrlVPastePending = false; }, 1000);
    }
  });

  scanInputForPaste.addEventListener('paste', async (e) => {
    if (!ctrlVPastePending) return;
    ctrlVPastePending = false;

    const pastedText = e.clipboardData?.getData('text') || '';
    const codes = getPastedLines(pastedText);
    if (codes.length <= 1) return;

    e.preventDefault();
    await scanPastedLines(codes);
  });

  // -- Nuevo pallet --------------------------------------------------------------
  async function newPallet() {
    const wasRunning = isRunning;
    addDiagnosticLog('info', 'UI', 'Solicitud de nuevo pallet.', wasRunning ? 'proceso_activo=true' : 'proceso_activo=false');
    if (wasRunning) {
      showSapLoginMode('Cerrando pallet y validando sesión SAP para continuar...');
      updateButtons();
    }

    let data;
    try {
      const res  = await fetch('/pallet/nuevo/', {
        method: 'POST',
        headers: csrfHeaders({ 'Content-Type': 'application/json' }),
        body: JSON.stringify({
          run_f1: document.getElementById('chk-f1').checked,
          run_f2: document.getElementById('chk-f2').checked,
          run_pdf: true,
        })
      });
      data = await readJsonResponse(res);
    } catch (e) {
      setHint(`Error: ${e.message}`, 'warn');
      setProgStatus('No se pudo cerrar o crear el pallet.', 'error');
      setFooterStatus('Revisa conexión con Django y vuelve a intentar.', 'error');
      flash(`Error: ${e.message}`, 'red');
      return;
    }
    if (data.ok) {
      setHint(`OK ${data.message}`, 'ok');
      flash(`OK ${data.message}`, 'green');
      if (data.stats) handleStatsUpdate(data.stats);
      handleAutoStartResult(data);
      if (wasRunning && !data.auto_started && !data.auto_start_error) {
        setProgBadge('EN ESPERA', 'waiting');
        setProgStatus('Pallet cerrado. El worker tomara el siguiente lote en unos segundos.', 'waiting');
        setFooterStatus('Esperando reactivacion del worker continuo.', 'waiting');
      }
      ensurePalletSep(data.pallet_id, 'Sin origen');
      updatePalletSep(data.pallet_id);
      updateStickyPallet();
    } else {
      setHint(`Error: ${data.error}`, 'warn');
      flash(`Error: ${data.error}`, 'orange');
    }
  }

  // -- Iniciar proceso -----------------------------------------------------------
  async function startProcess() {
    if (isRunning) return;
    addDiagnosticLog('info', 'UI', 'Operador inicio procesamiento.', `pending=${stats.pending} pdf_pending=${Number(stats.pdf_pending || 0)}`);
    if (stats.pending === 0 && Number(stats.pdf_pending || 0) === 0) {
      setProgBadge('EN ESPERA', '');
      setProgStatus('No hay HUs pendientes ni PDFs por imprimir.', '');
      setFooterStatus('Escanea HUs o cierra un pallet antes de iniciar.', '');
      flash('Error: No hay HUs pendientes ni PDFs por imprimir.', 'orange');
      return;
    }

    isRunning = true;
    clearStopRequestState();
    showSapLoginMode('Validando sesión SAP antes de iniciar la cola...');
    updateButtons();

    try {
      const res = await fetchWithTimeout('/api/procesar/', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          ...csrfHeaders()
        },
        body: JSON.stringify({
          run_f1: document.getElementById('chk-f1').checked,
          run_f2: document.getElementById('chk-f2').checked,
        }),
      }, FETCH_TIMEOUTS.startProcess, 'Inicio de proceso');
      const data = await readJsonResponse(res);

      if (!data.ok) {
        flash(`Error: ${data.error}`, 'red');
        isRunning = false;
        clearStopRequestState();
        setProgBadge('EN ESPERA', '');
        setProgStatus('Error al iniciar.', '');
        document.getElementById('prog-bar').classList.remove('animated', 'done', 'error', 'waiting');
        updateButtons();
        return;
      }

      const huCount = Number(data.count || 0);
      const pdfCount = Number(data.pdf_count || 0);
      const message = pdfCount && !huCount
        ? `${pdfCount} PDF pendiente enviado a Celery`
        : `${huCount} HUs enviadas a Celery`;
      showRunningMode('SAP listo. Cola enviada a Celery.');
      flash(message, 'green');

    } catch (e) {
      flash(`Error: Error: ${e.message}`, 'red');
      isRunning = false;
      clearStopRequestState();
      setProgBadge('EN ESPERA', '');
      setProgStatus(e.message || 'Error al iniciar.', 'error');
      setFooterStatus('No se pudo confirmar el inicio. Revisa Daphne/Celery/Redis y vuelve a intentar.', 'error');
      document.getElementById('prog-bar').classList.remove('animated', 'waiting');
      document.getElementById('prog-bar').classList.add('error');
      updateButtons();
    }
  }

  // -- Detener proceso -----------------------------------------------------------
  async function stopProcess() {
    const alreadyWaitingForStop = stopRequested;
    const stopWaitMs = stopRequestedAt ? Date.now() - stopRequestedAt : 0;
    const forceRecovery = alreadyWaitingForStop || hasOrphanedQueueRuntime();
    addDiagnosticLog(
      'warn',
      'UI',
      forceRecovery ? 'Operador intentó liberar una detención pendiente.' : 'Operador solicitó detener el proceso.',
      forceRecovery ? `stop_wait=${Math.round(stopWaitMs / 1000)}s` : ''
    );
    markStopRequested();
    setProgBadge(forceRecovery ? 'LIBERANDO' : 'DETENIENDO', 'waiting');
    setProgStatus(
      forceRecovery
        ? 'Revisando si la cola puede liberarse de forma segura.'
        : 'Detencion solicitada. Esperando punto seguro del worker.',
      'waiting'
    );
    setFooterStatus(
      forceRecovery
        ? 'Validando lock, Celery y ultimo heartbeat del worker.'
        : 'SAP terminará la operación actual antes de liberar la cola.',
      'waiting'
    );
    updateButtons();

    try {
      const res = await fetchWithTimeout('/cola/detener/', {
        method: 'POST',
        headers: csrfHeaders({ 'Content-Type': 'application/json' }),
        body: JSON.stringify({ force: forceRecovery })
      }, FETCH_TIMEOUTS.stopProcess, 'Detener proceso');
      const data = await readJsonResponse(res);
      if (!data.ok) {
        isRunning = false;
        clearStopRequestState();
        flash(data.error || 'No hay proceso activo.', 'orange');
        updateButtons();
        return;
      }
      if (data.waiting_for_safe_stop) {
        const wait = Number(data.force_available_after_seconds || 0);
        setProgBadge('DETENIENDO', 'waiting');
        setProgStatus(data.message || 'Detencion segura en curso.', 'waiting');
        setFooterStatus(
          wait > 0
            ? `Esperando cierre seguro. Puedes intentar liberar de nuevo en ${wait}s.`
            : 'Si no hay avance, presiona Detener otra vez para liberar la cola.',
          'waiting'
        );
        flash(data.message || 'Detencion segura en curso.', 'orange');
        updateButtons();
        return;
      }
      if (data.recovered) {
        isRunning = false;
        clearStopRequestState();
        if (data.stats) handleStatsUpdate({ ...data.stats, is_running: false });
        clearSessionCloseCountdown();
        setProgBadge('Detenido', '');
        setProgStatus(data.message || 'Cola liberada tras cierre de Celery.', '');
        setFooterStatus('Revisa la cola antes de reintentar.', '');
        document.getElementById('prog-bar').classList.remove('animated', 'waiting', 'done', 'error');
        flash(data.message || 'Cola liberada tras cierre de Celery.', 'orange');
        updateButtons();
        return;
      }
      setProgBadge('DETENIENDO', 'waiting');
      setProgStatus(data.message || 'Detencion solicitada.', 'waiting');
      setFooterStatus('Deteniendo en punto seguro...', 'waiting');
      flash(data.message || 'Detencion solicitada.', 'orange');
    } catch (e) {
      clearStopRequestState();
      setProgBadge('SIN RESPUESTA', 'error');
      setProgStatus(e.message || 'No se pudo confirmar la detención.', 'error');
      setFooterStatus('El backend no confirmó la detención. Si el proceso sigue activo, intenta Detener nuevamente.', 'error');
      flash(`Error al detener: ${e.message}`, 'red');
    }
    updateButtons();
  }
  // -- Limpiar -------------------------------------------------------------------
  async function clearQueue() {
    if (isRunning) {
      flash('No se puede limpiar mientras el proceso está activo.', 'orange');
      return;
    }
    const confirmed = await showConfirmToast({
      title: 'Limpiar HUs',
      message: 'Esto eliminara todas las HUs de la cola y reiniciara los pallets. Esta accion no se puede deshacer.',
      confirmText: 'Limpiar HUs',
      cancelText: 'Cancelar',
    });
    if (!confirmed) return;

    addDiagnosticLog('warn', 'UI', 'Operador solicitó limpiar la cola.', `total=${stats.total}`);
    setProgBadge('LIMPIANDO', 'waiting');
    setProgStatus('Limpiando cola...', 'waiting');
    setFooterStatus('Eliminando HUs y reiniciando contadores.', 'waiting');

    let data;
    try {
      const res  = await fetchWithTimeout('/cola/limpiar/', {
        method: 'POST',
        headers: { ...csrfHeaders() }
      }, FETCH_TIMEOUTS.clearQueue, 'Limpiar cola');
      data = await readJsonResponse(res);
    } catch (e) {
      setProgBadge('EN ESPERA', '');
      setProgStatus(e.message || 'No se pudo limpiar la cola.', 'error');
      setFooterStatus('Revisa conexión con Django y vuelve a intentar.', 'error');
      flash(`Error: ${e.message}`, 'red');
      updateButtons();
      return;
    }

    if (data.ok) {
      document.getElementById('queue-tbody').innerHTML = emptyQueueRowHTML();
      refreshIcons();
      Object.keys(palletSepRows).forEach(k => delete palletSepRows[k]);
      _visibleSeps.clear();
      clearPalletTimers();
      clearLocalUiStatus();
      if (stickyPallet) stickyPallet.classList.remove('visible');
      isRunning = false;
      clearStopRequestState();
      stats = {
        total:0, ok:0, errors:0, pending:0, pallets:0,
        pallets_total:0, pallets_done:0,
        pallets_processing_seconds:0, pallets_processing_display:'0s',
        pdf_pending:0,
      };
      handleStatsUpdate(stats);
      setProgBadge('EN ESPERA', '');
      setProgStatus('Cola limpiada. Listo para escanear.', '');
      setFooterStatus('En espera.', '');
      document.getElementById('prog-bar').classList.remove('animated','done','error','waiting');
      flash('OK Cola limpiada.', 'green');
      document.getElementById('scan-input').focus();
    } else {
      setProgBadge('EN ESPERA', '');
      setProgStatus('No se pudo limpiar la cola.', 'error');
      setFooterStatus(data.error || 'Limpieza rechazada por el servidor.', 'error');
      flash(`Error: ${data.error}`, 'red');
    }
  }

  // -- Reprocesar ----------------------------------------------------------------
  async function reprocess() {
    if (isRunning) return;
    if (stats.pending > 0 || ((stats.ok + stats.errors) === 0 && !hasErrorRows())) {
      setFooterStatus('No hay HUs disponibles para reprocesar.', '');
      updateButtons();
      return;
    }

    if (hasErrorRows()) {
      showReprocessMenu();
      return;
    }

    await reprocessQueue('all');
  }

  function hasErrorRows() {
    return Boolean(document.querySelector(
      '#queue-tbody tr[data-status="error"], #queue-tbody tr[data-status="hu_not_found"]'
    ));
  }

  function showReprocessMenu() {
    const menu = document.getElementById('reprocess-menu');
    const btn = document.getElementById('btn-reprocess');
    if (!menu || !btn) return;

    const rect = btn.getBoundingClientRect();
    menu.style.display = 'block';
    menu.style.left = Math.min(rect.left, window.innerWidth - 210) + 'px';
    menu.style.top = Math.max(8, rect.top - menu.offsetHeight - 8) + 'px';
  }

  function hideReprocessMenu() {
    const menu = document.getElementById('reprocess-menu');
    if (menu) menu.style.display = 'none';
  }

  async function reprocessQueue(mode) {
    hideReprocessMenu();
    if (isRunning) return;

    const isErrorsOnly = mode === 'errors';
    const message = isErrorsOnly
      ? 'Marcará solo los HUs con error como Pendientes para reprocesar.\n¿Continuar?'
      : 'Marcará todos los HUs procesados como Pendientes para reprocesar.\n¿Continuar?';
    const confirmed = await showConfirmToast({
      title: isErrorsOnly ? 'Reprocesar errores' : 'Reprocesar lista completa',
      message: isErrorsOnly
        ? 'Marcara solo los HUs con error como pendientes para reprocesar.'
        : 'Marcara todos los HUs procesados como pendientes para reprocesar.',
      confirmText: 'Reprocesar',
      cancelText: 'Cancelar',
    });
    if (!confirmed) return;

    addDiagnosticLog('warn', 'UI', 'Operador inicio reproceso.', `mode=${mode}`);
    isRunning = true;
    clearStopRequestState();
    showSapLoginMode('Validando sesión SAP antes de reprocesar...');
    updateButtons();

    let data;
    try {
      const res  = await fetchWithTimeout('/cola/reprocesar/', {
        method: 'POST',
        headers: csrfHeaders({ 'Content-Type': 'application/json' }),
        body: JSON.stringify({ mode })
      }, FETCH_TIMEOUTS.reprocessQueue, 'Reproceso');
      data = await readJsonResponse(res);
    } catch (e) {
      flash(`Error al reprocesar: ${e.message}`, 'red');
      isRunning = false;
      clearStopRequestState();
      setProgBadge('EN ESPERA', '');
      setProgStatus(e.message || 'Error al reprocesar.', 'error');
      setFooterStatus('No se pudo confirmar el reproceso. Revisa Daphne/Celery/Redis y vuelve a intentar.', 'error');
      document.getElementById('prog-bar').classList.remove('animated', 'waiting');
      document.getElementById('prog-bar').classList.add('error');
      updateButtons();
      return;
    }

    if (data.ok) {
      flash('OK ' + data.count + ' HUs marcados como pendientes.', 'green');
      const selector = isErrorsOnly
        ? '#queue-tbody tr[data-status="error"], #queue-tbody tr[data-status="hu_not_found"]'
        : '#queue-tbody tr[data-hu]';

      document.querySelectorAll(selector).forEach(tr => {
        if (tr.cells[3]) tr.cells[3].innerHTML = statusCellHTML('pending', 'Pendiente', '', '', 'f1');
        if (tr.cells[4]) tr.cells[4].innerHTML = statusCellHTML('pending', '', 'Pendiente', '', 'f2');
        if (tr.cells[5]) tr.cells[5].innerHTML = pdfCellHTML('pending');
        tr.dataset.status = 'pending';
      });
      refreshIcons();

      if (isErrorsOnly) {
        stats.pending += data.count;
        stats.errors = Math.max(0, stats.errors - data.count);
      } else {
        stats.ok = 0;
        stats.errors = 0;
        stats.pending = stats.total;
      }

      updateProgress();
      showRunningMode('SAP listo. Reproceso enviado a Celery.');
      updateButtons();
    } else {
      isRunning = false;
      clearStopRequestState();
      setProgBadge('EN ESPERA', '');
      setProgStatus('Error al reprocesar.', 'error');
      document.getElementById('prog-bar').classList.remove('animated', 'waiting');
      document.getElementById('prog-bar').classList.add('error');
      updateButtons();
      flash('Error: ' + data.error, 'red');
    }
  }
  async function deleteHU(huCode) {
    if (isRunning) {
      flash('No se puede borrar mientras el proceso está activo.', 'orange');
      return;
    }
    const row = document.getElementById(`row-${huCode}`);
    if (!row) {
      flash('HU no encontrada en la tabla actual.', 'orange');
      return;
    }
    const currentStatus = row.dataset.status || '';
    if (!canDeleteStatus(currentStatus)) {
      flash('Esta HU ya no se puede borrar.', 'orange');
      return;
    }

    setFooterStatus(`Eliminando HU ${huCode}...`, 'waiting');

    let data;
    try {
      const res  = await fetch(`/hu/${huCode}/borrar/`, {
        method: 'POST',
        headers: { ...csrfHeaders() }
      });
      data = await readJsonResponse(res);
    } catch (e) {
      setFooterStatus('No se pudo borrar la HU.', 'error');
      flash(`Error: ${e.message}`, 'red');
      return;
    }

    if (data.ok) {
      if (row) {
        const palletId = parseInt(row.dataset.pallet);
        row.remove();
        if (data.pallet_deleted) {
          const sep = document.querySelector(`tr.pallet-sep[data-pallet-id="${palletId}"]`);
          if (sep) sep.remove();
          delete palletSepRows[palletId];
          removePalletTimer(palletId);
        } else {
          if (data.pdf_reset) {
            updatePalletPdfCells({
              pallet_id: palletId,
              status: 'pending',
              pdf_status: data.pdf_status || '',
              pdf_display: data.pdf_display || '',
              pdf_msg: data.pdf_msg || '',
            });
          }
          updatePalletSep(palletId);
        }
        if (data.stats) {
          handleStatsUpdate(data.stats);
        } else {
          adjustStatsForDeletedStatus(currentStatus);
        }
        updateStickyPallet();
        ensureEmptyQueueMessage();
        updateButtons();
      }
      flash(data.pdf_reset
        ? `OK HU ${huCode} eliminada. Pallet listo para imprimir PDF.`
        : `OK HU ${huCode} eliminada.`,
        'green');
      setFooterStatus(data.pdf_reset
        ? `HU ${huCode} eliminada. PDF del pallet recalculado.`
        : `HU ${huCode} eliminada.`,
        '');
    } else {
      setFooterStatus(data.error || 'Borrado rechazado por el servidor.', 'error');
      flash(`Error: ${data.error}`, 'orange');
    }
  }

  // -- Flash ---------------------------------------------------------------------
  function adjustStatsForDeletedStatus(status) {
    stats.total = Math.max(0, stats.total - 1);
    if (status === 'pending') stats.pending = Math.max(0, stats.pending - 1);
    if (status === 'ok' || status === 'duplicate') stats.ok = Math.max(0, stats.ok - 1);
    if (status === 'error' || status === 'hu_not_found') stats.errors = Math.max(0, stats.errors - 1);
    handleStatsUpdate(stats);
  }

  function flash(msg, color) {
    const bar = document.getElementById('flash-bar');
    bar.textContent = msg;
    bar.className = `show ${color}`;
    clearTimeout(flashTimer);
    flashTimer = setTimeout(() => { bar.className = ''; }, 3000);
  }

  // -- CSRF helper ---------------------------------------------------------------
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
        addDiagnosticLog(
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
    addDiagnosticLog(
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

  // -- SAP Status polling (equivale al QTimer de _check_sap) --------------------
  function sapStatusLogConfig(status, data = {}) {
    const configs = {
      connected: {
        level: 'ok',
        label: `CONECTADO${data.user ? ' - ' + data.user : ''}`,
        message: 'SAP conectado.',
      },
      disconnected: {
        level: 'warn',
        label: 'Sin sesión SAP',
        message: 'SAP desconectado.',
      },
      timeout: {
        level: 'warn',
        label: 'SAP sin confirmar',
        message: 'SAP no respondió a tiempo.',
      },
      stale: {
        level: 'warn',
        label: 'SAP sin confirmar',
        message: 'Verificación SAP pausada temporalmente.',
      },
      unavailable: {
        level: 'error',
        label: 'SAP no disponible',
        message: 'SAP no disponible para verificacion.',
      },
      unknown: {
        level: 'warn',
        label: 'Estado SAP no confirmado',
        message: 'Estado SAP no confirmado.',
      },
    };
    return configs[status] || configs.unknown;
  }

  function updateSapIndicator(data = {}) {
    const status = data.status || (data.connected ? 'connected' : 'disconnected');
    const config = sapStatusLogConfig(status, data);
    document.getElementById('conn-dot').className = `conn-dot ${status === 'connected' ? 'ok' : ''}`;
    document.getElementById('conn-label').textContent = config.label;
    return { status, config };
  }

  function applySapStatusBackoff(reason) {
    sapStatusFailureCount += 1;
    const multiplier = 2 ** Math.min(sapStatusFailureCount, 4);
    const nextDelay = Math.min(SAP_STATUS_MAX_POLL_MS, SAP_STATUS_BASE_POLL_MS * multiplier);
    if (nextDelay !== sapStatusPollDelayMs) {
      addDiagnosticLog(
        'warn',
        'SAP',
        'Verificación SAP pausada temporalmente.',
        `próxima consulta en ${Math.round(nextDelay / 1000)}s; motivo=${reason}`
      );
    }
    sapStatusPollDelayMs = nextDelay;
  }

  function resetSapStatusBackoff() {
    if (sapStatusFailureCount > 0 || sapStatusPollDelayMs !== SAP_STATUS_BASE_POLL_MS) {
      addDiagnosticLog('ok', 'SAP', 'Verificación SAP recuperada.');
    }
    sapStatusFailureCount = 0;
    sapStatusPollDelayMs = SAP_STATUS_BASE_POLL_MS;
  }

  function scheduleNextSapCheck(delayMs = sapStatusPollDelayMs) {
    clearTimeout(sapStatusPollTimer);
    sapStatusPollTimer = setTimeout(async () => {
      await checkSAP();
      scheduleNextSapCheck();
    }, delayMs);
  }

  async function checkSAP() {
    if (sapStatusCheckRunning) return Boolean(lastSapConnected);
    sapStatusCheckRunning = true;
    try {
      const res  = await fetchWithTimeout(
        '/api/sap-status/',
        {},
        FETCH_TIMEOUTS.checkSAP,
        'Consulta de estado SAP'
      );
      const data = await readJsonResponse(res);
      const { status, config } = updateSapIndicator(data);
      const connected = status === 'connected';
      const shouldBackoff = ['timeout', 'stale', 'unavailable', 'unknown'].includes(status);

      if (shouldBackoff) {
        applySapStatusBackoff(status);
      } else {
        resetSapStatusBackoff();
      }

      if (lastSapStatus !== status || lastSapConnected !== connected) {
        addDiagnosticLog(
          config.level,
          'SAP',
          config.message,
          data.user ? `user=${data.user}` : data.message || data.error || ''
        );
      }

      lastSapStatus = status;
      lastSapConnected = connected;
      return connected;
    } catch (error) {
      applySapStatusBackoff('http_error');
      updateSapIndicator({ status: 'unavailable' });
      if (lastSapConnected !== false) {
        addDiagnosticLog('error', 'SAP', 'No se pudo consultar /api/sap-status/.', error.message);
      }
      lastSapStatus = 'unavailable';
      lastSapConnected = false;
    } finally {
      sapStatusCheckRunning = false;
    }
    return false;
  }
  scheduleNextSapCheck(SAP_STATUS_BASE_POLL_MS);

  async function ensureSapReady() {
    try {
      const res = await fetch('/api/sap-status/');
      const data = await readJsonResponse(res);
      if (data.connected) return true;
    } catch {}

    flash('Inicializando SAP e iniciando sesión...', 'orange');
    setProgBadge('SAP', 'running');
    setProgStatus('Inicializando SAP e iniciando sesión...', 'running');
    setFooterStatus('Inicializando SAP...', 'running');

    try {
      const res = await fetch('/api/sap/iniciar/', {
        method: 'POST',
        headers: { ...csrfHeaders() },
      });
      const data = await readJsonResponse(res);

      if (data.ok) {
        flash(data.message || 'SAP conectado correctamente.', 'green');
        await checkSAP();
        return true;
      }

      flash(`Error SAP: ${data.error}`, 'red');
      setProgBadge('EN ESPERA', '');
      setProgStatus(data.error || 'SAP sin sesión activa.', 'error');
      setFooterStatus('SAP sin sesión activa.', 'error');
      return false;
    } catch (e) {
      flash(`Error SAP: ${e.message}`, 'red');
      setProgBadge('EN ESPERA', '');
      setProgStatus('No se pudo inicializar SAP.', 'error');
      setFooterStatus('SAP sin sesión activa.', 'error');
      return false;
    }
  }

  // -- Init ----------------------------------------------------------------------
  // La inicialización principal vive al final del archivo para evitar dobles
  // registros cuando Django renderiza la tabla antes de cargar este script.

  // -- Menú hamburguesa ----------------------------------------------------------
  function toggleHamMenu() {
    const m = document.getElementById('ham-menu');
    m.style.display = m.style.display === 'none' ? 'block' : 'none';
  }
  // Cierra el menú al hacer click fuera
  document.addEventListener('click', (e) => {
    const menu  = document.getElementById('ham-menu');
    const btn   = document.getElementById('ham-btn');
    if (menu && btn && !btn.contains(e.target) && !menu.contains(e.target)) {
      menu.style.display = 'none';
    }
  });

  function installResyncMenuAction() {
    const menu = document.getElementById('ham-menu');
    if (!menu || document.getElementById('ham-resync')) return;

    const item = document.createElement('div');
    item.className = 'ham-item ham-item-help';
    item.id = 'ham-resync';
    item.title = 'Actualiza la pantalla con la información real del servidor. Úsalo si la tabla, botones o progreso se ven congelados.';
    item.innerHTML = `
      ${iconHTML('refresh-cw')}
      <span class="ham-item-copy">
        <strong>Resincronizar UI</strong>
        <small>Actualiza la pantalla con datos del servidor. Úsalo si ves estados congelados o botones incorrectos.</small>
      </span>
    `;
    item.addEventListener('click', () => {
      menu.style.display = 'none';
      manualResyncQueueUI();
    });

    const diagnosticItem = Array
      .from(menu.querySelectorAll('.ham-item'))
      .find(el => (el.textContent || '').includes('Diagn'));
    menu.insertBefore(item, diagnosticItem || menu.firstChild);
    refreshIcons();
  }

  // -- Scan hint -----------------------------------------------------------------
  let _hintTimer = null;
  function setHint(msg, type, timeoutMs = 2500) {
    const el = document.getElementById('scan-hint');
    el.textContent = msg;
    el.className = `scan-hint ${type}`;
    clearTimeout(_hintTimer);
    if (!timeoutMs) return;
    _hintTimer = setTimeout(() => {
      el.textContent = 'Presiona Enter para confirmar - Escribe PALLET para separar';
      el.className = 'scan-hint idle';
    }, timeoutMs);
  }

  // -- Pipeline phase cards ------------------------------------------------------
  function updatePhaseCards() {
    [
      ['f1',   document.getElementById('chk-f1')],
      ['f2',   document.getElementById('chk-f2')],
    ].forEach(([id, chk]) => {
      if (!chk) return;
      const card = document.getElementById(`phase-${id}`);
      if (!card) return;
      const active = chk.checked;
      card.classList.toggle('active',   active);
      card.classList.toggle('inactive', !active);
      card.classList.toggle('is-disabled', chk.disabled);
      card.setAttribute('aria-pressed', active ? 'true' : 'false');
      card.setAttribute('aria-disabled', chk.disabled ? 'true' : 'false');
    });
  }

  function togglePhaseCard(card) {
    const checkboxId = card.dataset.checkboxId;
    const checkbox = checkboxId ? document.getElementById(checkboxId) : null;
    if (!checkbox || checkbox.disabled) return;

    checkbox.checked = !checkbox.checked;
    checkbox.dispatchEvent(new Event('change', { bubbles: true }));
  }

  function initializePhaseToggleCards() {
    document.querySelectorAll('.phase-card.phase-toggle').forEach(card => {
      if (card.dataset.bound === '1') return;
      card.dataset.bound = '1';

      card.addEventListener('click', () => togglePhaseCard(card));
      card.addEventListener('keydown', event => {
        if (event.key !== 'Enter' && event.key !== ' ') return;
        event.preventDefault();
        togglePhaseCard(card);
      });
    });
  }
  // -- Context menu de tabla -----------------------------------------------------
  let _ctxTargetHU      = null;
  let _ctxTargetPallet  = null;

  document.getElementById('queue-table').addEventListener('contextmenu', (e) => {
    e.preventDefault();
    if (isRunning) return;
    const row = e.target.closest('tr');
    if (!row) return;

    _ctxTargetHU     = null;
    _ctxTargetPallet = null;

    const menu       = document.getElementById('ctx-menu');
    const itemHU     = document.getElementById('ctx-delete-hu');
    const itemPallet = document.getElementById('ctx-delete-pal');

    if (row.classList.contains('pallet-sep')) {
      // Click en separador de pallet
      _ctxTargetPallet = row.dataset.palletId;
      itemHU.style.display     = 'none';
      itemPallet.style.display = 'block';
    } else if (row.dataset.hu) {
      // Click en fila de HU
      _ctxTargetHU = row.dataset.hu;
      const status = row.dataset.status || '';
      const canDel = canDeleteStatus(status);
      itemHU.style.display     = canDel ? 'block' : 'none';
      itemPallet.style.display = 'none';
    } else {
      return;
    }

    menu.style.display = 'block';
    menu.style.left    = `${Math.min(e.clientX, window.innerWidth  - 190)}px`;
    menu.style.top     = `${Math.min(e.clientY, window.innerHeight - 80)}px`;
  });

  document.addEventListener('click', (e) => {
    const m = document.getElementById('ctx-menu');
    if (m) m.style.display = 'none';
    const reprocessMenu = document.getElementById('reprocess-menu');
    const reprocessBtn = document.getElementById('btn-reprocess');
    if (
      reprocessMenu &&
      reprocessBtn &&
      !reprocessMenu.contains(e.target) &&
      !reprocessBtn.contains(e.target)
    ) {
      reprocessMenu.style.display = 'none';
    }
  });

  function ctxDeleteHU() {
    if (_ctxTargetHU) deleteHU(_ctxTargetHU);
  }

  async function ctxDeletePallet() {
    if (!_ctxTargetPallet) return;
    if (isRunning) {
      flash('No se puede borrar mientras el proceso está activo.', 'orange');
      return;
    }
    if (!confirm(`¿Borrar el pallet P${String(_ctxTargetPallet).padStart(2,'0')} completo?`)) return;

    setFooterStatus(`Eliminando Pallet P${String(_ctxTargetPallet).padStart(2, '0')}...`, 'waiting');

    let data;
    try {
      const res  = await fetch(`/pallet/${_ctxTargetPallet}/borrar/`, {
        method: 'POST',
        headers: { ...csrfHeaders() },
      });
      data = await readJsonResponse(res);
    } catch (e) {
      setFooterStatus('No se pudo borrar el pallet.', 'error');
      flash(`Error: ${e.message}`, 'red');
      return;
    }

    if (data.ok) {
      // Quitar separador y filas asociadas del DOM
      const sep = document.querySelector(`tr.pallet-sep[data-pallet-id="${_ctxTargetPallet}"]`);
      document.querySelectorAll(`tr[data-pallet="${_ctxTargetPallet}"]`).forEach(r => {
        adjustStatsForDeletedStatus(r.dataset.status || '');
        r.remove();
      });
      if (sep) sep.remove();
      delete palletSepRows[_ctxTargetPallet];
      removePalletTimer(_ctxTargetPallet);
      stats.pallets = Math.max(0, stats.pallets - 1);
      handleStatsUpdate(stats);
      updateStickyPallet();
      ensureEmptyQueueMessage();
      updateButtons();
      setFooterStatus(`Pallet P${String(_ctxTargetPallet).padStart(2, '0')} borrado.`, '');
      flash(`OK Pallet borrado.`, 'green');
    } else {
      setFooterStatus(data.error || 'Borrado de pallet rechazado por el servidor.', 'error');
      flash(`Error: ${data.error}`, 'red');
    }
  }
  // -- Render de filas -----------------------------------------------------------
  function buildRowHTML(item) {
    const pid  = String(item.pallet_id).padStart(2, '0');
    const safe = (item.origin_code || 'UNK').replace('/', '_');

    return `
      <td>P${pid}</td>
      <td><span class="origin-badge origin-${safe}">${item.origin_code || ''}</span></td>
      <td style="font-family:var(--mono);font-size:8.5pt;">${item.hu_code}</td>
      <td>${statusCellHTML(item.status, item.f1_display, item.f2_display, item.phase2_msg, 'f1')}</td>
      <td>${statusCellHTML(item.status, item.f1_display, item.f2_display, item.phase2_msg, 'f2')}</td>
      <td>${pdfCellHTML(item.status, item.pdf_status, item.pdf_display, item.pdf_msg)}</td>
    `;
  }

  // -- Sticky pallet -------------------------------------------------------------
  function initStickyObserver() {
    const wrap = document.getElementById('table-wrap');
    if (!wrap) return;

    _stickyObserver = new IntersectionObserver((entries) => {
      entries.forEach(entry => {
        const pid = parseInt(entry.target.dataset.palletId || '0');
        if (!pid) return;
        if (entry.isIntersecting) {
          _visibleSeps.add(pid);
        } else {
          _visibleSeps.delete(pid);
        }
      });
      _renderStickyPallet();
    }, { root: wrap, threshold: 0 });

    // Observar separadores existentes
    document.querySelectorAll('tr.pallet-sep').forEach(sep => {
      _stickyObserver.observe(sep);
    });
  }

  function _observeSep(sepRow) {
    if (_stickyObserver) _stickyObserver.observe(sepRow);
  }

  function _renderStickyPallet() {
    const sticky = document.getElementById('sticky-pallet');
    if (!sticky) return;

    // Encontrar el pallet más alto cuyo sep NO está visible (ya quedó arriba del scroll)
    const allPids    = Object.keys(palletSepRows).map(Number).sort((a,b) => a-b);
    const hiddenAbove = allPids.filter(pid => !_visibleSeps.has(pid));

    if (hiddenAbove.length === 0) {
      sticky.classList.remove('visible');
      return;
    }
    const pid   = hiddenAbove[hiddenAbove.length - 1];
    const rows  = document.querySelectorAll(`tr[data-pallet="${pid}"]`);
    const count = rows.length;
    const sep = palletSepRows[pid];
    const originCode = sep ? (sep.dataset.origin || 'Sin origen') : 'Sin origen';
    const processingTime = sep ? (sep.dataset.processingTime || '') : '';
    sticky.innerHTML = palletHeaderHTML(pid, originCode, count, processingTime);
    sticky.classList.add('visible');
    refreshIcons();
  }

  // -- Exportar ------------------------------------------------------------------
  function exportCSV() {
    flash(' Preparando exportación...', 'green');
    const link = document.createElement('a');
    link.href  = '/exportar/';
    link.style.display = 'none';
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
  }

  // -- Inicialización única ------------------------------------------------------
  function initializeQueueUI() {
    document.querySelectorAll('tr.pallet-sep').forEach(sep => {
      const pid = parseInt(sep.dataset.palletId);
      if (pid) {
        palletSepRows[pid] = sep;
        syncPalletTimer(
          pid,
          sep.dataset.processingStartedAt,
          sep.dataset.processingFinishedAt || sep.dataset.receiptDoneAt,
          sep.dataset.processingTime || ''
        );
      }
    });

    initializePhaseToggleCards();
    const confirmAccept = document.getElementById('confirm-toast-accept');
    const confirmCancel = document.getElementById('confirm-toast-cancel');
    if (confirmAccept) confirmAccept.addEventListener('click', () => closeConfirmToast(true));
    if (confirmCancel) confirmCancel.addEventListener('click', () => closeConfirmToast(false));
    installResyncMenuAction();
    updatePhaseCards();
    updateButtons();
    initStickyObserver();
    updateStickyPallet();
    applyLocalUiStatus();
    checkSAP();
    if (scanOutbox.length) {
      scanOutboxHadFailures = true;
      updateScanOutboxStatus();
      scheduleScanOutboxProcessing(500);
    }
    refreshIcons();
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initializeQueueUI);
  } else {
    initializeQueueUI();
  }
