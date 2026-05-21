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
  let palletSepRows = {}; // pallet_id -> tr element
  const _visibleSeps = new Set();
  let _stickyObserver = null;
  const tableWrap = document.getElementById('table-wrap');
  const stickyPallet = document.getElementById('sticky-pallet');
  const QUEUE_TABLE_COLSPAN = 5;
  const EMPTY_QUEUE_MESSAGE = 'Sin HUs en cola - escanea el primer código';

  const initialStats = JSON.parse(
    document.getElementById('initial-stats').textContent || '{}'
  );

  let stats = {
    total:   Number(initialStats.total || 0),
    ok:      Number(initialStats.ok || 0),
    errors:  Number(initialStats.errors || 0),
    pending: Number(initialStats.pending || 0),
    pallets: Number(initialStats.pallets || 0)
  };

  function emptyQueueRowHTML() {
    return `<tr class="empty-row" id="empty-row"><td colspan="${QUEUE_TABLE_COLSPAN}">${EMPTY_QUEUE_MESSAGE}</td></tr>`;
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

  function connectWS() {
    const proto = location.protocol === 'https:' ? 'wss' : 'ws';
    ws = new WebSocket(`${proto}://${location.host}/ws/queue/`);

    ws.onopen = () => {
      console.log('WS conectado');
      wsRetry = 0;
    };

    ws.onclose = () => {
      const delay = Math.min(1000 * 2 ** wsRetry, 30000); // max 30s
      wsRetry++;
      console.warn(`WS cerrado. Reintentando en ${delay/1000}s...`);
      setTimeout(connectWS, delay);
    };

    ws.onerror = e => console.error('WS error', e);

    ws.onmessage = (e) => {
      const msg = JSON.parse(e.data);
      switch (msg.type) {
        case 'initial_state': handleInitialState(msg); break;
        case 'item_update':   handleItemUpdate(msg);   break;
        case 'stats_update':  handleStatsUpdate(msg);  break;
        case 'receipt_done':  handleReceiptDone(msg);  break;
        case 'pallet_created': handlePalletCreated(msg); break;
        case 'queue_done':    handleQueueDone(msg);    break;
        case 'pallet_done':   handlePalletDone(msg);   break;
        case 'error':         handleError(msg);         break;
      }
    };
  }

  connectWS();

  // -- Handlers WebSocket --------------------------------------------------------

  function handleInitialState(msg) {
    const tbody = document.getElementById('queue-tbody');
    tbody.innerHTML = '';
    palletSepRows = {};
    _visibleSeps.clear();
    if (stickyPallet) stickyPallet.classList.remove('visible');

    if (msg.items && msg.items.length > 0) {
      msg.items.forEach(item => updateOrCreateRow(item));
    } else {
      tbody.innerHTML = emptyQueueRowHTML();
    }
    if (msg.stats) handleStatsUpdate(msg.stats);
    updateStickyPallet();
  }

  function handleItemUpdate(msg) {
    updateOrCreateRow(msg);
    updateProgress();

    if (msg.status === 'processing') {
      setProgStatus(`Procesando -> Pallet ${msg.pallet_id}  |  HU: ${msg.hu_code}`, 'running');
      setFooterStatus(`  Procesando HU ${msg.hu_code}…`, 'running');
    } else if (msg.status === 'error' || msg.status === 'hu_not_found') {
      const detail = (msg.phase1_msg || msg.phase2_msg || 'Sin detalles').slice(0, 60);
      setProgStatus(`ERROR en HU ${msg.hu_code}: ${detail}`, 'error');
      setFooterStatus(`Error:  Error en ${msg.hu_code}`, 'error');
    }
  }

  function handleStatsUpdate(msg) {
  stats = {
    total:   Number(msg.total || 0),
    ok:      Number(msg.ok || 0),
    errors:  Number(msg.errors || 0),
    pending: Number(msg.pending || 0),
    pallets: Number(msg.pallets || 0)
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
  if (palLbl) palLbl.textContent = `Pallets activos: ${stats.pallets}`;

  const errCard = document.querySelector('.kpi-card.kpi-err');
  if (errCard) errCard.classList.toggle('has-errors', stats.errors > 0);

  updateProgress();
  updateButtons();
}

  function handleReceiptDone(msg) {
    if (msg.status !== 'ok') {
      setProgStatus(`ZE16/PDF: ${msg.message}`, 'error');
      setFooterStatus(`ZE16/PDF: ${msg.message}`, 'error');
    }
    updateButtons();
  }

  function handlePalletCreated(msg) {
    ensurePalletSep(msg.pallet_id, msg.origin_code || 'Sin origen');
    updatePalletSep(msg.pallet_id);
    if (msg.stats) handleStatsUpdate(msg.stats);
    updateStickyPallet();
  }

  function handleQueueDone(msg) {
    isRunning = false;
    if (msg.status === 'stopped') {
      setProgStatus(msg.message, '');
      setProgBadge('Detenido', '');
      document.getElementById('prog-bar').classList.remove('animated');
      setFooterStatus(msg.message, '');
      updateButtons();
      flash(msg.message, 'orange');
      return;
    }
    const hasErrors = msg.status !== 'ok' || Number(msg.errors || stats.errors || 0) > 0;
    setProgStatus(msg.message, hasErrors ? 'error' : 'done');
    setProgBadge(hasErrors ? 'Completado con errores' : 'Completado', hasErrors ? 'error' : 'done');
    document.getElementById('prog-bar').classList.remove('animated');
    document.getElementById('prog-bar').classList.toggle('done', !hasErrors);
    document.getElementById('prog-bar').classList.toggle('error', hasErrors);
    setFooterStatus(msg.message, hasErrors ? 'error' : 'done');
    updateButtons();
    flash(msg.message, hasErrors ? 'orange' : 'green');
    setTimeout(() => alert(msg.message), 200);
  }

  function handlePalletDone(msg) {
    updatePalletSep(msg.pallet_id);
  }

  function handleError(msg) {
    isRunning = false;
    setProgBadge('Error: ERROR', 'error');
    document.getElementById('prog-bar').classList.remove('animated');
    document.getElementById('prog-bar').classList.add('error');
    setFooterStatus('Error:  Error en el proceso.', 'error');
    updateButtons();
    setTimeout(() => alert(`Error SAP:\n\n${msg.message}`), 200);
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

    if (col === 'f1') {
      if (status === 'ok' || status === 'duplicate')
        return `<span class="ok-cell">${display || 'OK'}</span>`;
      if (status === 'error' || status === 'hu_not_found')
        return `<span style="color:var(--red)">${display || 'Error'}</span>`;
      const labels = { pending:'Pendiente', processing:'Procesando...' };
      return `<span class="cell-status status-${status}">${display || labels[status] || status}</span>`;
    }

    // F2
    if (status === 'ok')
      return `<span class="ok-cell">${display || 'OK'}</span>`;
    if (status === 'error' && !phase2_msg)
      return `<span style="color:var(--muted)">- omitido</span>`;
    if (status === 'error')
      return `<span style="color:var(--red)">${display || 'Error'}</span>`;
    return display || '';
  }

  function updateOrCreateRow(item) {
    const existing = document.getElementById(`row-${item.hu_code}`);

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
      tr.scrollIntoView({ block: 'nearest' });
    } else {
      existing.dataset.status = item.status;
      existing.cells[1].innerHTML = originBadgeHTML(item.origin_code);
      existing.cells[3].innerHTML = statusCellHTML(item.status, item.f1_display, item.f2_display, item.phase2_msg, 'f1');
      existing.cells[4].innerHTML = statusCellHTML(item.status, item.f1_display, item.f2_display, item.phase2_msg, 'f2');
    }

    updatePalletSep(item.pallet_id);
  }

  function ensurePalletSep(palletId, originCode) {
    if (palletSepRows[palletId]) return;
    const tbody = document.getElementById('queue-tbody');
    const sep = document.createElement('tr');
    sep.className = 'pallet-sep';
    sep.dataset.palletId = palletId;
    const pid = String(palletId).padStart(2, '0');
    sep.innerHTML = `<td colspan="${QUEUE_TABLE_COLSPAN}" id="sep-${palletId}">-- Pallet P${pid} - ${originCode || 'Sin origen'} - 0 HUs --</td>`;
    palletSepRows[palletId] = sep;
    tbody.appendChild(sep);
    _observeSep(sep);
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

    const pid = String(palletId).padStart(2, '0');
    td.innerHTML = `-- Pallet P${pid} - ${originCode} - <strong style="color:var(--text)">${count} HU${count !== 1 ? 's' : ''}</strong> ---`;
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
      const pid = String(lastHiddenSep.pid).padStart(2, '0');
      stickyPallet.textContent = `-- Pallet P${pid} - ${count} HU${count !== 1 ? 's' : ''} ---`;
      stickyPallet.classList.add('visible');
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
    if (total === 0) bar.classList.remove('animated', 'done', 'error');
  }

  function setProgBadge(text, mode) {
    const b = document.getElementById('prog-badge');
    b.textContent = text;
    b.className = 'prog-badge ' + (mode || '');
  }

  function setProgStatus(text, mode) {
    const s = document.getElementById('prog-status');
    s.textContent = text;
    const colors = { running: 'var(--blue)', error: 'var(--red)', done: 'var(--green-text)' };
    s.style.color      = colors[mode] || 'var(--muted)';
    s.style.fontWeight = mode ? 'bold' : 'normal';
  }

  function setFooterStatus(text, mode) {
    const el = document.getElementById('footer-status');
    el.textContent = text;
    el.className = 'footer-status ' + (mode || '');
  }

  // -- Botones -------------------------------------------------------------------
  function updateButtons() {
    const hasPending   = stats.pending > 0;
    const hasProcessed = stats.ok > 0 || stats.errors > 0;
    document.getElementById('btn-start').disabled     = isRunning || !hasPending;
    document.getElementById('btn-stop').disabled      = !isRunning;
    document.getElementById('btn-clear').disabled     = isRunning;
    document.getElementById('btn-reprocess').disabled = isRunning || hasPending || !hasProcessed;
    const newPalletBtn = document.getElementById('btn-new-pallet');
    if (newPalletBtn) newPalletBtn.disabled = isRunning;

    // Deshabilitar checkboxes durante ejecución
    ['chk-f1','chk-f2','chk-auto'].forEach(id => {
      const el = document.getElementById(id);
      if (el) el.disabled = isRunning;
    });
  }

  function ensureEmptyQueueMessage() {
    const tbody = document.getElementById('queue-tbody');
    if (!tbody.querySelector('tr[data-hu]') && !tbody.querySelector('#empty-row')) {
      tbody.innerHTML = emptyQueueRowHTML();
    }
  }

  // -- Scan ----------------------------------------------------------------------
  document.getElementById('scan-input').addEventListener('keydown', async (e) => {
    if (e.key !== 'Enter') return;
    const raw = e.target.value.trim();
    e.target.value = '';
    if (!raw) return;

    // Separador de pallet
    if (isPalletSeparator(raw)) {
      newPallet();
      return;
    }

    // Validar longitud (igual que PyQt: 10-15 chars)
    if (raw.length < 10 || raw.length > 15) {
      setHint(`Error: Código inválido: '${raw}' tiene ${raw.length} chars. Rango: 10-15`, 'warn');
      flash(`Error: Código HU inválido: ${raw.length} caracteres`, 'orange');
      return;
    }

    const res = await fetch('/scan/', {
      method: 'POST',
      headers: csrfHeaders({ 'Content-Type': 'application/json' }),
      body: JSON.stringify({
        code:   raw,
        run_f1: document.getElementById('chk-f1').checked,
        run_f2: document.getElementById('chk-f2').checked,
      }),
    });

    const data = await readJsonResponse(res);
    if (data.ok) {
      setHint(`OK  ${raw}  ->  Pallet ${data.pallet_id}  [${data.origin}]`, 'ok');
      flash(`OK  ${raw}  ->  Pallet ${data.pallet_id}  [${data.origin}]`, 'green');
      if (data.stats) handleStatsUpdate(data.stats);
      // Auto-start solo si chk-auto está activado Y el sistema está completamente inactivo
      if (!isRunning && document.getElementById('chk-auto').checked && (stats.pending > 0 || data.ok)) {
        // Auto-iniciar SOLO una vez después de agregar el primer HU
        if (!window._autoStarted) {
          window._autoStarted = true;
          setTimeout(() => {
            startProcess();
            // Resetear bandera tras 2 segundos (para permitir nuevo auto-start después)
            setTimeout(() => { window._autoStarted = false; }, 2000);
          }, 500);
        }
      }
    } else {
      const color = data.type === 'duplicate' ? 'warn' : 'error';
      setHint(`Error: ${data.error}`, color);
      flash(`Error: ${data.error}`, data.type === 'duplicate' ? 'orange' : 'red');
    }
  });

  // -- Nuevo pallet --------------------------------------------------------------
  async function newPallet() {
    if (isRunning) {
      flash('No se puede crear pallet durante el proceso.', 'orange');
      return;
    }
    const res  = await fetch('/pallet/nuevo/', {
      method: 'POST',
      headers: { ...csrfHeaders() }
    });
    const data = await readJsonResponse(res);
    if (data.ok) {
      setHint(`OK ${data.message}`, 'ok');
      flash(`OK ${data.message}`, 'green');
      if (data.stats) handleStatsUpdate(data.stats);
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
    if (stats.pending === 0) {
      flash('Error: No hay HUs pendientes.', 'orange');
      return;
    }

    if (!(await ensureSapReady())) return;

    isRunning = true;
    setProgBadge('PROCESANDO', 'running');
    setProgStatus('Enviando HUs a procesar...', 'running');
    setFooterStatus('  Procesando…', 'running');
    document.getElementById('prog-bar').classList.add('animated');
    document.getElementById('prog-bar').classList.remove('done', 'error');
    updateButtons();

    try {
      const res = await fetch('/api/procesar/', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          ...csrfHeaders()
        },
        body: JSON.stringify({
          run_f1: document.getElementById('chk-f1').checked,
          run_f2: document.getElementById('chk-f2').checked,
        }),
      });
      const data = await readJsonResponse(res);

      if (!data.ok) {
        flash(`Error: ${data.error}`, 'red');
        isRunning = false;
        setProgBadge('EN ESPERA', '');
        setProgStatus('Error al iniciar.', '');
        document.getElementById('prog-bar').classList.remove('animated', 'done', 'error');
        updateButtons();
        return;
      }

      flash(` ${data.count} HUs enviadas a Celery`, 'green');

    } catch (e) {
      flash(`Error: Error: ${e.message}`, 'red');
      isRunning = false;
      setProgBadge('EN ESPERA', '');
      setProgStatus('Error al iniciar.', 'error');
      document.getElementById('prog-bar').classList.remove('animated');
      document.getElementById('prog-bar').classList.add('error');
      updateButtons();
    }
  }

  // -- Detener proceso -----------------------------------------------------------
  async function stopProcess() {
    try {
      const res = await fetch('/cola/detener/', {
        method: 'POST',
        headers: { ...csrfHeaders() }
      });
      const data = await readJsonResponse(res);
      if (!data.ok) {
        isRunning = false;
        flash(data.error || 'No hay proceso activo.', 'orange');
        updateButtons();
        return;
      }
      setProgBadge('Deteniendo', 'running');
      setProgStatus(data.message || 'Detencion solicitada.', 'running');
      setFooterStatus('Deteniendo en punto seguro...', 'running');
      flash(data.message || 'Detencion solicitada.', 'orange');
    } catch (e) {
      flash(`Error al detener: ${e.message}`, 'red');
    }
    updateButtons();
  }
  // -- Limpiar -------------------------------------------------------------------
  async function clearQueue() {
    if (isRunning) {
      flash('No se puede limpiar mientras el proceso esta activo.', 'orange');
      return;
    }
    if (!confirm('Esto eliminará todas las HUs.\n¿Continuar?')) return;
    const res  = await fetch('/cola/limpiar/', {
      method: 'POST',
      headers: { ...csrfHeaders() }
    });
    const data = await readJsonResponse(res);
    if (data.ok) {
      document.getElementById('queue-tbody').innerHTML = emptyQueueRowHTML();
      Object.keys(palletSepRows).forEach(k => delete palletSepRows[k]);
      _visibleSeps.clear();
      if (stickyPallet) stickyPallet.classList.remove('visible');
      isRunning = false;
      stats = { total:0, ok:0, errors:0, pending:0, pallets:0 };
      handleStatsUpdate(stats);
      setProgBadge('EN ESPERA', '');
      setProgStatus('Cola limpiada. Listo para escanear.', '');
      setFooterStatus('En espera.', '');
      document.getElementById('prog-bar').classList.remove('animated','done','error');
      flash('OK Cola limpiada.', 'green');
      document.getElementById('scan-input').focus();
    } else {
      flash(`Error: ${data.error}`, 'red');
    }
  }

  // -- Reprocesar ----------------------------------------------------------------
  async function reprocess() {
    if (isRunning) return;
    if (stats.pending > 0 || ((stats.ok + stats.errors) === 0 && !hasErrorRows())) {
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

    if (!(await ensureSapReady())) return;

    const isErrorsOnly = mode === 'errors';
    const message = isErrorsOnly
      ? 'Marcará solo los HUs con error como Pendientes para reprocesar.\n¿Continuar?'
      : 'Marcará todos los HUs procesados como Pendientes para reprocesar.\n¿Continuar?';
    if (!confirm(message)) return;

    const res  = await fetch('/cola/reprocesar/', {
      method: 'POST',
      headers: csrfHeaders({ 'Content-Type': 'application/json' }),
      body: JSON.stringify({ mode })
    });
    const data = await readJsonResponse(res);

    if (data.ok) {
      flash('OK ' + data.count + ' HUs marcados como pendientes.', 'green');
      const selector = isErrorsOnly
        ? '#queue-tbody tr[data-status="error"], #queue-tbody tr[data-status="hu_not_found"]'
        : '#queue-tbody tr[data-hu]';

      document.querySelectorAll(selector).forEach(tr => {
        if (tr.cells[3]) tr.cells[3].innerHTML = '<span class="cell-status status-pending">Pendiente</span>';
        if (tr.cells[4]) tr.cells[4].innerHTML = '';
        tr.dataset.status = 'pending';
      });

      if (isErrorsOnly) {
        stats.pending += data.count;
        stats.errors = Math.max(0, stats.errors - data.count);
      } else {
        stats.ok = 0;
        stats.errors = 0;
        stats.pending = stats.total;
      }

      updateProgress();
      isRunning = true;
      setProgBadge('PROCESANDO', 'running');
      setProgStatus('Reprocesando HUs...', 'running');
      setFooterStatus('Procesando reproceso...', 'running');
      document.getElementById('prog-bar').classList.add('animated');
      document.getElementById('prog-bar').classList.remove('done','error');
      updateButtons();
    } else {
      flash('Error: ' + data.error, 'red');
    }
  }
  async function deleteHU(huCode) {
    if (isRunning) {
      flash('No se puede borrar mientras el proceso esta activo.', 'orange');
      return;
    }
    const row = document.getElementById(`row-${huCode}`);
    const currentStatus = row.dataset.status || '';
    if (!canDeleteStatus(currentStatus)) {
      flash('Esta HU ya no se puede borrar.', 'orange');
      return;
    }
    const res  = await fetch(`/hu/${huCode}/borrar/`, {
      method: 'POST',
      headers: { ...csrfHeaders() }
    });
    const data = await readJsonResponse(res);
    if (data.ok) {
      if (row) {
        const palletId = parseInt(row.dataset.pallet);
        row.remove();
        adjustStatsForDeletedStatus(currentStatus);
        if (data.pallet_deleted) {
          const sep = document.querySelector(`tr.pallet-sep[data-pallet-id="${palletId}"]`);
          if (sep) sep.remove();
          delete palletSepRows[palletId];
          stats.pallets = Math.max(0, stats.pallets - 1);
          handleStatsUpdate(stats);
        } else {
          updatePalletSep(palletId);
        }
        updateStickyPallet();
        ensureEmptyQueueMessage();
        updateButtons();
      }
      flash(`OK HU ${huCode} eliminada.`, 'green');
    } else {
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
      return await res.json();
    }

    const text = await res.text();
    const titleMatch = text.match(/<title>(.*)<\/title>/i);
    const title = titleMatch ? titleMatch[1].replace(/\s+/g, ' ').trim() : '';
    return {
      ok: false,
      error: title || `Error HTTP ${res.status}`,
      status: res.status,
    };
  }

  // -- SAP Status polling (equivale al QTimer de _check_sap) --------------------
  async function checkSAP() {
    try {
      const res  = await fetch('/api/sap-status/');
      const data = await readJsonResponse(res);
      document.getElementById('conn-dot').className   = `conn-dot ${data.connected ? 'ok' : ''}`;
      document.getElementById('conn-label').textContent = data.connected
        ? `CONECTADO${data.user ? ' - ' + data.user : ''}`
        : 'Sin sesión SAP';
    } catch { /* silencioso */ }
  }
  setInterval(checkSAP, 5000);

  async function ensureSapReady() {
    try {
      const res = await fetch('/api/sap-status/');
      const data = await readJsonResponse(res);
      if (data.connected) return true;
    } catch {}

    flash('Inicializando SAP e iniciando sesion...', 'orange');
    setProgBadge('SAP', 'running');
    setProgStatus('Inicializando SAP e iniciando sesion...', 'running');
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
      setProgStatus(data.error || 'SAP sin sesion activa.', 'error');
      setFooterStatus('SAP sin sesion activa.', 'error');
      return false;
    } catch (e) {
      flash(`Error SAP: ${e.message}`, 'red');
      setProgBadge('EN ESPERA', '');
      setProgStatus('No se pudo inicializar SAP.', 'error');
      setFooterStatus('SAP sin sesion activa.', 'error');
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

  // -- Scan hint -----------------------------------------------------------------
  let _hintTimer = null;
  function setHint(msg, type) {
    const el = document.getElementById('scan-hint');
    el.textContent = msg;
    el.className = `scan-hint ${type}`;
    clearTimeout(_hintTimer);
    _hintTimer = setTimeout(() => {
      el.textContent = 'Presiona Enter para confirmar - Escribe PALLET para separar';
      el.className = 'scan-hint idle';
    }, 2500);
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
      flash('No se puede borrar mientras el proceso esta activo.', 'orange');
      return;
    }
    if (!confirm(`¿Borrar el pallet P${String(_ctxTargetPallet).padStart(2,'0')} completo`)) return;

    const res  = await fetch(`/pallet/${_ctxTargetPallet}/borrar/`, {
      method: 'POST',
      headers: { ...csrfHeaders() },
    });
    const data = await readJsonResponse(res);
    if (data.ok) {
      // Quitar separador y filas asociadas del DOM
      const sep = document.querySelector(`tr.pallet-sep[data-pallet-id="${_ctxTargetPallet}"]`);
      document.querySelectorAll(`tr[data-pallet="${_ctxTargetPallet}"]`).forEach(r => {
        adjustStatsForDeletedStatus(r.dataset.status || '');
        r.remove();
      });
      if (sep) sep.remove();
      delete palletSepRows[_ctxTargetPallet];
      stats.pallets = Math.max(0, stats.pallets - 1);
      handleStatsUpdate(stats);
      updateStickyPallet();
      ensureEmptyQueueMessage();
      updateButtons();
      flash(`OK Pallet borrado.`, 'green');
    } else {
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
    sticky.textContent = `-- Pallet P${String(pid).padStart(2,'0')} - ${count} HU${count !== 1 ? 's' : ''} ---`;
    sticky.classList.add('visible');
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
      if (pid) palletSepRows[pid] = sep;
    });

    updatePhaseCards();
    updateButtons();
    initStickyObserver();
    updateStickyPallet();
    checkSAP();
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initializeQueueUI);
  } else {
    initializeQueueUI();
  }
