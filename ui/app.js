/* ══════════════════════════════════════
   Kur — App Logic + Settings
   ══════════════════════════════════════ */

const API_URL = window.location.hostname === 'localhost'
    ? 'http://localhost:8000' : '';

const $ = (sel) => document.querySelector(sel);
const $$ = (sel) => document.querySelectorAll(sel);

let isLoading = false;
let historyItems = [];
let loadingStepTimer = null;

// ──── Models per Provider ────
const PROVIDER_MODELS = {
    openai: ['gpt-4o', 'gpt-4o-mini', 'gpt-4.1', 'gpt-4.1-mini', 'gpt-3.5-turbo'],
    groq: ['llama-3.3-70b-versatile', 'llama-3.1-8b-instant', 'mixtral-8x7b-32768', 'gemma2-9b-it'],
    gemini: ['gemini-2.5-pro', 'gemini-2.5-flash', 'gemini-2.0-flash', 'gemini-1.5-pro'],
    ollama: ['snowflake-arctic-text2sql-r1:7b', 'llama3.1:8b', 'codellama:7b', 'custom'],
};

// ──── Init ────
document.addEventListener('DOMContentLoaded', () => {
    loadSuggestions();
    loadHealth();
    loadSettings();
    loadHistory();
    setupInput();
    setupSidebar();
    setupSettings();
});

// ──── API ────
async function loadSuggestions() {
    try {
        const res = await fetch(`${API_URL}/api/suggestions`);
        const data = await res.json();
        const container = $('#suggestions');
        const welcomeCards = $('#welcomeCards');

        data.suggestions.slice(0, 6).forEach(q => {
            const btn = document.createElement('button');
            btn.className = 'suggestion-item';
            btn.textContent = q;
            btn.onclick = () => askQuestion(q);
            container.appendChild(btn);

            const card = document.createElement('div');
            card.className = 'welcome-card';
            card.textContent = q;
            card.onclick = () => askQuestion(q);
            welcomeCards.appendChild(card);
        });
    } catch { /* ignore */ }
}

async function loadHealth() {
    try {
        const res = await fetch(`${API_URL}/api/health`);
        const data = await res.json();
        const info = $('#dbInfo');
        info.innerHTML = `<span class="db-stat">${data.status}</span><br>${data.tables} tables · ${data.engine || 'DuckDB'}`;
        if (data.engine) $('#engineBadge').textContent = data.engine;
    } catch {
        $('#dbInfo').textContent = 'Offline';
    }
}

// ──── Input ────
function setupInput() {
    const input = $('#questionInput');
    const sendBtn = $('#sendBtn');

    input.addEventListener('input', () => {
        sendBtn.disabled = !input.value.trim();
        input.style.height = 'auto';
        input.style.height = Math.min(input.scrollHeight, 120) + 'px';
    });

    input.addEventListener('keydown', (e) => {
        if (e.key === 'Enter' && !e.shiftKey) {
            e.preventDefault();
            if (input.value.trim() && !isLoading) askQuestion(input.value.trim());
        }
    });

    sendBtn.addEventListener('click', () => {
        if (input.value.trim() && !isLoading) askQuestion(input.value.trim());
    });
}

function setupSidebar() {
    $('#sidebarToggle').onclick = () => $('#sidebar').classList.toggle('open');
    $('#newChatBtn').onclick = () => {
        $('#messages').innerHTML = '';
        $('#welcome').style.display = 'flex';
    };
}

// ══════════════════════════════
//  SETTINGS
// ══════════════════════════════

function setupSettings() {
    // Open/close
    $('#settingsBtn').onclick = () => { $('#settingsOverlay').classList.add('open'); loadSettings(); };
    $('#closeSettingsBtn').onclick = closeSettings;
    $('#settingsOverlay').onclick = (e) => { if (e.target === $('#settingsOverlay')) closeSettings(); };
    document.addEventListener('keydown', (e) => { if (e.key === 'Escape') closeSettings(); });

    // Provider change → update model list + field visibility
    $('#sLlmProvider').onchange = () => {
        const prov = $('#sLlmProvider').value;
        updateModelList(prov);
        toggleProviderFields(prov);
    };

    // Engine change → field visibility
    $('#sDbEngine').onchange = () => {
        toggleEngineFields($('#sDbEngine').value);
    };

    // Tabs logic
    const tabs = $$('.settings-tab');
    const panes = $$('.settings-tab-pane');
    tabs.forEach(tab => {
        tab.onclick = () => {
            tabs.forEach(t => t.classList.remove('active'));
            panes.forEach(p => p.classList.remove('active'));
            tab.classList.add('active');
            $(`#${tab.dataset.target}`).classList.add('active');
        };
    });

    // Peek key
    $('#peekKeyBtn').onclick = () => {
        const inp = $('#sApiKey');
        inp.type = inp.type === 'password' ? 'text' : 'password';
    };

    // Save / Reset
    $('#saveSettingsBtn').onclick = saveSettings;
    $('#resetSettingsBtn').onclick = resetSettings;
}

function closeSettings() {
    $('#settingsOverlay').classList.remove('open');
}

function updateModelList(provider) {
    const select = $('#sModel');
    const models = PROVIDER_MODELS[provider] || [];
    select.innerHTML = '';
    models.forEach(m => {
        const opt = document.createElement('option');
        opt.value = m;
        opt.textContent = m;
        select.appendChild(opt);
    });
}

function toggleProviderFields(provider) {
    const isOllama = provider === 'ollama';
    const needsKey = provider !== 'ollama';

    $('#sApiKeyField').classList.toggle('hidden', !needsKey);
    $('#sModelField').classList.toggle('hidden', false);
    $('#sOllamaUrlField').classList.toggle('hidden', !isOllama);
    $('#sOllamaModelField').classList.toggle('hidden', !isOllama);

    // Update placeholder for API key
    const kp = { openai: 'sk-...', groq: 'gsk_...', gemini: 'AIza...' };
    if (needsKey) $('#sApiKey').placeholder = kp[provider] || 'API key';
}

function toggleEngineFields(engine) {
    const isDuckdb = engine === 'duckdb';
    $('#sDuckdbPathField').classList.toggle('hidden', !isDuckdb);
    ['sDbHostPortField', 'sDbNameField', 'sDbUserField', 'sDbPasswordField'].forEach(id => {
        const el = $(`#${id}`);
        if (el) el.classList.toggle('hidden', isDuckdb);
    });

    // Default ports
    const ports = { postgres: 5432, trino: 8080, clickhouse: 8123 };
    if (!isDuckdb) $('#sDbPort').value = ports[engine] || 5432;
}

async function loadSettings() {
    try {
        const res = await fetch(`${API_URL}/api/settings`);
        const s = await res.json();

        // LLM
        $('#sLlmProvider').value = s.llm_provider || 'openai';
        updateModelList(s.llm_provider || 'openai');
        toggleProviderFields(s.llm_provider || 'openai');
        if (s.llm_model) $('#sModel').value = s.llm_model;
        if (s.api_key_masked) $('#sApiKey').placeholder = s.api_key_masked;
        if (s.ollama_url) $('#sOllamaUrl').value = s.ollama_url;
        if (s.ollama_model) $('#sOllamaModel').value = s.ollama_model;

        // DB
        $('#sDbEngine').value = s.db_engine || 'duckdb';
        toggleEngineFields(s.db_engine || 'duckdb');
        if (s.duckdb_path) $('#sDuckdbPath').value = s.duckdb_path;
        if (s.db_host) $('#sDbHost').value = s.db_host;
        if (s.db_port) $('#sDbPort').value = s.db_port;
        if (s.db_name) $('#sDbName').value = s.db_name;
        if (s.db_user) $('#sDbUser').value = s.db_user;

        // UC
        if (s.uc_url) $('#sUcUrl').value = s.uc_url;
        if (s.uc_catalog) $('#sUcCatalog').value = s.uc_catalog;
        if (s.uc_schema) $('#sUcSchema').value = s.uc_schema;

        // Agent
        if (s.max_retries !== undefined) $('#sMaxRetries').value = s.max_retries;
        if (s.query_timeout !== undefined) $('#sQueryTimeout').value = s.query_timeout;
        if (s.max_rows !== undefined) $('#sMaxRows').value = s.max_rows;
        if (s.language) $('#sLanguage').value = s.language;

        // Update header badges
        $('#engineBadge').textContent = (s.db_engine || 'duckdb').toUpperCase();
        $('#providerBadge').textContent = (s.llm_provider || 'openai').charAt(0).toUpperCase() + (s.llm_provider || 'openai').slice(1);

    } catch { /* first load, no settings yet */ }
}

async function saveSettings() {
    const status = $('#settingsStatus');
    status.textContent = 'Saving...';
    status.className = 'settings-status';

    const payload = {
        llm_provider: $('#sLlmProvider').value,
        llm_model: $('#sModel').value,
        ollama_url: $('#sOllamaUrl').value || 'http://localhost:11434',
        ollama_model: $('#sOllamaModel').value,

        db_engine: $('#sDbEngine').value,
        duckdb_path: $('#sDuckdbPath').value,
        db_host: $('#sDbHost').value,
        db_port: parseInt($('#sDbPort').value) || 5432,
        db_name: $('#sDbName').value,
        db_user: $('#sDbUser').value,

        uc_url: $('#sUcUrl').value,
        uc_catalog: $('#sUcCatalog').value,
        uc_schema: $('#sUcSchema').value,

        max_retries: parseInt($('#sMaxRetries').value) || 3,
        query_timeout: parseInt($('#sQueryTimeout').value) || 30,
        max_rows: parseInt($('#sMaxRows').value) || 1000,
        language: $('#sLanguage').value,
    };

    // Only send API key if user typed a new one
    const apiKeyVal = $('#sApiKey').value.trim();
    if (apiKeyVal) payload.api_key = apiKeyVal;

    // Only send DB password if typed
    const dbPwVal = $('#sDbPassword').value.trim();
    if (dbPwVal) payload.db_password = dbPwVal;

    try {
        const res = await fetch(`${API_URL}/api/settings`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload),
        });

        if (res.ok) {
            status.textContent = '✅ Đã lưu!';
            status.className = 'settings-status success';
            // Clear sensitive inputs after save
            $('#sApiKey').value = '';
            $('#sDbPassword').value = '';
            // Update header badges
            $('#engineBadge').textContent = payload.db_engine.toUpperCase();
            const provName = payload.llm_provider;
            $('#providerBadge').textContent = provName.charAt(0).toUpperCase() + provName.slice(1);
            // Refresh health
            loadHealth();
        } else {
            const err = await res.json();
            status.textContent = `❌ ${err.detail || 'Error'}`;
            status.className = 'settings-status error';
        }
    } catch (e) {
        status.textContent = `❌ ${e.message}`;
        status.className = 'settings-status error';
    }

    setTimeout(() => { status.textContent = ''; }, 4000);
}

async function resetSettings() {
    if (!confirm('Reset tất cả settings về mặc định?')) return;
    try {
        await fetch(`${API_URL}/api/settings/reset`, { method: 'POST' });
        loadSettings();
        const status = $('#settingsStatus');
        status.textContent = '🔄 Reset thành công';
        status.className = 'settings-status success';
        setTimeout(() => { status.textContent = ''; }, 3000);
    } catch { /* ignore */ }
}

// ══════════════════════════════
//  CHAT
// ══════════════════════════════

async function askQuestion(question) {
    if (isLoading) return;
    isLoading = true;

    const input = $('#questionInput');
    input.value = '';
    input.style.height = 'auto';
    $('#sendBtn').disabled = true;
    $('#welcome').style.display = 'none';

    addMessage('user', question);
    const loadingEl = addLoading();

    try {
        const res = await fetch(`${API_URL}/api/ask`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ question }),
        });
        const data = await res.json();
        stopLoadingSteps();
        loadingEl.remove();
        addAssistantMessage(data);
    } catch (err) {
        stopLoadingSteps();
        loadingEl.remove();
        addMessage('assistant', `<div class="error-block">❌ Không thể kết nối server: ${err.message}</div>`);
    } finally {
        loadHistory();
    }

    isLoading = false;
}

function addMessage(role, html) {
    const container = $('#messages');
    const avatar = role === 'user' ? '👤' : '🔮';
    const sender = role === 'user' ? 'Bạn' : 'Kur';

    const el = document.createElement('div');
    el.className = `message ${role}`;
    el.innerHTML = `
        <div class="message-content">
            <div class="message-header">
                <div class="message-avatar">${avatar}</div>
                <span class="message-sender">${sender}</span>
            </div>
            <div class="message-body">${html}</div>
        </div>`;
    container.appendChild(el);
    scrollToBottom();
}

function addAssistantMessage(data, opts = {}) {
    const suppressSql = !!opts.suppressSql;
    const showSql = $('#showSqlToggle').checked;
    let bodyHtml = '';

    if (data.sql && showSql && !suppressSql) {
        bodyHtml += `
            <div class="sql-block">
                <div class="sql-header">
                    <span class="sql-label">SQL</span>
                    <button class="copy-btn" onclick="copySQL(this, \`${escapeHtml(data.sql)}\`)">Copy</button>
                </div>
                <div class="sql-code">${escapeHtml(data.sql)}</div>
            </div>`;
    }

    if (data.data && data.columns && data.data.length > 0) {
        const maxRows = Math.min(data.data.length, 100);
        bodyHtml += `
            <div class="data-table-wrapper">
                <div class="data-table-header">
                    <span class="sql-label">Kết quả</span>
                    <span class="data-table-info">${data.data.length} dòng</span>
                </div>
                <div class="data-table-scroll">
                    <table class="data-table">
                        <thead><tr>${data.columns.map(c => `<th>${escapeHtml(c)}</th>`).join('')}</tr></thead>
                        <tbody>${data.data.slice(0, maxRows).map(row =>
            `<tr>${data.columns.map(c => `<td>${formatCell(row[c])}</td>`).join('')}</tr>`
        ).join('')}</tbody>
                    </table>
                </div>
            </div>`;
    }

    if (data.answer) bodyHtml += `<p>${escapeHtml(data.answer)}</p>`;
    if (data.requires_approval && data.request_id) {
        bodyHtml += `
            <div class="approval-actions">
                <button class="btn-primary" onclick="executePreparedQuery('${escapeHtml(data.request_id)}', this)">Allow chạy query</button>
                <button class="btn-secondary" onclick="skipPreparedQuery(this)">Skip</button>
            </div>`;
    }
    if (data.error && !data.answer) bodyHtml += `<div class="error-block">${escapeHtml(data.error)}</div>`;
    if (data.latency_ms) bodyHtml += `<div class="latency">${data.latency_ms}ms${data.retries > 0 ? ` · ${data.retries} retries` : ''}</div>`;

    addMessage('assistant', bodyHtml);
}

async function executePreparedQuery(requestId, btn) {
    btn.disabled = true;
    btn.textContent = 'Đang chạy...';

    try {
        const res = await fetch(`${API_URL}/api/execute`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ request_id: requestId }),
        });
        const data = await res.json();
        addAssistantMessage(data, { suppressSql: true });
    } catch (err) {
        addMessage('assistant', `<div class="error-block">❌ Execute lỗi: ${err.message}</div>`);
    }
}

function skipPreparedQuery(btn) {
    const wrap = btn.closest('.approval-actions');
    if (wrap) {
        wrap.innerHTML = '<span class="trace-step">Đã bỏ qua chạy query.</span>';
    }
}

function addLoading() {
    const container = $('#messages');
    const el = document.createElement('div');
    el.className = 'message assistant';
    el.innerHTML = `
        <div class="message-content">
            <div class="message-header">
                <div class="message-avatar">🔮</div>
                <span class="message-sender">Kur</span>
            </div>
            <div class="message-body">
                <div class="loading">
                    <div class="loading-dot"></div>
                    <div class="loading-dot"></div>
                    <div class="loading-dot"></div>
                </div>
                <div class="loading-step" id="loadingStepText">Đang phân loại intent...</div>
            </div>
        </div>`;
    container.appendChild(el);
    scrollToBottom();
    startLoadingSteps(el);
    return el;
}

function startLoadingSteps(rootEl) {
    const steps = [
        'Đang phân loại intent...',
        'Đang lấy schema ngữ cảnh...',
        'Đang tìm ví dụ SQL tương tự...',
        'Đang sinh SQL...',
        'Đang validate SQL...',
        'Đang chạy query...',
        'Đang tổng hợp câu trả lời...'
    ];
    let idx = 0;
    const textEl = rootEl.querySelector('#loadingStepText');
    if (!textEl) return;

    textEl.textContent = steps[idx];
    loadingStepTimer = setInterval(() => {
        idx = (idx + 1) % steps.length;
        textEl.textContent = steps[idx];
    }, 1100);
}

function stopLoadingSteps() {
    if (loadingStepTimer) {
        clearInterval(loadingStepTimer);
        loadingStepTimer = null;
    }
}

function renderHistory() {
    const container = $('#history');
    container.innerHTML = '';
    historyItems.slice(0, 15).forEach(item => {
        const el = document.createElement('div');
        el.className = 'history-item';
        el.textContent = item.question;
        el.onclick = () => populateInput(item.question);
        container.appendChild(el);
    });
}

async function loadHistory() {
    try {
        const res = await fetch(`${API_URL}/api/history?limit=30`);
        const data = await res.json();
        historyItems = Array.isArray(data.items) ? data.items : [];
        renderHistory();
    } catch {
        historyItems = [];
        renderHistory();
    }
}

function populateInput(question) {
    const input = $('#questionInput');
    input.value = question || '';
    input.dispatchEvent(new Event('input'));
    input.focus();
}

// ──── Utils ────
function scrollToBottom() {
    const container = $('#chatContainer');
    requestAnimationFrame(() => container.scrollTop = container.scrollHeight);
}

function escapeHtml(str) {
    if (str === null || str === undefined) return '';
    return String(str).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;').replace(/'/g, '&#39;');
}

function formatCell(val) {
    if (val === null || val === undefined) return '<span style="color:var(--text-muted)">NULL</span>';
    if (typeof val === 'number') {
        return val % 1 === 0 ? val.toLocaleString('vi-VN') : val.toLocaleString('vi-VN', { maximumFractionDigits: 2 });
    }
    return escapeHtml(String(val));
}

function copySQL(btn, sql) {
    navigator.clipboard.writeText(sql.replace(/&amp;/g, '&').replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/&quot;/g, '"'));
    btn.textContent = 'Copied!';
    btn.classList.add('copied');
    setTimeout(() => { btn.textContent = 'Copy'; btn.classList.remove('copied'); }, 2000);
}
