(() => {
    const app = window.KurApp;
    const { $, API_URL, state } = app;

    function scrollToBottom() {
        const container = $('#chatContainer');
        if (!container) return;
        requestAnimationFrame(() => {
            container.scrollTop = container.scrollHeight;
        });
    }

    function escapeHtml(str) {
        if (str === null || str === undefined) return '';
        return String(str)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;')
            .replace(/'/g, '&#39;');
    }

    function formatCell(val) {
        if (val === null || val === undefined) return '<span style="color:var(--text-muted)">NULL</span>';
        if (typeof val === 'number') {
            return val % 1 === 0
                ? val.toLocaleString('vi-VN')
                : val.toLocaleString('vi-VN', { maximumFractionDigits: 2 });
        }
        return escapeHtml(String(val));
    }

    function addMessage(role, html) {
        const container = $('#messages');
        const el = document.createElement('div');
        el.className = `message ${role}`;
        el.innerHTML = `
        <div class="message-content">
            <div class="message-body">${html}</div>
        </div>`;
        container.appendChild(el);
        scrollToBottom();
    }

    function renderSqlBlock(sqlText) {
        const encodedSql = encodeURIComponent(String(sqlText || ''));
        return `
            <details class="sql-card" open>
                <summary class="sql-summary">
                    <span class="summary-left">
                        <i data-lucide="database" class="icon-18"></i>
                        <span class="sql-label">SQL Query</span>
                    </span>
                    <span class="summary-right">
                        <button class="copy-btn" onclick="copySQL(this, '${encodedSql}')">Copy</button>
                        <i data-lucide="chevron-down" class="summary-caret"></i>
                    </span>
                </summary>
                <div class="sql-code">${escapeHtml(sqlText)}</div>
            </details>`;
    }

    function renderDataTable(data) {
        const rows = Array.isArray(data.data) ? data.data : [];
        const columns = Array.isArray(data.columns) ? data.columns : [];
        if (!rows.length || !columns.length) return '';

        const maxRows = Math.min(rows.length, 100);
        return `
            <div class="data-table-wrapper">
                <div class="data-table-header">
                    <span class="sql-label">Kết quả</span>
                    <span class="data-table-info">${rows.length} dòng</span>
                </div>
                <div class="data-table-scroll">
                    <table class="data-table">
                        <thead><tr>${columns.map((c) => `<th>${escapeHtml(c)}</th>`).join('')}</tr></thead>
                        <tbody>${rows
                            .slice(0, maxRows)
                            .map((row) => `<tr>${columns.map((c) => `<td>${formatCell(row[c])}</td>`).join('')}</tr>`)
                            .join('')}</tbody>
                    </table>
                </div>
            </div>`;
    }

    function renderApproval(data) {
        if (!data.requires_approval || !data.request_id) return '';
        return `
            <div class="approval-actions">
                <button class="btn-primary" onclick="executePreparedQuery('${escapeHtml(data.request_id)}', this)">Allow chạy query</button>
                <button class="btn-secondary" onclick="skipPreparedQuery(this)">Skip</button>
            </div>`;
    }

    function addAssistantMessage(data, opts = {}) {
        const suppressSql = !!opts.suppressSql;
        const showSql = $('#showSqlToggle')?.checked ?? true;

        let html = '';
        if (data.answer) html += `<div class="assistant-text">${escapeHtml(data.answer)}</div>`;
        if (data.sql && showSql && !suppressSql) html += renderSqlBlock(data.sql);
        html += renderDataTable(data);
        html += renderApproval(data);
        if (data.error && !data.answer) html += `<div class="error-block">${escapeHtml(data.error)}</div>`;
        if (data.latency_ms) html += `<div class="latency">${data.latency_ms}ms${data.retries > 0 ? ` · ${data.retries} retries` : ''}</div>`;

        addMessage('assistant', html || '<div class="assistant-text">Không có nội dung phản hồi.</div>');
        if (window.lucide?.createIcons) {
            window.lucide.createIcons();
        }
    }

    function addLoading() {
        const container = $('#messages');
        const el = document.createElement('div');
        el.className = 'message assistant';
        el.innerHTML = `
        <div class="message-content">
            <div class="message-body">
                <div class="loading">
                    <div class="loading-dot"></div>
                    <div class="loading-dot"></div>
                    <div class="loading-dot"></div>
                </div>
                <span class="thinking-inline">Thinking...</span>
            </div>
        </div>`;
        container.appendChild(el);
        scrollToBottom();
        return el;
    }

    async function askQuestion(question) {
        if (state.isLoading) return;
        state.isLoading = true;

        const input = $('#questionInput');
        input.value = '';
        input.style.height = 'auto';
        $('#sendBtn').disabled = true;
        $('#welcome').style.display = 'none';

        addMessage('user', escapeHtml(question));
        const loadingEl = addLoading();

        try {
            const res = await fetch(`${API_URL}/api/ask`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ question }),
            });
            const contentType = res.headers.get('content-type') || '';
            let data = null;

            if (contentType.includes('application/json')) {
                data = await res.json();
            } else {
                const raw = await res.text();
                throw new Error(raw || `HTTP ${res.status}`);
            }

            if (!res.ok) {
                throw new Error(data?.detail || data?.error || `HTTP ${res.status}`);
            }

            loadingEl.remove();
            addAssistantMessage(data);
        } catch (err) {
            loadingEl.remove();
            addMessage('assistant', `<div class="error-block">❌ Không thể kết nối server: ${escapeHtml(err.message)}</div>`);
        } finally {
            app.api.loadHistory();
        }

        state.isLoading = false;
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
            addMessage('assistant', `<div class="error-block">❌ Execute lỗi: ${escapeHtml(err.message)}</div>`);
        }
    }

    function skipPreparedQuery(btn) {
        const wrap = btn.closest('.approval-actions');
        if (wrap) {
            wrap.innerHTML = '<span class="assistant-text">Đã bỏ qua chạy query.</span>';
        }
    }

    function copySQL(btn, sql) {
        const decodedSql = decodeURIComponent(String(sql || ''));
        navigator.clipboard.writeText(decodedSql);
        btn.textContent = 'Copied!';
        btn.classList.add('copied');
        setTimeout(() => {
            btn.textContent = 'Copy';
            btn.classList.remove('copied');
        }, 1500);
    }

    window.copySQL = copySQL;
    window.executePreparedQuery = executePreparedQuery;
    window.skipPreparedQuery = skipPreparedQuery;

    app.chat = {
        askQuestion,
        addMessage,
        addAssistantMessage,
        addLoading,
    };
})();
