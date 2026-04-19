(() => {
    const app = window.KurApp;
    const { $, state, API_URL } = app;

    function renderHistory() {
        const container = $('#history');
        if (!container) return;
        container.innerHTML = '';
        state.historyItems.slice(0, 15).forEach((item) => {
            const el = document.createElement('div');
            el.className = 'history-item';
            el.textContent = item.question;
            el.onclick = () => populateInput(item.question);
            container.appendChild(el);
        });
    }

    function populateInput(question) {
        const input = $('#questionInput');
        input.value = question || '';
        input.dispatchEvent(new Event('input'));
        input.focus();
    }

    async function loadSuggestions() {
        try {
            const res = await fetch(`${API_URL}/api/suggestions`);
            const data = await res.json();
            const container = $('#suggestions');
            const welcomeCards = $('#welcomeCards');

            if (container) container.innerHTML = '';
            if (welcomeCards) welcomeCards.innerHTML = '';

            data.suggestions.slice(0, 6).forEach((q) => {
                const btn = document.createElement('button');
                btn.className = 'suggestion-item';
                btn.textContent = q;
                btn.onclick = () => app.chat.askQuestion(q);
                if (container) container.appendChild(btn);

                const card = document.createElement('div');
                card.className = 'welcome-card';
                card.textContent = q;
                card.onclick = () => app.chat.askQuestion(q);
                if (welcomeCards) welcomeCards.appendChild(card);
            });
        } catch {
            // ignore
        }
    }

    async function loadHealth() {
        try {
            const res = await fetch(`${API_URL}/api/health`);
            const data = await res.json();
            const info = $('#dbInfo');
            if (info) {
                const engine = data.engine || 'DuckDB';
                info.textContent = `● Connected: ${engine}`;
            }
        } catch {
            const info = $('#dbInfo');
            if (info) info.textContent = '● Offline';
        }
    }

    async function loadHistory() {
        try {
            const res = await fetch(`${API_URL}/api/history?limit=30`);
            const data = await res.json();
            state.historyItems = Array.isArray(data.items) ? data.items : [];
            renderHistory();
        } catch {
            state.historyItems = [];
            renderHistory();
        }
    }

    app.api = {
        loadSuggestions,
        loadHealth,
        loadHistory,
        renderHistory,
        populateInput,
    };
})();
