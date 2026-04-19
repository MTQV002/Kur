(() => {
    const app = window.KurApp;
    const { $, $$, state, API_URL, PROVIDER_MODELS } = app;

    function closeSettings() {
        $('#settingsOverlay').classList.remove('open');
    }

    function updateModelList(provider, prefix) {
        const pillsContainer = $(`#${prefix}ModelPills`);
        if (!pillsContainer) return;

        const models = PROVIDER_MODELS[provider] || [];
        pillsContainer.innerHTML = '';

        models.forEach((m) => {
            const pill = document.createElement('span');
            pill.className = 'model-pill';
            pill.textContent = m;
            pill.onclick = () => {
                const input = $(`#${prefix}Model`);
                if (input) input.value = m;
            };
            pillsContainer.appendChild(pill);
        });
    }

    function toggleProviderFields(provider, prefix) {
        const needsKey = provider !== 'ollama';

        const apiKeyField = $(`#${prefix}ApiKeyField`);
        if (apiKeyField) apiKeyField.classList.toggle('hidden', !needsKey);

        const anyOllama = $('#sRouterLlmProvider').value === 'ollama' || $('#sGeneratorLlmProvider').value === 'ollama';
        const ollamaUrlField = $('#sOllamaUrlField');
        if (ollamaUrlField) ollamaUrlField.classList.toggle('hidden', !anyOllama);

        const kp = { openai: 'sk-...', groq: 'gsk_...', gemini: 'AIza...' };
        const apiKeyInput = $(`#${prefix}ApiKey`);
        if (needsKey && apiKeyInput) apiKeyInput.placeholder = kp[provider] || 'API key';
    }

    function toggleEngineFields(engine) {
        const isDuckdb = engine === 'duckdb';
        $('#sDuckdbPathField').classList.toggle('hidden', !isDuckdb);
        ['sDbHostPortField', 'sDbNameField', 'sDbUserField', 'sDbPasswordField'].forEach((id) => {
            const el = $(`#${id}`);
            if (el) el.classList.toggle('hidden', isDuckdb);
        });

        const ports = { postgres: 5432, trino: 8080, clickhouse: 8123 };
        if (!isDuckdb) $('#sDbPort').value = ports[engine] || 5432;
    }

    async function loadSettings() {
        try {
            const res = await fetch(`${API_URL}/api/settings`);
            const s = await res.json();

            if (s.router_provider) $('#sRouterLlmProvider').value = s.router_provider;
            if (s.router_model) $('#sRouterModel').value = s.router_model;
            state.routerApiKeyConfigured = !!s.router_api_key_configured;
            $('#sRouterApiKey').placeholder = s.router_api_key_masked || 'Chưa cấu hình API key';
            updateModelList($('#sRouterLlmProvider').value, 'sRouter');
            toggleProviderFields($('#sRouterLlmProvider').value, 'sRouter');

            if (s.generator_provider) $('#sGeneratorLlmProvider').value = s.generator_provider;
            if (s.generator_model) $('#sGeneratorModel').value = s.generator_model;
            state.generatorApiKeyConfigured = !!s.generator_api_key_configured;
            $('#sGeneratorApiKey').placeholder = s.generator_api_key_masked || 'Chưa cấu hình API key';
            updateModelList($('#sGeneratorLlmProvider').value, 'sGenerator');
            toggleProviderFields($('#sGeneratorLlmProvider').value, 'sGenerator');

            if (s.ollama_url) $('#sOllamaUrl').value = s.ollama_url;

            $('#sDbEngine').value = s.db_engine || 'duckdb';
            toggleEngineFields(s.db_engine || 'duckdb');
            if (s.duckdb_path) $('#sDuckdbPath').value = s.duckdb_path;
            if (s.db_host) $('#sDbHost').value = s.db_host;
            if (s.db_port) $('#sDbPort').value = s.db_port;
            if (s.db_name) $('#sDbName').value = s.db_name;
            if (s.db_user) $('#sDbUser').value = s.db_user;

            if (s.polaris_url) $('#sPolarisUrl').value = s.polaris_url;
            if (s.polaris_catalog) $('#sPolarisCatalog').value = s.polaris_catalog;
            if (s.polaris_credentials_masked) $('#sPolarisCredentials').placeholder = s.polaris_credentials_masked;

            if (s.max_retries !== undefined) $('#sMaxRetries').value = s.max_retries;
            if (s.query_timeout !== undefined) $('#sQueryTimeout').value = s.query_timeout;
            if (s.max_rows !== undefined) $('#sMaxRows').value = s.max_rows;
            if (s.language) $('#sLanguage').value = s.language;

            const dbInfo = $('#dbInfo');
            if (dbInfo) dbInfo.textContent = `● Connected: ${(s.db_engine || 'duckdb').toUpperCase()}`;
        } catch {
            // ignore
        }
    }

    async function saveSettings() {
        const status = $('#settingsStatus');
        status.textContent = 'Saving...';
        status.className = 'settings-status';

        const payload = {
            router_provider: $('#sRouterLlmProvider').value,
            router_model: $('#sRouterModel').value,
            generator_provider: $('#sGeneratorLlmProvider').value,
            generator_model: $('#sGeneratorModel').value,
            ollama_url: $('#sOllamaUrl').value || 'http://localhost:11434',
            db_engine: $('#sDbEngine').value,
            duckdb_path: $('#sDuckdbPath').value,
            db_host: $('#sDbHost').value,
            db_port: parseInt($('#sDbPort').value) || 5432,
            db_name: $('#sDbName').value,
            db_user: $('#sDbUser').value,
            polaris_url: $('#sPolarisUrl').value,
            polaris_catalog: $('#sPolarisCatalog').value,
            polaris_credentials: $('#sPolarisCredentials').value,
            max_retries: parseInt($('#sMaxRetries').value) || 3,
            query_timeout: parseInt($('#sQueryTimeout').value) || 30,
            max_rows: parseInt($('#sMaxRows').value) || 1000,
            language: $('#sLanguage').value,
        };

        const routerKeyVal = $('#sRouterApiKey').value.trim();
        if (routerKeyVal) payload.router_api_key = routerKeyVal;

        const genKeyVal = $('#sGeneratorApiKey').value.trim();
        if (genKeyVal) payload.generator_api_key = genKeyVal;

        const missingRouterKey = payload.router_provider !== 'ollama' && !routerKeyVal && !state.routerApiKeyConfigured;
        const missingGeneratorKey = payload.generator_provider !== 'ollama' && !genKeyVal && !state.generatorApiKeyConfigured;
        if (missingRouterKey || missingGeneratorKey) {
            const missingParts = [];
            if (missingRouterKey) missingParts.push('Router API key');
            if (missingGeneratorKey) missingParts.push('Generator API key');
            status.textContent = `❌ Thiếu ${missingParts.join(' và ')}. Vui lòng nhập rồi bấm Lưu.`;
            status.className = 'settings-status error';
            return;
        }

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
                if (routerKeyVal) state.routerApiKeyConfigured = true;
                if (genKeyVal) state.generatorApiKeyConfigured = true;
                $('#sRouterApiKey').value = '';
                $('#sGeneratorApiKey').value = '';
                $('#sDbPassword').value = '';
                const dbInfo = $('#dbInfo');
                if (dbInfo) dbInfo.textContent = `● Connected: ${payload.db_engine.toUpperCase()}`;
                app.api.loadHealth();
            } else {
                const err = await res.json();
                status.textContent = `❌ ${err.detail || 'Error'}`;
                status.className = 'settings-status error';
            }
        } catch (e) {
            status.textContent = `❌ ${e.message}`;
            status.className = 'settings-status error';
        }

        setTimeout(() => {
            status.textContent = '';
        }, 4000);
    }

    async function resetSettings() {
        if (!confirm('Reset tất cả settings về mặc định?')) return;
        try {
            await fetch(`${API_URL}/api/settings/reset`, { method: 'POST' });
            loadSettings();
            const status = $('#settingsStatus');
            status.textContent = '🔄 Reset thành công';
            status.className = 'settings-status success';
            setTimeout(() => {
                status.textContent = '';
            }, 3000);
        } catch {
            // ignore
        }
    }

    function setupSettings() {
        $('#settingsBtn').onclick = () => {
            $('#settingsOverlay').classList.add('open');
            loadSettings();
        };
        $('#closeSettingsBtn').onclick = closeSettings;
        $('#settingsOverlay').onclick = (e) => {
            if (e.target === $('#settingsOverlay')) closeSettings();
        };
        document.addEventListener('keydown', (e) => {
            if (e.key === 'Escape') closeSettings();
        });

        $('#sRouterLlmProvider').onchange = () => {
            const prov = $('#sRouterLlmProvider').value;
            updateModelList(prov, 'sRouter');
            toggleProviderFields(prov, 'sRouter');
        };
        $('#sGeneratorLlmProvider').onchange = () => {
            const prov = $('#sGeneratorLlmProvider').value;
            updateModelList(prov, 'sGenerator');
            toggleProviderFields(prov, 'sGenerator');
        };

        $('#sDbEngine').onchange = () => {
            toggleEngineFields($('#sDbEngine').value);
        };

        const tabs = $$('.settings-tab');
        const panes = $$('.settings-tab-pane');
        tabs.forEach((tab) => {
            tab.onclick = () => {
                tabs.forEach((t) => t.classList.remove('active'));
                panes.forEach((p) => p.classList.remove('active'));
                tab.classList.add('active');
                $(`#${tab.dataset.target}`).classList.add('active');
            };
        });

        $('#saveSettingsBtn').onclick = saveSettings;
        $('#resetSettingsBtn').onclick = resetSettings;
    }

    app.settings = {
        setupSettings,
        loadSettings,
        saveSettings,
        resetSettings,
        toggleEngineFields,
        toggleProviderFields,
        updateModelList,
    };
})();
