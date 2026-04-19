/* Kur — Bootstrap + Shared State */

window.KurApp = window.KurApp || {
    API_URL: '',
    $: (sel) => document.querySelector(sel),
    $$: (sel) => document.querySelectorAll(sel),
    state: {
        isLoading: false,
        historyItems: [],
        routerApiKeyConfigured: false,
        generatorApiKeyConfigured: false,
    },
    PROVIDER_MODELS: {
        openai: ['gpt-4o', 'gpt-4o-mini', 'gpt-4-turbo', 'o1-mini', 'o1-preview'],
        groq: ['llama-3.3-70b-versatile', 'llama-3.1-8b-instant', 'mixtral-8x7b-32768', 'gemma2-9b-it'],
        gemini: ['gemini-2.5-pro', 'gemini-2.5-flash', 'gemini-2.0-flash', 'gemini-1.5-pro'],
        ollama: ['snowflake-arctic-text2sql-r1:7b', 'llama3.1:8b', 'codellama:7b'],
    },
};

const app = window.KurApp;
const { $, $$ } = app;

function applyTheme(themeName) {
    const body = document.body;
    const isDark = themeName === 'dark';
    body.classList.toggle('theme-dark', isDark);
    const themeBtn = $('#themeToggleBtn');
    if (themeBtn) {
        themeBtn.setAttribute('data-theme', themeName);
        themeBtn.title = isDark ? 'Switch to light mode' : 'Switch to dark mode';
        themeBtn.setAttribute('aria-label', themeBtn.title);
        themeBtn.innerHTML = `<i data-lucide="${isDark ? 'sun' : 'moon'}" class="icon-18"></i>`;
        if (window.lucide?.createIcons) {
            window.lucide.createIcons();
        }
    }
}

function setupTheme() {
    const saved = localStorage.getItem('kur_theme');
    const preferredDark = window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches;
    const initial = saved || (preferredDark ? 'dark' : 'light');
    applyTheme(initial);

    const themeBtn = $('#themeToggleBtn');
    if (!themeBtn) return;
    themeBtn.onclick = () => {
        const next = document.body.classList.contains('theme-dark') ? 'light' : 'dark';
        localStorage.setItem('kur_theme', next);
        applyTheme(next);
    };
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
            if (input.value.trim() && !app.state.isLoading) app.chat.askQuestion(input.value.trim());
        }
    });

    sendBtn.addEventListener('click', () => {
        if (input.value.trim() && !app.state.isLoading) app.chat.askQuestion(input.value.trim());
    });
}

function setupSidebar() {
    $('#newChatBtn').onclick = () => {
        $('#messages').innerHTML = '';
        $('#welcome').style.display = 'flex';
        const input = $('#questionInput');
        input.value = '';
        input.dispatchEvent(new Event('input'));
        input.focus();
    };
}
document.addEventListener('DOMContentLoaded', () => {
    setupTheme();
    app.api.loadSuggestions();
    app.api.loadHealth();
    app.settings.loadSettings();
    app.api.loadHistory();
    setupInput();
    setupSidebar();
    app.settings.setupSettings();
});
