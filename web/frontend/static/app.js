/**
 * scan-job 公共 JS — API 客户端 + 格式化工具 + Toast
 */

const API = {
    async get(path) {
        const r = await fetch(path);
        if (!r.ok) throw new Error(`${r.status} ${r.statusText}`);
        return r.json();
    },
    async post(path, body) {
        const r = await fetch(path, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(body),
        });
        if (!r.ok) throw new Error(`${r.status} ${r.statusText}`);
        return r.json();
    },
    async delete(path) {
        const r = await fetch(path, { method: 'DELETE' });
        if (!r.ok) throw new Error(`${r.status} ${r.statusText}`);
        return r.json();
    },
    async put(path, body) {
        const r = await fetch(path, {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(body),
        });
        if (!r.ok) throw new Error(`${r.status} ${r.statusText}`);
        return r.json();
    },
};

const fmt = {
    datetime(iso) {
        if (!iso) return '-';
        const d = new Date(iso);
        return d.toLocaleString('zh-CN', { hour12: false });
    },
    number(n) {
        if (n == null) return '-';
        if (typeof n === 'string') n = parseFloat(n);
        if (isNaN(n)) return '-';
        if (n >= 1e9) return (n / 1e9).toFixed(2) + 'B';
        if (n >= 1e6) return (n / 1e6).toFixed(2) + 'M';
        if (n >= 1e3) return (n / 1e3).toFixed(1) + 'K';
        return n.toFixed(0);
    },
    money(n) {
        if (n == null) return '-';
        return '$' + fmt.number(n);
    },
    sourceBadge(s) {
        const colors = {
            rootdata: '#a78bfa',
            cryptorank: '#fbbf24',
            both: '#34d399',
        };
        const c = colors[s] || '#9ca3af';
        return `<span class="badge" style="background:${c};color:#000;font-weight:600">${s || '-'}</span>`;
    },
    listedBadge(cmc, cr) {
        if (cmc || cr) {
            return '<span class="badge bg-danger">已上所</span>';
        }
        return '<span class="badge bg-success">未上所</span>';
    },
    tags(json_str) {
        try {
            const arr = JSON.parse(json_str || '[]');
            return arr.map(t => `<span class="badge bg-dark">${t}</span>`).join(' ');
        } catch { return ''; }
    },
    investors(json_str) {
        try {
            const arr = JSON.parse(json_str || '[]');
            return arr.slice(0, 3).join(', ') + (arr.length > 3 ? ` +${arr.length - 3}` : '');
        } catch { return ''; }
    },
};

function toast(msg, type = 'info') {
    const el = document.createElement('div');
    el.className = `toast-custom toast-${type}`;
    el.textContent = msg;
    document.body.appendChild(el);
    setTimeout(() => el.classList.add('show'), 10);
    setTimeout(() => {
        el.classList.remove('show');
        setTimeout(() => el.remove(), 300);
    }, 3000);
}
