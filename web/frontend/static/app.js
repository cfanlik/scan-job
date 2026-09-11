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
            cryptorank: '#fbbf24',
            both: '#34d399',
            rootdata: '#a78bfa'
        };
        const c = colors[s] || '#94a3b8';
        return `<span class="badge" style="background:${c};color:#0f172a">${s || '-'}</span>`;
    },
    listedBadge(cmc, cr) {
        if (cmc) return '<span class="badge bg-success">CMC已收录</span>';
        if (cr) return '<span class="badge bg-info">CR已交易</span>';
        return '<span class="badge bg-danger">未上市</span>';
    },
    upbitFitBadge(score, isListed) {
        if (isListed) {
            return `<span class="badge bg-success">已上 Upbit</span>`;
        }
        if (score >= 85) {
            return `<span class="badge bg-danger text-white">S级 ${score}分</span>`;
        } else if (score >= 70) {
            return `<span class="badge bg-warning text-dark">A级 ${score}分</span>`;
        } else if (score >= 50) {
            return `<span class="badge bg-info text-white">B级 ${score}分</span>`;
        }
        return `<span class="badge bg-secondary text-white">${score || 0}分</span>`;
    },
    backersBadges(backers) {
        if (!backers || !backers.length) return '-';
        return backers.slice(0, 3).map(b => {
            const name = typeof b === 'object' ? b.name : b;
            return `<span class="badge bg-dark border border-secondary me-1">${name}</span>`;
        }).join('');
    },
    investors(s) {
        if (!s) return '-';
        try {
            const arr = typeof s === 'string' ? JSON.parse(s) : s;
            if (Array.isArray(arr)) return arr.slice(0, 3).join(', ');
        } catch {
            return s.length > 30 ? s.slice(0, 30) + '...' : s;
        }
        return s;
    },
};

const Toast = {
    show(msg, type = 'info') {
        const el = document.createElement('div');
        el.className = `alert alert-${type} position-fixed top-0 end-0 m-3 shadow`;
        el.style.zIndex = '9999';
        el.innerText = msg;
        document.body.appendChild(el);
        setTimeout(() => el.remove(), 3000);
    },
};
