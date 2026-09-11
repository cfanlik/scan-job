/**
 * scan-job 公共 JS — API 客户端 + 格式化工具 + Toast
 * 升级：支持近90天Upbit四维拟合度、现货/合约覆盖徽章、Coinbase联动标记、赛道展示
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
    listedBadge(cmc, cr, hasSpot, hasFutures) {
        if (hasSpot || hasFutures) {
            return '<span class="badge badge-spot">已上所</span>';
        }
        if (cmc) return '<span class="badge bg-success">CMC已收录</span>';
        if (cr) return '<span class="badge bg-info">CR已交易</span>';
        return '<span class="badge badge-unlisted">未上市</span>';
    },
    marketBadges(hasSpot, hasFutures, spotExchanges, futuresExchanges) {
        let spotList = [];
        let futList = [];
        try {
            spotList = typeof spotExchanges === 'string' ? JSON.parse(spotExchanges) : (spotExchanges || []);
        } catch { spotList = []; }
        try {
            futList = typeof futuresExchanges === 'string' ? JSON.parse(futuresExchanges) : (futuresExchanges || []);
        } catch { futList = []; }

        let html = '';
        if (hasSpot && spotList.length > 0) {
            const label = spotList.slice(0, 2).join('/');
            const more = spotList.length > 2 ? `+${spotList.length - 2}` : '';
            html += `<span class="badge badge-spot me-1" title="现货市场: ${spotList.join(', ')}">现货: ${label}${more}</span>`;
        } else {
            html += `<span class="badge badge-muted me-1">无现货</span>`;
        }

        if (hasFutures && futList.length > 0) {
            const label = futList.slice(0, 2).join('/');
            const more = futList.length > 2 ? `+${futList.length - 2}` : '';
            html += `<span class="badge badge-futures me-1" title="合约市场: ${futList.join(', ')}">合约: ${label}${more}</span>`;
        } else {
            html += `<span class="badge badge-muted me-1">无合约</span>`;
        }
        return html;
    },
    coinbaseBadge(cbListed) {
        if (cbListed) {
            return `<span class="badge badge-coinbase" title="已上线 Coinbase 交易对，具备合规大所流动性背书">CB 联动</span>`;
        }
        return `<span class="badge badge-muted text-muted" style="opacity:0.6">-</span>`;
    },
    sectorBadge(sector) {
        if (!sector) return '<span class="badge badge-muted">-</span>';
        return `<span class="badge badge-sector">${sector}</span>`;
    },
    upbitFitBadge(score, isListed, breakdown) {
        if (isListed) {
            return `<span class="badge badge-upbit-listed">已上 Upbit</span>`;
        }
        score = Math.round(score || 0);

        let bd = null;
        if (breakdown) {
            try {
                bd = typeof breakdown === 'string' ? JSON.parse(breakdown) : breakdown;
            } catch { bd = null; }
        }

        let tip = `近90天Upbit新币四维拟合综合得分: ${score}分`;
        if (bd) {
            tip = `90天拟合明细: 资方偏好 ${bd.investor_score || 0}/35 | Coinbase联动 ${bd.coinbase_score || 0}/25 | 赛道契合 ${bd.sector_score || 0}/25 | 公链生态 ${bd.chain_score || 0}/15`;
        }

        if (score >= 85) {
            return `<span class="badge badge-fit-s" title="${tip}">S级 ${score}分</span>`;
        } else if (score >= 70) {
            return `<span class="badge badge-fit-a" title="${tip}">A级 ${score}分</span>`;
        } else if (score >= 50) {
            return `<span class="badge badge-fit-b" title="${tip}">B级 ${score}分</span>`;
        }
        return `<span class="badge badge-muted" title="${tip}">${score}分</span>`;
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
