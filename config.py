"""
scan-job 全局配置
"""
import os
import threading
from dotenv import load_dotenv

_PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(_PROJECT_ROOT, ".env"))

# ---------- API Keys ---------- #
CMC_API_KEY: str = os.environ.get("CMC_API_KEY", "")

# CryptoRank API 配置 (支持多 Key 轮询)
_RAW_CR_KEY = os.environ.get("CRYPTORANK_API_KEY", "28a4b5170f486ac1ee855f493a46015f1ded3ea0408e30a60577af6cfc25")
_RAW_CR_KEYS = os.environ.get("CRYPTORANK_API_KEYS", "")

if _RAW_CR_KEYS:
    CRYPTORANK_API_KEYS = [k.strip() for k in _RAW_CR_KEYS.split(",") if k.strip()]
elif _RAW_CR_KEY:
    CRYPTORANK_API_KEYS = [_RAW_CR_KEY.strip()]
else:
    CRYPTORANK_API_KEYS = []

_cr_key_lock = threading.Lock()
_cr_key_usage = {}  # {key: active_count}


def acquire_cryptorank_key() -> str | None:
    """最少使用数优先分配 CryptoRank Key。"""
    if not CRYPTORANK_API_KEYS:
        return None
    with _cr_key_lock:
        for k in CRYPTORANK_API_KEYS:
            _cr_key_usage.setdefault(k, 0)
        key = min(CRYPTORANK_API_KEYS, key=lambda k: _cr_key_usage[k])
        _cr_key_usage[key] += 1
        return key


def release_cryptorank_key(key: str):
    """归还并释放 Key 计数。"""
    if not key:
        return
    with _cr_key_lock:
        if key in _cr_key_usage:
            _cr_key_usage[key] = max(0, _cr_key_usage[key] - 1)


# Upbit 配置
UPBIT_API_BASE: str = os.environ.get("UPBIT_API_BASE", "https://api.upbit.com/v1")
UPBIT_FIT_THRESHOLD: int = int(os.environ.get("UPBIT_FIT_THRESHOLD", "75"))

# ---------- 代理 ---------- #
PROXY_URL: str = os.environ.get("PROXY_URL", "")
PROXY_ENABLED: bool = os.environ.get("PROXY_ENABLED", "true").lower() in ("1", "true", "yes")


def get_proxy() -> str | None:
    """统一代理入口。返回代理 URL 字符串或 None。"""
    if not PROXY_ENABLED:
        return None
    url = os.environ.get("PROXY_URL", "") or PROXY_URL
    if not url:
        return None
    # httpx 兼容：socks:// → socks5://
    if url.startswith("socks://"):
        url = "socks5://" + url[len("socks://"):]
    return url
