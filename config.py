"""
scan-job 全局配置
"""
import os
from dotenv import load_dotenv

_PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(_PROJECT_ROOT, ".env"))

# ---------- API Keys ---------- #
CMC_API_KEY: str = os.environ.get("CMC_API_KEY", "")

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
