"""测试 RootData 爬虫"""
import sys
sys.path.insert(0, ".")
from core.rootdata_scraper import RootDataScraper

s = RootDataScraper()
items = s.fetch_fundraising_page(page=1, page_size=20, on_log=print)
print(f"\n=== 结果: {len(items)} 个项目 ===")
for i, p in enumerate(items[:5]):
    name = p.get("project_name", "?")
    fund = p.get("total_funding")
    rnd = p.get("latest_round")
    print(f"{i+1}. {name} | 融资: {fund} | 轮次: {rnd}")
