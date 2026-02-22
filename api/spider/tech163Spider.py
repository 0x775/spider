import asyncio
import json
import re
from datetime import datetime
from playwright.async_api import async_playwright
from storage import NewsStorage


def clean_html_content(raw_html):
    """
    内置的 HTML 清洗工具，确保存入 Redis 前就是干净的
    """
    if not raw_html:
        return ""
    # 移除 style, class, id, data-check-id 等冗余属性
    clean = re.sub(r'\s+(style|class|id|data-check-id)="[^"]*"', '', raw_html)
    # 压缩多余空白
    clean = re.sub(r'\s+', ' ', clean)
    return clean.strip()


class Spider:
    def __init__(self):
        self.base_url = "https://tech.163.com/"
        self.stop_scrolling = False
        self.storage = NewsStorage()
        self.category = "163_tech"
        self.urls = []

    async def parse_main_page(self, page):
        """
        抓取首屏已有数据
        """
        items = await page.locator(".news_article").all()
        for item in items:
            title_node = item.locator(".news_title a").first
            if await title_node.count() > 0:
                title = (await title_node.inner_text()).strip()
                href = await title_node.get_attribute("href")
                if not href:
                    continue
                full_url = f"{self.base_url}{href}" if href.startswith(
                    '/') else href
                self.urls.append({"title": title, "url": full_url})

    async def fetch_details(self, browser):
        """
        需求2 & 处理所有详情
        """
        print("\n=== 开始处理所有详情页任务 ===")
        while True:
            try:
                item = self.urls.pop()
            except IndexError:
                print("✅ 已清空，详情处理完毕。")
                break

            title = item["title"]
            url = item["url"]
            print(f"[新任务] {title} -> {url}")

            clean_url = url.split('?')[0].split('#')[0]
            news_id = clean_url.split('/')[-1].replace('.html', '')
            news_id = self.category+"_"+news_id

            if self.storage.r.exists("news:detail:"+news_id):
                print(f"[重复] {url}")
                continue

            context = await browser.new_context()
            page = await context.new_page()
            try:
                result = {
                    "title": "", "url": url, "author": "", "category": self.category, "summary": "", "pic_path": "", "content": ""}

                # 优化加载速度，只等待 DOM
                await page.goto(url, wait_until="domcontentloaded", timeout=30000)

                title = (await page.title()).split('|')[0]
                result["title"] = title
                result["pubDate"] = await page.locator("#contain").get_attribute("data-ptime")
                author = await page.locator(".post_info a").first.text_content()
                if author:
                    result["author"] = author.strip()

                post_body = page.locator(".post_body")
                if await post_body.count() > 0:
                    post_body_html = await post_body.inner_html()
                    result["content"] = clean_html_content(
                        post_body_html)

                    first_img = post_body.locator("img").first
                    if await first_img.count() > 0:
                        pic_path = await first_img.get_attribute("src")
                        pic_path = pic_path.strip() if pic_path else ""
                        result["pic_path"] = pic_path

                    publish_time = self.storage.parse_publish_time(
                        result.get("pubDate"))
                    self.storage.save_news(news_id=news_id, news_data=result,
                                           category=self.category, publish_time=publish_time)
                    print(f"已保存详情: {result['title']}")
                else:
                    print(f"未找到数据标签: {url}")
                # print(result)
            except Exception as e:
                print(f"处理失败 {url}: {e}")
            finally:
                await context.close()

    async def run(self):
        async with async_playwright() as p:
            # 建议开启 headless=True 以提高效率
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()

            # 2. 访问主页并解析首屏
            print("正在打开主页...")
            await page.goto(f"https://tech.163.com/", wait_until="networkidle", timeout=30000)

            # 3. 需求3：循环滚动直到获取 10 组 API 数据
            print("开始滚动触发加载...")
            while not self.stop_scrolling:
                await page.mouse.wheel(0, 2000)
                await asyncio.sleep(2.5)  # 给一点加载时间
                # 防止死循环（例如页面到底了）
                if await page.evaluate("window.innerHeight + window.scrollY >= document.body.scrollHeight"):
                    print("已到达页面底部，停止滚动。")
                    break

            # 3.1提取首页url
            print("处理首页URL提取..")
            await self.parse_main_page(page)

            # 4. 处理详情页
            await self.fetch_details(browser)

            print("\n所有任务已完成，程序退出。")
            await browser.close()


if __name__ == "__main__":
    # 运行前清理旧队列（可选）
    # r.delete(REDIS_KEY_QUEUE, REDIS_KEY_VISITED)

    spider = Spider()
    asyncio.run(spider.run())


"""
redis查询数据

1. 查看任务队列 (对应 key: huxiu:urls)
查看还有多少个 URL 没爬： LLEN huxiu:urls

查看队列里最前面的 10 个 URL： LRANGE huxiu:urls 0 9

2. 查看已访问记录 (对应 key: huxiu:visited)
查看总共去重后的 URL 数量： SCARD huxiu:visited

随机看 5 个已经抓过的 URL： SRANDMEMBER huxiu:visited 5

3. 查看抓取结果 (对应 key: huxiu:details)
查看已经存入 Redis 的详情总数： HLEN huxiu:details

列出所有已经抓取成功的文章 URL： HKEYS huxiu:details

查看某一篇的具体内容 (把 URL 换成上面搜出来的)： HGET huxiu:details "你的URL地址"

"""
