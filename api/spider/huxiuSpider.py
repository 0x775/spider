import asyncio
import json
import re
import redis
from playwright.async_api import async_playwright

# Redis 配置
r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
REDIS_KEY_QUEUE = "huxiu:urls"          # 待处理队列 (List)
REDIS_KEY_VISITED = "huxiu:visited"    # 已去重集合 (Set)
REDIS_KEY_RESULT = "huxiu:details"     # 最终结果存储 (Hash)
EXPIRE_TIME = 3600 * 24 * 7  # 过期时间 1 小时 ( 7天)


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


def restore_nuxt_data(raw_data):
    """
    修改后的还原函数：直接定位核心内容，舍弃无用数据
    """
    try:
        # 1. 正常的 Nuxt 数据解析逻辑 (假设 raw_data 是提取出的那个列表/字典)
        # 这里保留你原有的还原逻辑，得到一个列表数据（例如叫 nuxt_list）

        # --- 假设 nuxt_list 是你还原出来的那个包含很多 dict 的数组 ---
        # 我们直接在这里进行“清洗拦截”

        for entry in raw_data:
            # 匹配你的清洗规则：必须有 summary 和 content
            if isinstance(entry, dict) and "summary" in entry and "content" in entry:
                # 提取并精简
                clean_result = {
                    "title": entry.get("title", ""),
                    "pic_path": entry.get("pic_path", "").split('?', 1)[0],
                    "author": entry.get("author", ""),
                    "summary": entry.get("summary", ""),
                    "pubDate": entry.get("fdateline", ""),
                    "content": clean_html_content(entry.get("content", ""))
                }
                # 找到之后直接返回这个精简后的字典
                return clean_result

        # 如果遍历完都没找到核心内容，返回 None 或者一个空字典
        return None

    except Exception as e:
        print(f"还原并清洗数据时出错: {e}")
        return None


class HuXiuSpider:
    def __init__(self):
        self.base_url = "https://www.huxiu.com"
        self.api_count = 0
        self.max_api_pages = 10
        self.stop_scrolling = False

    async def push_to_redis(self, title, url):
        """
        需求1：检查去重并推入 Redis
        """
        # SADD 返回 1 表示是新元素，返回 0 表示已存在
        if r.sadd(REDIS_KEY_VISITED, url):
            print(f"[新任务] {title} -> {url}")
            r.lpush(REDIS_KEY_QUEUE, json.dumps({"title": title, "url": url}))
            r.expire(REDIS_KEY_QUEUE, EXPIRE_TIME)
            return True
        return False

    async def handle_api_response(self, response):
        """
        需求3：监听 API 并在获取 10 组后停止
        """
        if "api-web-article.huxiu.com/web/channel/articleListV1" in response.url:
            if response.status == 200 and self.api_count < self.max_api_pages:
                try:
                    data = await response.json()
                    items = data.get('data', {}).get('datalist', [])
                    if items:
                        self.api_count += 1
                        print(f"\n--- 拦截到第 {self.api_count} 组 API 数据 ---")
                        for item in items:
                            title = item.get('title')
                            path = item.get('url', '')
                            full_url = f"{self.base_url}{path}" if path.startswith(
                                '/') else path
                            await self.push_to_redis(title, full_url)

                    if self.api_count >= self.max_api_pages:
                        print("已达到 10 组 API 数据，准备停止滚动。")
                        self.stop_scrolling = True
                except Exception as e:
                    print(f"API 解析失败: {e}")

    async def parse_main_page(self, page):
        """
        抓取首屏已有数据
        """
        items = await page.query_selector_all(".article-item-wrap")
        for item in items:
            title_node = await item.query_selector(".article-item__content__title")
            if title_node:
                title = (await title_node.inner_text()).strip()
                href = await title_node.get_attribute("href")
                full_url = f"{self.base_url}{href}" if href.startswith(
                    '/') else href
                await self.push_to_redis(title, full_url)

    async def fetch_details(self, browser):
        """
        需求2 & 处理所有详情
        """
        print("\n=== 开始处理所有详情页任务 ===")
        while True:
            item_json = r.rpop(REDIS_KEY_QUEUE)
            if not item_json:
                print("Redis 队列已清空，详情处理完毕。")
                break

            item = json.loads(item_json)
            url = item['url']

            context = await browser.new_context()
            page = await context.new_page()
            try:
                # 优化加载速度，只等待 DOM
                await page.goto(url, wait_until="domcontentloaded", timeout=30000)

                # 提取 __NUXT_DATA__
                script_tag = page.locator("script#__NUXT_DATA__")
                if await script_tag.count() > 0:
                    raw_text = await script_tag.inner_text()
                    raw_json = json.loads(raw_text)

                    # 还原数据结构
                    clean_data = restore_nuxt_data(raw_json)

                    # 存储到 Redis Hash
                    result = {
                        "title": item['title'],
                        "url": url,
                        "data": clean_data
                    }
                    r.hset(REDIS_KEY_RESULT, url, json.dumps(
                        result, ensure_ascii=False))
                    r.expire(REDIS_KEY_RESULT, EXPIRE_TIME)
                    print(f"已保存详情: {item['title']}")
                else:
                    print(f"未找到数据标签: {url}")

            except Exception as e:
                print(f"处理失败 {url}: {e}")
            finally:
                await context.close()

    async def run(self):
        async with async_playwright() as p:
            # 建议开启 headless=True 以提高效率
            browser = await p.chromium.launch(headless=False)
            page = await browser.new_page()

            # 1. 启动监听
            page.on("response", self.handle_api_response)

            # 2. 访问主页并解析首屏
            print("正在打开主页...")
            await page.goto(f"{self.base_url}/article/", wait_until="networkidle")
            await self.parse_main_page(page)

            # 3. 需求3：循环滚动直到获取 10 组 API 数据
            print("开始滚动触发加载...")
            while not self.stop_scrolling:
                await page.mouse.wheel(0, 3000)
                await asyncio.sleep(1.5)  # 给一点加载时间
                # 防止死循环（例如页面到底了但没到10组）
                if await page.evaluate("window.innerHeight + window.scrollY >= document.body.scrollHeight"):
                    print("已到达页面底部，停止滚动。")
                    break

            # 4. 处理详情页
            await self.fetch_details(browser)

            print("\n所有任务已完成，程序退出。")
            await browser.close()


if __name__ == "__main__":
    # 运行前清理旧队列（可选）
    # r.delete(REDIS_KEY_QUEUE, REDIS_KEY_VISITED)

    spider = HuXiuSpider()
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
