import json
import redis
from fastapi import FastAPI, Response, Query
from datetime import datetime
from fastapi.responses import HTMLResponse
import hashlib
from spider.storage import NewsStorage

storage = NewsStorage()
app = FastAPI()


def get_url_id(url):
    return hashlib.md5(url.encode()).hexdigest()[:8]


@app.get("/", response_class=HTMLResponse)
async def lynx_index(
    cate: str = Query("", description="分类Key"),
    page: int = Query(1, ge=1),
    size: int = Query(30, ge=1)
):
    result = storage.get_news_list(cate, page, size)
    print(result)

    data_start = result["page"] if result["page"] == 1 else (
        result["page"]-1)*result["page_size"]
    data_end = result["page"]*result["page_size"]
    # 4. 构建 HTML
    html = f"""
    <html>
    <head><title>终端资讯站 - {cate}</title></head>
    <body bgcolor="#eeeeee">
        <h1>[ 资讯中心 ]</h1>
        <p>
            <b>分类：</b> 
            <a href="/?cat=huxiu:clean_data">虎嗅</a> | 
            <a href="/?cat=news:money163:details">网易财经</a>
            <a href="/?cat=other:data">其他源</a>
        </p>
        <hr>
        <h3>当前列表 ({data_start}-{data_end} / 共{result["total"]}条)</h3>
        <ul>
    """

    for item in result["items"]:
        print(item)
        print(type(item))
        # data = json.loads(data_str)
        article_id = item["id"].split("_")[-1]
        # 这里的链接带上 cat 参数，防止查看详情回来时分类丢失
        html += f"""<li><a href="/view/{article_id}?cate={item.get('category')}">{item.get("title")}</a></li>\n"""

    html += "</ul><hr><p>"

    # 5. 上下页逻辑
    if result["page"] > 1:
        html += f"""<a href="/?cate={cate}&page={page-1}">[ 上一页 ]</a> """
    if result["page"] < result["total"]:
        html += f"""<a href="/?cate={cate}&page={page+1}">[ 下一页 ]</a>"""

    html += """
        </p>
        <hr>
        <p>操作提示：使用箭头键导航，Enter进入，Backspace返回</p>
    </body>
    </html>
    """
    return html


@app.get("/view/{article_id}", response_class=HTMLResponse)
async def lynx_detail(article_id: str, cate: str = ""):
    key = cate+"_"+article_id
    target_data = storage.get_news_detail(key)
    if not target_data:
        return "<h1>文章未找到</h1><a href='/'>返回首页</a>"

    return f"""
    <html>
    <head><title>{target_data['title']}</title></head>
    <body>
        <p><a href="javascript:history.back()">[ 返回列表 ]</a></p>
        <h1>{target_data['title']}</h1>
        <p><b>作者：</b>{target_data.get('author')} | <b>时间：</b>{target_data.get('pubDate')}</p>
        <hr>
        {target_data.get('content', '')}
        <hr>
        <p><a href="javascript:history.back()">[ 返回列表 ]</a></p>
    </body>
    </html>
    """


def generate_rss_xml(articles):
    """
    将清洗后的精简数据包装成符合 Newsboat 离线阅读要求的 RSS 2.0 格式
    """
    # 按照 RSS 标准格式生成当前时间
    now = datetime.now().strftime("%a, %d %b %Y %H:%M:%S +0800")

    items_xml = ""
    for url, data_str in articles.items():
        try:
            data = json.loads(data_str)

            # 提取清洗后的字段
            title = data.get("title", "无标题")
            author = data.get("author", "未知作者")
            summary = data.get("summary", "")
            content = data.get("content", "")
            pic_path = data.get("pic_path", "")
            # 尝试使用数据自带的 pubDate，如果没有则用当前抓取时间
            pub_date_raw = data.get("pubDate", now)

            # --- 核心改进：拼接详情页显示内容 ---
            # 我们在 description 里手动拼出一个简单的 HTML 结构
            # 包含：封面图 + 作者 + 摘要 + 分割线 + 正文
            full_display_html = ""
            if pic_path:
                full_display_html += f'<img src="{pic_path}"><br>'

            full_display_html += f"<b>作者：{author}</b><br>"
            full_display_html += f"<i>摘要：{summary}</i><hr>"
            full_display_html += content  # 这里是带有 HTML 标签的富文本正文内容

            items_xml += f"""
        <item>
            <title><![CDATA[{title}]]></title>
            <link>{url}</link>
            <author><![CDATA[{author}]]></author>
            <description><![CDATA[{full_display_html}]]></description>
            <pubDate>{pub_date_raw}</pubDate>
            <guid isPermaLink="true">{url}</guid>
        </item>"""
        except Exception as e:
            print(f"生成 XML 条目失败: {e}")
            continue

    # 返回完整的 RSS 结构
    return f"""<?xml version="1.0" encoding="UTF-8" ?>
<rss version="2.0">
<channel>
    <title>虎嗅网 - 离线全文订阅</title>
    <link>https://www.huxiu.com/</link>
    <description>自动化生成的实时全文订阅资讯</description>
    <language>zh-cn</language>
    <lastBuildDate>{now}</lastBuildDate>
    {items_xml}
</channel>
</rss>"""


@app.get("/news")
async def get_news(format: str = Query("xml", regex="^(xml|json)$"), limit: int = Query(None, description="获取条数限制")):
    # 1. 先获取 Hash 中所有的 field (即 URL)
    all_urls = r.hkeys(REDIS_KEY_RESULT)
    # 2. 如果提供了 limit 参数，则进行切片（例如取前10条）
    target_urls = all_urls[:limit] if limit else all_urls
    # 3. 批量获取这些 URL 对应的数据内容
    if not target_urls:
        all_articles = {}
    else:
        # 使用 hmget 批量获取，提高效率
        values = r.hmget(REDIS_KEY_RESULT, target_urls)
        # 将 url 和对应的 json 字符串重新组合成字典
        all_articles = dict(zip(target_urls, values))

    # 从 Redis 获取所有已抓取的数据
    # all_articles = r.hgetall(REDIS_KEY_RESULT)

    if format == "json":
        # 返回 JSON 格式
        return {url: json.loads(val) for url, val in all_articles.items()}

    # 默认返回 XML 格式
    xml_content = generate_rss_xml(all_articles)
    return Response(content=xml_content, media_type="application/xml")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
