import json
import redis
import re

# --- 配置区 ---
r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
REDIS_KEY_RAW = "huxiu:details"       # 原始抓取的数据
REDIS_KEY_CLEAN = "huxiu:clean_data"  # 清洗后的精简数据


def clean_html(raw_html):
    """
    清洗富文本：移除冗余属性，保留基础标签供 Newsboat 渲染
    """
    if not raw_html:
        return ""

    # 移除所有 style 属性
    clean = re.sub(r'\s+style="[^"]*"', '', raw_html)
    # 移除所有 class 属性
    clean = re.sub(r'\s+class="[^"]*"', '', clean)
    # 移除所有 id 属性
    clean = re.sub(r'\s+id="[^"]*"', '', clean)
    # 移除多余的空白符
    clean = re.sub(r'\s+', ' ', clean)

    clean = re.sub(r'\s+data-check-id="[^"]*"', '', clean)
    clean = re.sub(r'\s+', ' ', clean)

    return clean.strip()


def process_cleaning():
    # 获取原始 Hash 表中所有的 field 和 value
    raw_data_map = r.hgetall(REDIS_KEY_RAW)
    print(f"[*] 开始清洗数据，总计: {len(raw_data_map)} 条")

    success_count = 0

    for url, val in raw_data_map.items():
        try:
            item = json.loads(val)
            # 原始数据中还原出来的数组在 data 键下
            raw_data_list = item.get("data", [])

            if not isinstance(raw_data_list, list):
                continue

            # 遍历数组寻找包含关键信息的字典
            for entry in raw_data_list:
                # 需求点：当发现有 summary 和 content 键的时候，这就是我们要的块
                if isinstance(entry, dict) and "summary" in entry and "content" in entry:

                    # 提取并构建精简版字典
                    clean_dict = {
                        "title": entry.get("title", item.get("title", "")),
                        "pic_path": entry.get("pic_path", "").split('?', 1)[0],
                        "url": url,
                        "author": entry.get("author", ""),
                        "summary": entry.get("summary", ""),
                        "pubDate": entry.get("fdateline", ""),
                        # 清洗 HTML
                        "content": clean_html(entry.get("content", ""))
                    }

                    # 存入新的 Redis Key (Hash 结构)
                    r.hset(REDIS_KEY_CLEAN, url, json.dumps(
                        clean_dict, ensure_ascii=False))

                    success_count += 1
                    print(f"[OK] 已提取: {clean_dict['title']}")
                    break  # 找到关键块后直接跳出当前文章的遍历

        except Exception as e:
            print(f"[Err] 处理失败 {url}: {e}")

    print(f"\n[*] 清洗完毕！成功处理 {success_count} 条。数据已存入: {REDIS_KEY_CLEAN}")


if __name__ == "__main__":
    process_cleaning()
