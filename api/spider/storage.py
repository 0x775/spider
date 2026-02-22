# storage.py
import redis
import json
import time
from typing import List, Optional, Dict, Any
from datetime import datetime


class NewsStorage:
    def __init__(self, redis_url: str = "redis://localhost:6379"):
        self.r = redis.Redis.from_url(redis_url, decode_responses=True)

        # Key 设计
        self.TIMELINE_ZSET = "news:timeline"
        self.CATEGORY_PREFIX = "news:category:"
        self.LIST_HASH_PREFIX = "news:list:"      # 列表字段 Hash
        self.DETAIL_PREFIX = "news:detail:"       # 详情 String
        self.CATEGORIES_SET = "news:categories"
        self.EXPIRE_TIME = 3600 * 24 * 7  # 过期时间(7天)

    def parse_publish_time(self, pub_date_str: str) -> int:
        """
        将 "2026-02-21 16:40:10" 格式转换为毫秒级时间戳
        :param pub_date_str: 时间字符串
        :return: 毫秒级时间戳（int）
        """
        if not pub_date_str:
            return int(time.time() * 1000)
        try:
            # 解析时间字符串
            dt = datetime.strptime(pub_date_str.strip(), "%Y-%m-%d %H:%M:%S")
            # 转换为毫秒级时间戳
            return int(dt.timestamp() * 1000)
        except (ValueError, AttributeError):
            # 解析失败时返回当前时间
            return int(time.time() * 1000)

    def save_news(self, news_id: str, news_data: Dict[str, Any],
                  category: str, publish_time: Optional[int] = None) -> bool:
        """
        保存新闻（列表字段 + 详情分离存储）
        """
        pipe = self.r.pipeline()

        # 时间戳（毫秒）
        score = publish_time or int(time.time() * 1000)

        # ========== 1. 存储列表字段（Hash，用于列表展示）==========
        list_hash_key = f"{self.LIST_HASH_PREFIX}{news_id}"
        list_fields = {
            "title": news_data.get("title", ""),
            "category": category,
            "publish_time": str(score),  # 存字符串
            "author": news_data.get("author", ""),
            "pic_path": news_data.get("pic_path", ""),
            "url": news_data.get("url", ""),
        }
        # print(list_hash_key)
        # print(list_fields)
        # print(news_data)
        pipe.hmset(list_hash_key, list_fields)
        pipe.expire(list_hash_key, self.EXPIRE_TIME)  # 7 天过期

        # ========== 2. 存储完整详情（String，用于详情页）==========
        detail_key = f"{self.DETAIL_PREFIX}{news_id}"
        pipe.setex(detail_key, self.EXPIRE_TIME,
                   json.dumps(news_data, ensure_ascii=False, separators=(',', ':')))

        # ========== 3. 添加到全局时间线 ==========
        pipe.zadd(self.TIMELINE_ZSET, {news_id: score})

        # ========== 4. 添加到分类时间线 ==========
        pipe.zadd(f"{self.CATEGORY_PREFIX}{category}", {news_id: score})

        # ========== 5. 记录分类 ==========
        pipe.sadd(self.CATEGORIES_SET, category)

        pipe.execute()
        return True

    def get_news_list(self, category: Optional[str] = None,
                      page: int = 1, page_size: int = 30) -> Dict[str, Any]:
        """
        查询新闻列表（只取列表字段，不取详情）
        """
        # 1. 确定 ZSET
        zset_key = f"{self.CATEGORY_PREFIX}{category}" if category else self.TIMELINE_ZSET

        # 2. 获取总数
        total = self.r.zcard(zset_key)
        if total == 0:
            return {"items": [], "total": 0, "page": page, "page_size": page_size, "has_next": False}

        # 3. 分页获取新闻 ID
        start = (page - 1) * page_size
        end = start + page_size - 1
        news_ids = self.r.zrevrange(zset_key, start, end)

        if not news_ids:
            return {"items": [], "total": total, "page": page, "page_size": page_size, "has_next": False}

        # 4. 批量获取列表字段（Hash）
        items = []
        pipe = self.r.pipeline()
        for news_id in news_ids:
            pipe.hgetall(f"{self.LIST_HASH_PREFIX}{news_id}")
        hash_results = pipe.execute()

        for news_id, fields in zip(news_ids, hash_results):
            if fields:
                # Hash 返回的 publish_time 是字符串，转为 int
                if "publish_time" in fields:
                    fields["publish_time"] = int(fields["publish_time"])
                fields["id"] = news_id
                items.append(fields)

        return {
            "items": items,
            "total": total,
            "page": page,
            "page_size": page_size,
            "has_next": start + len(items) < total
        }

    def get_news_detail(self, news_id: str) -> Optional[Dict[str, Any]]:
        """
        获取新闻详情（点击标题后调用）
        """
        detail = self.r.get(f"{self.DETAIL_PREFIX}{news_id}")
        if detail:
            data = json.loads(detail)
            data["id"] = news_id
            return data
        return None

    def get_categories(self) -> List[str]:
        """获取所有分类"""
        return list(self.r.smembers(self.CATEGORIES_SET))

    def delete_news(self, news_id: str, category: str) -> bool:
        """删除新闻"""
        pipe = self.r.pipeline()
        pipe.delete(f"{self.LIST_HASH_PREFIX}{news_id}")
        pipe.delete(f"{self.DETAIL_PREFIX}{news_id}")
        pipe.zrem(self.TIMELINE_ZSET, news_id)
        pipe.zrem(f"{self.CATEGORY_PREFIX}{category}", news_id)
        pipe.srem(self.CATEGORIES_SET, category)
        pipe.execute()
        return True

    def cleanup_old_news(self, days: int = 7) -> Dict[str, int]:
        """
        清理 N 天前的旧数据（定时任务调用）
        :return: 清理的数据条数
        """
        cutoff_time = int(time.time() * 1000) - (days * 86400 * 1000)
        stats = {"deleted": 0, "categories_cleaned": 0}

        # 1. 获取 7 天前的所有新闻 ID
        old_ids = self.r.zrangebyscore(self.TIMELINE_ZSET, 0, cutoff_time)

        if not old_ids:
            return stats

        # 2. 批量删除
        pipe = self.r.pipeline()

        for news_id in old_ids:
            # 删除 Hash 和 String
            pipe.delete(f"{self.LIST_HASH_PREFIX}{news_id}")
            pipe.delete(f"{self.DETAIL_PREFIX}{news_id}")
            stats["deleted"] += 1

        # 从 ZSET 中移除旧数据
        pipe.zremrangebyscore(self.TIMELINE_ZSET, 0, cutoff_time)

        # 清理各分类 ZSET
        categories = self.r.smembers(self.CATEGORIES_SET)
        for category in categories:
            pipe.zremrangebyscore(
                f"{self.CATEGORY_PREFIX}{category}", 0, cutoff_time)
            stats["categories_cleaned"] += 1

        pipe.execute()

        print(
            f"✅ 清理完成：删除 {stats['deleted']} 条旧新闻，清理 {stats['categories_cleaned']} 个分类")
        return stats
