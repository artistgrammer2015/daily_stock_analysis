# -*- coding: utf-8 -*-
"""
===================================
A股自选股智能分析系统 - 搜索服务模块
===================================

职责：
1. 提供统一的新闻搜索接口
2. 支持 Tavily 和 SerpAPI 两种搜索引擎
3. 多 Key 负载均衡和故障转移
4. 搜索结果缓存和格式化
"""

import logging
import random
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Dict, Any, Optional
from itertools import cycle

logger = logging.getLogger(__name__)


@dataclass
class SearchResult:
    """搜索结果数据类"""
    title: str
    snippet: str  # 摘要
    url: str
    source: str  # 来源网站
    published_date: Optional[str] = None
    
    def to_text(self) -> str:
        """转换为文本格式"""
        date_str = f" ({self.published_date})" if self.published_date else ""
        return f"【{self.source}】{self.title}{date_str}\n{self.snippet}"


@dataclass 
class SearchResponse:
    """搜索响应"""
    query: str
    results: List[SearchResult]
    provider: str  # 使用的搜索引擎
    success: bool = True
    error_message: Optional[str] = None
    search_time: float = 0.0  # 搜索耗时（秒）
    
    def to_context(self, max_results: int = 5) -> str:
        """将搜索结果转换为可用于 AI 分析的上下文"""
        if not self.success or not self.results:
            return f"搜索 '{self.query}' 未找到相关结果。"
        
        lines = [f"【{self.query} 搜索结果】（来源：{self.provider}）"]
        for i, result in enumerate(self.results[:max_results], 1):
            lines.append(f"\n{i}. {result.to_text()}")
        
        return "\n".join(lines)


class BaseSearchProvider(ABC):
    """搜索引擎基类"""
    
    def __init__(self, api_keys: List[str], name: str):
        """
        初始化搜索引擎
        
        Args:
            api_keys: API Key 列表（支持多个 key 负载均衡）
            name: 搜索引擎名称
        """
        self._api_keys = api_keys
        self._name = name
        self._key_cycle = cycle(api_keys) if api_keys else None
        self._key_usage: Dict[str, int] = {key: 0 for key in api_keys}
        self._key_errors: Dict[str, int] = {key: 0 for key in api_keys}
    
    @property
    def name(self) -> str:
        return self._name
    
    @property
    def is_available(self) -> bool:
        """检查是否有可用的 API Key"""
        return bool(self._api_keys)
    
    def _get_next_key(self) -> Optional[str]:
        """
        获取下一个可用的 API Key（负载均衡）
        
        策略：轮询 + 跳过错误过多的 key
        """
        if not self._key_cycle:
            return None
        
        # 最多尝试所有 key
        for _ in range(len(self._api_keys)):
            key = next(self._key_cycle)
            # 跳过错误次数过多的 key（超过 3 次）
            if self._key_errors.get(key, 0) < 3:
                return key
        
        # 所有 key 都有问题，重置错误计数并返回第一个
        logger.warning(f"[{self._name}] 所有 API Key 都有错误记录，重置错误计数")
        self._key_errors = {key: 0 for key in self._api_keys}
        return self._api_keys[0] if self._api_keys else None
    
    def _record_success(self, key: str) -> None:
        """记录成功使用"""
        self._key_usage[key] = self._key_usage.get(key, 0) + 1
        # 成功后减少错误计数
        if key in self._key_errors and self._key_errors[key] > 0:
            self._key_errors[key] -= 1
    
    def _record_error(self, key: str) -> None:
        """记录错误"""
        self._key_errors[key] = self._key_errors.get(key, 0) + 1
        logger.warning(f"[{self._name}] API Key {key[:8]}... 错误计数: {self._key_errors[key]}")
    
    @abstractmethod
    def _do_search(self, query: str, api_key: str, max_results: int) -> SearchResponse:
        """执行搜索（子类实现）"""
        pass
    
    def search(self, query: str, max_results: int = 5) -> SearchResponse:
        """
        执行搜索
        
        Args:
            query: 搜索关键词
            max_results: 最大返回结果数
            
        Returns:
            SearchResponse 对象
        """
        api_key = self._get_next_key()
        if not api_key:
            return SearchResponse(
                query=query,
                results=[],
                provider=self._name,
                success=False,
                error_message=f"{self._name} 未配置 API Key"
            )
        
        start_time = time.time()
        try:
            response = self._do_search(query, api_key, max_results)
            response.search_time = time.time() - start_time
            
            if response.success:
                self._record_success(api_key)
                logger.info(f"[{self._name}] 搜索 '{query}' 成功，返回 {len(response.results)} 条结果，耗时 {response.search_time:.2f}s")
            else:
                self._record_error(api_key)
            
            return response
            
        except Exception as e:
            self._record_error(api_key)
            elapsed = time.time() - start_time
            logger.error(f"[{self._name}] 搜索 '{query}' 失败: {e}")
            return SearchResponse(
                query=query,
                results=[],
                provider=self._name,
                success=False,
                error_message=str(e),
                search_time=elapsed
            )


class TavilySearchProvider(BaseSearchProvider):
    """
    Tavily 搜索引擎
    
    特点：
    - 专为 AI/LLM 优化的搜索 API
    - 免费版每月 1000 次请求
    - 返回结构化的搜索结果
    
    文档：https://docs.tavily.com/
    """
    
    def __init__(self, api_keys: List[str]):
        super().__init__(api_keys, "Tavily")
    
    def _do_search(self, query: str, api_key: str, max_results: int) -> SearchResponse:
        """执行 Tavily 搜索"""
        try:
            from tavily import TavilyClient
        except ImportError:
            return SearchResponse(
                query=query,
                results=[],
                provider=self.name,
                success=False,
                error_message="tavily-python 未安装，请运行: pip install tavily-python"
            )
        
        try:
            client = TavilyClient(api_key=api_key)
            
            # 执行搜索（优化：使用advanced深度、限制最近7天）
            response = client.search(
                query=query,
                search_depth="advanced",  # advanced 获取更多结果
                max_results=max_results,
                include_answer=False,
                include_raw_content=False,
                days=7,  # 只搜索最近7天的内容
            )
            
            # 记录原始响应到日志
            logger.info(f"[Tavily] 搜索完成，query='{query}', 返回 {len(response.get('results', []))} 条结果")
            logger.debug(f"[Tavily] 原始响应: {response}")
            
            # 解析结果
            results = []
            for item in response.get('results', []):
                results.append(SearchResult(
                    title=item.get('title', ''),
                    snippet=item.get('content', '')[:500],  # 截取前500字
                    url=item.get('url', ''),
                    source=self._extract_domain(item.get('url', '')),
                    published_date=item.get('published_date'),
                ))
            
            return SearchResponse(
                query=query,
                results=results,
                provider=self.name,
                success=True,
            )
            
        except Exception as e:
            error_msg = str(e)
            # 检查是否是配额问题
            if 'rate limit' in error_msg.lower() or 'quota' in error_msg.lower():
                error_msg = f"API 配额已用尽: {error_msg}"
            
            return SearchResponse(
                query=query,
                results=[],
                provider=self.name,
                success=False,
                error_message=error_msg
            )
    
    @staticmethod
    def _extract_domain(url: str) -> str:
        """从 URL 提取域名作为来源"""
        try:
            from urllib.parse import urlparse
            parsed = urlparse(url)
            domain = parsed.netloc.replace('www.', '')
            return domain or '未知来源'
        except:
            return '未知来源'


class SerpAPISearchProvider(BaseSearchProvider):
    """
    SerpAPI 搜索引擎
    
    特点：
    - 支持 Google、Bing、百度等多种搜索引擎
    - 免费版每月 100 次请求
    - 返回真实的搜索结果
    
    文档：https://serpapi.com/
    """
    
    def __init__(self, api_keys: List[str]):
        super().__init__(api_keys, "SerpAPI")
    
    def _do_search(self, query: str, api_key: str, max_results: int) -> SearchResponse:
        """执行 SerpAPI 搜索"""
        try:
            from serpapi import GoogleSearch
        except ImportError:
            return SearchResponse(
                query=query,
                results=[],
                provider=self.name,
                success=False,
                error_message="google-search-results 未安装，请运行: pip install google-search-results"
            )
        
        try:
            # 使用百度搜索（对中文股票新闻更友好）
            params = {
                "engine": "baidu",  # 使用百度搜索
                "q": query,
                "api_key": api_key,
            }
            
            search = GoogleSearch(params)
            response = search.get_dict()
            
            # 记录原始响应到日志
            logger.debug(f"[SerpAPI] 原始响应 keys: {response.keys()}")
            
            # 解析结果
            results = []
            organic_results = response.get('organic_results', [])
            
            for item in organic_results[:max_results]:
                results.append(SearchResult(
                    title=item.get('title', ''),
                    snippet=item.get('snippet', '')[:500],
                    url=item.get('link', ''),
                    source=item.get('source', self._extract_domain(item.get('link', ''))),
                    published_date=item.get('date'),
                ))
            
            return SearchResponse(
                query=query,
                results=results,
                provider=self.name,
                success=True,
            )
            
        except Exception as e:
            error_msg = str(e)
            return SearchResponse(
                query=query,
                results=[],
                provider=self.name,
                success=False,
                error_message=error_msg
            )
    
    @staticmethod
    def _extract_domain(url: str) -> str:
        """从 URL 提取域名"""
        try:
            from urllib.parse import urlparse
            parsed = urlparse(url)
            return parsed.netloc.replace('www.', '') or '未知来源'
        except:
            return '未知来源'


class BochaSearchProvider(BaseSearchProvider):
    """
    博查搜索引擎
    
    特点：
    - 专为AI优化的中文搜索API
    - 结果准确、摘要完整
    - 支持时间范围过滤和AI摘要
    - 兼容Bing Search API格式
    
    文档：https://bocha-ai.feishu.cn/wiki/RXEOw02rFiwzGSkd9mUcqoeAnNK
    """
    
    def __init__(self, api_keys: List[str]):
        super().__init__(api_keys, "Bocha")
    
    def _do_search(self, query: str, api_key: str, max_results: int) -> SearchResponse:
        """执行博查搜索"""
        try:
            import requests
        except ImportError:
            return SearchResponse(
                query=query,
                results=[],
                provider=self.name,
                success=False,
                error_message="requests 未安装，请运行: pip install requests"
            )
        
        try:
            # API 端点
            url = "https://api.bocha.cn/v1/web-search"
            
            # 请求头
            headers = {
                'Authorization': f'Bearer {api_key}',
                'Content-Type': 'application/json'
            }
            
            # 请求参数（严格按照API文档）
            payload = {
                "query": query,
                "freshness": "oneMonth",  # 搜索近一个月，适合捕获财报、公告等信息
                "summary": True,  # 启用AI摘要
                "count": min(max_results, 50)  # 最大50条
            }
            
            # 执行搜索
            response = requests.post(url, headers=headers, json=payload, timeout=10)
            
            # 检查HTTP状态码
            if response.status_code != 200:
                # 尝试解析错误信息
                try:
                    if response.headers.get('content-type', '').startswith('application/json'):
                        error_data = response.json()
                        error_message = error_data.get('message', response.text)
                    else:
                        error_message = response.text
                except:
                    error_message = response.text
                
                # 根据错误码处理
                if response.status_code == 403:
                    error_msg = f"余额不足: {error_message}"
                elif response.status_code == 401:
                    error_msg = f"API KEY无效: {error_message}"
                elif response.status_code == 400:
                    error_msg = f"请求参数错误: {error_message}"
                elif response.status_code == 429:
                    error_msg = f"请求频率达到限制: {error_message}"
                else:
                    error_msg = f"HTTP {response.status_code}: {error_message}"
                
                logger.warning(f"[Bocha] 搜索失败: {error_msg}")
                
                return SearchResponse(
                    query=query,
                    results=[],
                    provider=self.name,
                    success=False,
                    error_message=error_msg
                )
            
            # 解析响应
            try:
                data = response.json()
            except ValueError as e:
                error_msg = f"响应JSON解析失败: {str(e)}"
                logger.error(f"[Bocha] {error_msg}")
                return SearchResponse(
                    query=query,
                    results=[],
                    provider=self.name,
                    success=False,
                    error_message=error_msg
                )
            
            # 检查响应code
            if data.get('code') != 200:
                error_msg = data.get('msg') or f"API返回错误码: {data.get('code')}"
                return SearchResponse(
                    query=query,
                    results=[],
                    provider=self.name,
                    success=False,
                    error_message=error_msg
                )
            
            # 记录原始响应到日志
            logger.info(f"[Bocha] 搜索完成，query='{query}'")
            logger.debug(f"[Bocha] 原始响应: {data}")
            
            # 解析搜索结果
            results = []
            web_pages = data.get('data', {}).get('webPages', {})
            value_list = web_pages.get('value', [])
            
            for item in value_list[:max_results]:
                # 优先使用summary（AI摘要），fallback到snippet
                snippet = item.get('summary') or item.get('snippet', '')
                
                # 截取摘要长度
                if snippet:
                    snippet = snippet[:500]
                
                results.append(SearchResult(
                    title=item.get('name', ''),
                    snippet=snippet,
                    url=item.get('url', ''),
                    source=item.get('siteName') or self._extract_domain(item.get('url', '')),
                    published_date=item.get('datePublished'),  # UTC+8格式，无需转换
                ))
            
            logger.info(f"[Bocha] 成功解析 {len(results)} 条结果")
            
            return SearchResponse(
                query=query,
                results=results,
                provider=self.name,
                success=True,
            )
            
        except requests.exceptions.Timeout:
            error_msg = "请求超时"
            logger.error(f"[Bocha] {error_msg}")
            return SearchResponse(
                query=query,
                results=[],
                provider=self.name,
                success=False,
                error_message=error_msg
            )
        except requests.exceptions.RequestException as e:
            error_msg = f"网络请求失败: {str(e)}"
            logger.error(f"[Bocha] {error_msg}")
            return SearchResponse(
                query=query,
                results=[],
                provider=self.name,
                success=False,
                error_message=error_msg
            )
        except Exception as e:
            error_msg = f"未知错误: {str(e)}"
            logger.error(f"[Bocha] {error_msg}")
            return SearchResponse(
                query=query,
                results=[],
                provider=self.name,
                success=False,
                error_message=error_msg
            )
    
    @staticmethod
    def _extract_domain(url: str) -> str:
        """从 URL 提取域名作为来源"""
        try:
            from urllib.parse import urlparse
            parsed = urlparse(url)
            domain = parsed.netloc.replace('www.', '')
            return domain or '未知来源'
        except:
            return '未知来源'


class SearchService:
    """
    搜索服务
    
    功能：
    1. 管理多个搜索引擎
    2. 自动故障转移
    3. 结果聚合和格式化
    """
    
    def __init__(
        self,
        bocha_keys: Optional[List[str]] = None,
        tavily_keys: Optional[List[str]] = None,
        serpapi_keys: Optional[List[str]] = None,
    ):
        """
        初始化搜索服务
        
        Args:
            bocha_keys: 博查搜索 API Key 列表
            tavily_keys: Tavily API Key 列表
            serpapi_keys: SerpAPI Key 列表
        """
        self._providers: List[BaseSearchProvider] = []
        
        # 初始化搜索引擎（按优先级排序）
        # 1. Bocha 优先（中文搜索优化，AI摘要）
        if bocha_keys:
            self._providers.append(BochaSearchProvider(bocha_keys))
            logger.info(f"已配置 Bocha 搜索，共 {len(bocha_keys)} 个 API Key")
        
        # 2. Tavily（免费额度更多，每月 1000 次）
        if tavily_keys:
            self._providers.append(TavilySearchProvider(tavily_keys))
            logger.info(f"已配置 Tavily 搜索，共 {len(tavily_keys)} 个 API Key")
        
        # 3. SerpAPI 作为备选（每月 100 次）
        if serpapi_keys:
            self._providers.append(SerpAPISearchProvider(serpapi_keys))
            logger.info(f"已配置 SerpAPI 搜索，共 {len(serpapi_keys)} 个 API Key")
        
        if not self._providers:
            logger.warning("未配置任何搜索引擎 API Key，新闻搜索功能将不可用")
    
    @property
    def is_available(self) -> bool:
        """检查是否有可用的搜索引擎"""
        return any(p.is_available for p in self._providers)
    
    def search_stock_news(
        self,
        stock_code: str,
        stock_name: str,
        max_results: int = 5,
        focus_keywords: Optional[List[str]] = None
    ) -> SearchResponse:
        """
        搜索股票相关新闻
        
        Args:
            stock_code: 股票代码
            stock_name: 股票名称
            max_results: 最大返回结果数
            focus_keywords: 重点关注的关键词列表
            
        Returns:
            SearchResponse 对象
        """
        # 默认重点关注关键词（基于交易理念）
        if focus_keywords is None:
            focus_keywords = [
                "年报预告", "业绩预告", "业绩快报",  # 业绩相关
                "减持", "增持", "回购",              # 股东动向
                "机构调研", "机构评级",              # 机构动向
                "利好", "利空",                      # 消息面
                "合同", "订单", "中标",              # 业务进展
            ]
        
        # 构建搜索查询（优化搜索效果）
        # 主查询：股票名称 + 核心关键词
        query = f"{stock_name} {stock_code} 股票 最新消息"
        
        logger.info(f"搜索股票新闻: {stock_name}({stock_code})")
        
        # 依次尝试各个搜索引擎
        for provider in self._providers:
            if not provider.is_available:
                continue
            
            response = provider.search(query, max_results)
            
            if response.success and response.results:
                logger.info(f"使用 {provider.name} 搜索成功")

                # 打印搜索结果的标题和URL
                logger.info(f"[{stock_name}] 股票新闻搜索结果:")
                for i, result in enumerate(response.results, 1):
                    logger.info(f"  {i}. 📄 {result.title}")
                    logger.info(f"     🔗 {result.url}")

                return response
            else:
                logger.warning(f"{provider.name} 搜索失败: {response.error_message}，尝试下一个引擎")
        
        # 所有引擎都失败
        return SearchResponse(
            query=query,
            results=[],
            provider="None",
            success=False,
            error_message="所有搜索引擎都不可用或搜索失败"
        )
    
    def search_stock_events(
        self,
        stock_code: str,
        stock_name: str,
        event_types: Optional[List[str]] = None
    ) -> SearchResponse:
        """
        搜索股票特定事件（年报预告、减持等）
        
        专门针对交易决策相关的重要事件进行搜索
        
        Args:
            stock_code: 股票代码
            stock_name: 股票名称
            event_types: 事件类型列表
            
        Returns:
            SearchResponse 对象
        """
        if event_types is None:
            event_types = ["年报预告", "减持公告", "业绩快报"]
        
        # 构建针对性查询
        event_query = " OR ".join(event_types)
        query = f"{stock_name} ({event_query})"
        
        logger.info(f"搜索股票事件: {stock_name}({stock_code}) - {event_types}")
        
        # 依次尝试各个搜索引擎
        for provider in self._providers:
            if not provider.is_available:
                continue
            
            response = provider.search(query, max_results=5)
            
            if response.success:
                # 打印搜索结果的标题和URL
                if response.results:
                    logger.info(f"[{stock_name}] 股票事件搜索结果:")
                    for i, result in enumerate(response.results, 1):
                        logger.info(f"  {i}. 📄 {result.title}")
                        logger.info(f"     🔗 {result.url}")

                return response
        
        return SearchResponse(
            query=query,
            results=[],
            provider="None",
            success=False,
            error_message="事件搜索失败"
        )
    
    def search_comprehensive_intel(
        self,
        stock_code: str,
        stock_name: str,
        max_searches: int = 3
    ) -> Dict[str, SearchResponse]:
        """
        多维度情报搜索（同时使用多个引擎、多个维度）
        
        搜索维度：
        1. 最新消息 - 近期新闻动态
        2. 风险排查 - 减持、处罚、利空
        3. 业绩预期 - 年报预告、业绩快报
        
        Args:
            stock_code: 股票代码
            stock_name: 股票名称
            max_searches: 最大搜索次数
            
        Returns:
            {维度名称: SearchResponse} 字典
        """
        results = {}
        search_count = 0
        
        # 定义搜索维度
        search_dimensions = [
            {
                'name': 'latest_news',
                'query': f"{stock_name} {stock_code} 最新 新闻 2026年1月",
                'desc': '最新消息'
            },
            {
                'name': 'risk_check', 
                'query': f"{stock_name} 减持 处罚 利空 风险",
                'desc': '风险排查'
            },
            {
                'name': 'earnings',
                'query': f"{stock_name} 年报预告 业绩预告 业绩快报 2025年报",
                'desc': '业绩预期'
            },
        ]
        
        logger.info(f"开始多维度情报搜索: {stock_name}({stock_code})")
        
        # 轮流使用不同的搜索引擎
        provider_index = 0
        
        for dim in search_dimensions:
            if search_count >= max_searches:
                break
            
            # 选择搜索引擎（轮流使用）
            available_providers = [p for p in self._providers if p.is_available]
            if not available_providers:
                break
            
            provider = available_providers[provider_index % len(available_providers)]
            provider_index += 1
            
            logger.info(f"[情报搜索] {dim['desc']}: 使用 {provider.name}")
            
            response = provider.search(dim['query'], max_results=3)
            results[dim['name']] = response
            search_count += 1
            
            if response.success:
                logger.info(f"[情报搜索] {dim['desc']}: 获取 {len(response.results)} 条结果")

                # 打印搜索结果的标题和URL
                if response.results:
                    logger.info(f"[情报搜索] {dim['desc']} 搜索结果:")
                    for i, result in enumerate(response.results, 1):
                        logger.info(f"  {i}. 📄 {result.title}")
                        logger.info(f"     🔗 {result.url}")

            else:
                logger.warning(f"[情报搜索] {dim['desc']}: 搜索失败 - {response.error_message}")
            
            # 短暂延迟避免请求过快
            time.sleep(0.5)
        
        return results
    
    def format_intel_report(self, intel_results: Dict[str, SearchResponse], stock_name: str) -> str:
        """
        格式化情报搜索结果为报告
        
        Args:
            intel_results: 多维度搜索结果
            stock_name: 股票名称
            
        Returns:
            格式化的情报报告文本
        """
        lines = [f"【{stock_name} 情报搜索结果】"]
        
        # 最新消息
        if 'latest_news' in intel_results:
            resp = intel_results['latest_news']
            lines.append(f"\n📰 最新消息 (来源: {resp.provider}):")
            if resp.success and resp.results:
                for i, r in enumerate(resp.results[:3], 1):
                    date_str = f" [{r.published_date}]" if r.published_date else ""
                    lines.append(f"  {i}. {r.title}{date_str}")
                    lines.append(f"     📄 {r.snippet[:100]}...")
                    lines.append(f"     🔗 {r.url}")
            else:
                lines.append("  未找到相关消息")
        
        # 风险排查
        if 'risk_check' in intel_results:
            resp = intel_results['risk_check']
            lines.append(f"\n⚠️ 风险排查 (来源: {resp.provider}):")
            if resp.success and resp.results:
                for i, r in enumerate(resp.results[:3], 1):
                    lines.append(f"  {i}. {r.title}")
                    lines.append(f"     📄 {r.snippet[:100]}...")
                    lines.append(f"     🔗 {r.url}")
            else:
                lines.append("  未发现明显风险信号")
        
        # 业绩预期
        if 'earnings' in intel_results:
            resp = intel_results['earnings']
            lines.append(f"\n📊 业绩预期 (来源: {resp.provider}):")
            if resp.success and resp.results:
                for i, r in enumerate(resp.results[:3], 1):
                    lines.append(f"  {i}. {r.title}")
                    lines.append(f"     📄 {r.snippet[:100]}...")
                    lines.append(f"     🔗 {r.url}")
            else:
                lines.append("  未找到业绩相关信息")
        
        return "\n".join(lines)
    
    def batch_search(
        self,
        stocks: List[Dict[str, str]],
        max_results_per_stock: int = 3,
        delay_between: float = 1.0
    ) -> Dict[str, SearchResponse]:
        """
        批量搜索多只股票新闻
        
        Args:
            stocks: 股票列表 [{"code": "300389", "name": "艾比森"}, ...]
            max_results_per_stock: 每只股票的最大结果数
            delay_between: 每次搜索之间的延迟（秒）
            
        Returns:
            {股票代码: SearchResponse} 字典
        """
        results = {}
        
        for i, stock in enumerate(stocks):
            if i > 0:
                time.sleep(delay_between)
            
            code = stock.get('code', '')
            name = stock.get('name', '')
            
            response = self.search_stock_news(code, name, max_results_per_stock)
            results[code] = response
        
        return results


# === 便捷函数 ===
_search_service: Optional[SearchService] = None


def get_search_service() -> SearchService:
    """获取搜索服务单例"""
    global _search_service
    
    if _search_service is None:
        from config import get_config
        config = get_config()
        
        _search_service = SearchService(
            bocha_keys=config.bocha_api_keys,
            tavily_keys=config.tavily_api_keys,
            serpapi_keys=config.serpapi_keys,
        )
    
    return _search_service


def reset_search_service() -> None:
    """重置搜索服务（用于测试）"""
    global _search_service
    _search_service = None


if __name__ == "__main__":
    # 测试搜索服务
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s | %(levelname)-8s | %(name)-20s | %(message)s'
    )
    
    # 手动测试（需要配置 API Key）
    service = get_search_service()
    
    if service.is_available:
        print("=== 测试股票新闻搜索 ===")
        response = service.search_stock_news("300389", "艾比森")
        print(f"搜索状态: {'成功' if response.success else '失败'}")
        print(f"搜索引擎: {response.provider}")
        print(f"结果数量: {len(response.results)}")
        print(f"耗时: {response.search_time:.2f}s")
        print("\n" + response.to_context())
    else:
        print("未配置搜索引擎 API Key，跳过测试")
