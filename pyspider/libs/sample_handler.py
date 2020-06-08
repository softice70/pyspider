#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# Created on __DATE__
# Project: __PROJECT_NAME__


from pyspider.libs.base_handler import *
from pyspider.libs.page_detect_handler import *
from urldetector.urldetector import EngineType


'''properties of the project'''
g_start_urls = '__START_URL__'.split(',')
g_tld_groups = '__TLD_GROUPS__'.split(',') if len('__TLD_GROUPS__') > 0 else []
g_hosts = '__HOSTS__'.split(',') if len('__HOSTS__') > 0 else []
g_max_crawl_depth = '__DEPTH__'
g_engine_type = EngineType.GOV_ENGINE

g_workspace_path = '/Users/wangdongsheng/pyspider/'
g_config_path = g_workspace_path + 'conf/'


class Handler(BaseHandler, PageDetectUtil):
    crawl_config = {
        "headers": {
            "Proxy-Connection": "keep-alive",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
            "Accept-Language": "zh-CN,zh;q=0.9",
            "User-Agent": "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.123 Safari/537.36",
            "Accept-Encoding": "gzip, deflate",
            "Connection": "Keep-Alive"
        },
        "timeout": 120,
        "connect_timeout": 30,
        "load_images": False
    }

    @every(minutes=5)
    def on_start(self):
        self.on_start_page(self, g_start_urls, g_hosts, g_tld_groups)

    @config(age=-1)
    def index_page(self, response):
        return self.on_index_page(self, response, g_start_urls, g_max_crawl_depth, g_engine_type, g_config_path)

    def on_finished(self, response, task):
        self.on_end(response, task, g_start_urls, g_hosts, g_tld_groups, g_engine_type, g_config_path, g_workspace_path)

