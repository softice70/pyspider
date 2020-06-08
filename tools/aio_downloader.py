#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:
# Author: ryan
# Created on 2019-09-26 00:16:35

import asyncio, aiohttp
from aiohttp import TCPConnector
from progress.bar import Bar
import logging
from functools import wraps


log = logging.getLogger(__name__)


def retry(*exceptions, retries=3, cooldown=1, verbose=True):
    """Decorate an async function to execute it a few times before giving up.
    Hopes that problem is resolved by another side shortly.

    Args:
        exceptions (Tuple[Exception]) : The exceptions expected during function execution
        retries (int): Number of retries of function execution.
        cooldown (int): Seconds to wait before retry.
        verbose (bool): Specifies if we should log about not successful attempts.
    """

    def wrap(func):
        @wraps(func)
        async def inner(*args, **kwargs):
            retries_count = 0

            while True:
                try:
                    result = await func(*args, **kwargs)
                except exceptions as err:
                    retries_count += 1
                    message = "Exception during {} execution. " \
                              "{} of {} retries attempted".format(func, retries_count, retries)

                    if retries_count > retries:
                        verbose and log.exception(message)
                        raise Exception(func.__qualname__, args, kwargs) from err
                    else:
                        verbose and log.warning(message)

                    if cooldown:
                        await asyncio.sleep(cooldown)
                else:
                    return result
        return inner
    return wrap


class AioDownloader:
    # 获取链接,下载文件
    @staticmethod
    @retry(aiohttp.ServerTimeoutError)
    async def _fetch(sem, session: aiohttp.ClientSession, url: str):
        # 控制协程并发数量
        with (await sem):
            try:
                return url, await session.get(url), None
            except Exception as e:
                return url, None, e


    @staticmethod
    async def _download(url_list, results, sem: asyncio.Semaphore, show_process=False):
        headers = {
            "Proxy-Connection": "keep-alive",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
            "Accept-Language": "zh-CN,zh;q=0.9",
            "User-Agent": "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.123 Safari/537.36",
            "Accept-Encoding": "gzip, deflate",
            "Connection": "Keep-Alive"
        }
        async with aiohttp.ClientSession(connector=TCPConnector(verify_ssl=False), headers=headers,
                                         timeout=aiohttp.ClientTimeout(total=60, sock_connect=30)) as session:
            tasks = []
            for url in url_list:
                # 构造一个协程列表
                tasks.append(asyncio.ensure_future(AioDownloader._fetch(sem, session, url)))
            # 等待返回结果
            tasks_iter = asyncio.as_completed(tasks)
            # 创建一个进度条
            bar = Bar('Downloading', fill='#', suffix='[%(index)d/%(max)d - %(percent).1f%% - %(elapsed)ds]',
                      max=len(url_list)) if show_process else None
            for coroutine in tasks_iter:
                # 获取结果
                res = await coroutine
                results.append(res)
                if bar:
                    bar.next()
            if bar:
                bar.finish()

    @staticmethod
    def aio_download(url_list, fetcher_count=10, show_process=False):
        # 获取事件循环
        lp = asyncio.get_event_loop()
        # 创建一个信号量以防止DDos
        sem = asyncio.Semaphore(fetcher_count)
        results = []
        lp.run_until_complete(AioDownloader._download(url_list, results, sem, show_process=show_process))
        lp.close()
        return results

