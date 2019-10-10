#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# Created on __DATE__
# Project: __PROJECT_NAME__


import os
import subprocess
from urllib import parse
from threading import Timer
import requests
import zipfile
from pyspider.libs.base_handler import *
from urldetector.urldetector import PageTypeDetector, PageType


'''properties of the project'''
g_start_urls = '__START_URL__'.split(',')
g_tld_groups = '__TLD_GROUPS__'.split(',')
g_hosts = '__HOSTS__'.split(',')
g_knowledge = {}

g_workspace_path = '/Users/wangdongsheng/pyspider/'
g_config_path = g_workspace_path + 'conf/'
g_max_crawl_level = 1


class Handler(BaseHandler):
    crawl_config = {
        "headers": {
            "Proxy-Connection": "keep-alive",
            "Accept": "*/*",
            "Accept-Language": "zh-CN,zh;q=0.8,en-US;q=0.6,en;q=0.4",
            "User-Agent": "Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36",
            "Accept-Encoding": "gzip, deflate, sdch",
            "DNT": "1",
            "Connection": "Keep-Alive"
        }
    }

    @every(minutes=5)
    def on_start(self):
        self.crawl(g_start_urls, callback=self.index_page, validate_cert=False, save={'level': 0})
#            self.crawl(url, callback=self.index_page, save=projectData, headers={'Referer': ''})

    @config(age=-1)
    def index_page(self, response):
        # set the urls and tlds and hosts that u want the crawler to visit
        tld_groups = g_tld_groups if len(g_tld_groups) > 0 else self._get_tld_groups(g_start_urls)
        cur_level = response.save['level']

        links = []
        for each in response.doc('a[href^="http"]').items():
            links.append((each.attr.href, each.text()))

        if cur_level + 1 <= g_max_crawl_level:
            try:
                detector = PageTypeDetector(g_config_path,
                                            g_hosts,
                                            tld_groups,
                                            True,
                                            g_knowledge)
                detector.addUrlInfo(response.url, links)
                detector.detect(onePageFlag=True)
                type_list = [PageType.TYPE_LIST_PAGE, PageType.TYPE_UNKNOWN]
                download_urls = detector.getUrlByType(type_list)
            except Exception as e:
                download_urls = links
                self.logger.exception(e)

            urls = []
            for item in download_urls:
                urls.append(item[0])
            self.crawl(urls,
                       headers={'Referer': response.url},
                       callback=self.index_page,
                       validate_cert=False,
                       save={'level': cur_level + 1})

        return {
            "url": response.url,
            "title": response.doc('title').text(),
            "level": cur_level,
            "links": links
        }

    def on_finished(self, response, task):
        result_path = g_workspace_path + 'result/'
        src_file_name = self.project_name + '.json'
        result_file_name = self.project_name + '.txt'
        data_file = result_path + src_file_name
        if self._download_result_file(src_file_name, data_file):
            result_file = result_path + result_file_name
            try:
                tld_groups = g_tld_groups if len(g_tld_groups) > 0 else self._get_tld_groups(g_start_urls)
                detector = PageTypeDetector(g_config_path,
                                            g_hosts,
                                            tld_groups,
                                            True,
                                            g_knowledge)
                result_count = detector.analyzeUrlTypeFromFile(data_file,
                                                [PageType.TYPE_LIST_PAGE],
                                                result_file,
                                                onePageFlag=False)
                subject = 'the result of index page finder'
                mailto = '__MAIL_TO__'
                if mailto.strip():
                    zipFile = '%s%s.zip' % (result_path, self.project_name)
                    self._zipfile(zipFile, data_file, result_file)
                    sendMailCmd = ('echo "Results file is in the attachment." | mail -s \"%s -- %s\" -a %s %s'
                                    % (subject.decode('utf-8'), self.project_name, zipFile, mailto))
                    self._exec_cmd(sendMailCmd)
                    try:
                        os.remove(data_file)
                        os.remove(result_file)
                    except Exception:
                        pass

                if result_count > 0:
                    self._set_project_status('STOP')
                    self._delete_project()
                else:
                    self._set_project_group('E_0_Result')
                    self._set_project_status('STOP')
                    self.logger.error('handler exception %s\turls:%s\t:on_finished error:0 result' % (self.project_name, ','.join(g_start_urls)))
            except Exception as e:
                self.logger.error('handler exception %s\turls:%s\t:on_finished error:%s' % (self.project_name, ','.join(g_start_urls), e))
                self._set_project_group('E_Analyze')
                self._set_project_status('STOP')
        else:
            self.logger.error('handler exception %s\turls:%s\t:on_finished error:failed to download json file' % (self.project_name, ','.join(g_start_urls)))
            self._set_project_group('E_Download')
            self._set_project_status('STOP')

    def _exec_cmd(self, cmd):
        timeout_in_sec = 60
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)

        def kill_process():
            try:
                p.kill()
            except OSError:
                # Swallow the error
                pass

        timer = Timer(timeout_in_sec, kill_process)
        timer.start()
        p_ret = p.wait()
        timer.cancel()
        if p_ret == 0:
            return True
        else:
            return False

    def _set_project_status(self, status):
        cmd_url = 'http://localhost:5000/update'
        data = parse.urlencode({'name': 'status',
                                'value': status,
                                'pk': self.project_name})
        if not self._send_pyspider_cmd(cmd_url, data):
            self.logger.error("handler exception %s: _set_project_status error" % self.project_name)
            return False
        else:
            return True

    def _set_project_group(self, group):
        cmd_url = 'http://localhost:5000/update'
        data = parse.urlencode({'name': 'group',
                                'value': group,
                                'pk': self.project_name})
        ret, ret_text = self._send_pyspider_cmd(cmd_url, data)
        if not ret or ret_text != 'ok':
            self.logger.error("handler exception %s: _set_project_group error:%s" % (self.project_name, ret_text))
            return False
        else:
            return True

    def _delete_project(self):
        cmd_url = 'http://localhost:5000/delete'
        data = parse.urlencode({"project": self.project_name})
        if not self._send_pyspider_cmd(cmd_url, data):
            self.logger.error("handler exception %s: _delete_project error" % self.project_name)
            return False
        else:
            return True

    def _send_pyspider_cmd(self, cmd_url, data, headers=None):
        if headers is None:
            headers = {'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8'}
        r = requests.post(cmd_url, data=data, headers=headers)
        return r.status_code == requests.codes.ok, r.text

    def _download_result_file(self, src_filename, result_filename):
        dataUrl = 'http://127.0.0.1:5000/results/dump/' + src_filename
        res = requests.get(dataUrl)
        try:
            res.raise_for_status()
            with open(result_filename, "wb") as f:
                for chunk in res.iter_content(chunk_size=512):
                    if chunk:
                        f.write(chunk)
            return True
        except Exception as exc:
            self.logger.error('handler exception %s: _download_result_file, error:%s' % (self.project_name, exc))
        return False

    def _zipfile(self, zip, *src):
        try:
            with zipfile.ZipFile(zip, mode="w") as f:
                for fname in src:
                    f.write(fname)
        except Exception as e:
            self.logger.exception(e)
        finally:
            f.close()

    def _get_tld_groups(self, urls):
        tld_groups = []
        for url in urls:
            url = url.strip()
            if url == '':
                continue

            tld, host = PageTypeDetector.getTldAndHost(url)
            if tld not in tld_groups:
                tld_groups.append(tld)
        return tld_groups
