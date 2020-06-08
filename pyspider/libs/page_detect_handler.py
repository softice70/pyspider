#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# Created on __DATE__
# Project: __PROJECT_NAME__


import os
import json
import subprocess
import logging
from urllib import parse
from threading import Timer
import requests
import zipfile
from urldetector.urldetector import PageTypeDetector
from urldetector.pagetype import PageType


logger = logging.getLogger('PageDetectUtil')


class PageDetectUtil:
    def on_start_page(self, handler, start_urls, hosts, tld_groups):
        self.logger.info('on_start:handler  start_urls:%s' % ','.join(start_urls))
        tld_groups = tld_groups if len(tld_groups) > 0 else self._get_tld_groups(start_urls)
        handler.crawl(start_urls, callback=handler.index_page, validate_cert=False, fetch_type=None,
                   save={'level': 0,
                         'retry': False,
                         'tld_groups': tld_groups,
                         'hosts': hosts})
#            self.crawl(url, callback=self.index_page, save=projectData, headers={'Referer': ''})

    def on_index_page(self, handler, response, start_urls, max_crawl_depth, engine_type, config_path):
        # set the urls and tlds and hosts that u want the crawler to visit
        cur_level = response.save['level']
        tld_groups = response.save['tld_groups']
        hosts = response.save['hosts']

        title = response.doc('title').text()
        links = []
        urls = []
        for each in response.doc('a[href^="http"]').items():
            links.append((each.attr.href, each.text()))

        if cur_level == 0 and len(links) > 0:
            self._check_tld_host(response.url, tld_groups, hosts)
        if cur_level == 0 and not response.save['retry'] and len(links) <= 1:
            if len(links) == 0:
                start_urls = [url + '#retry' for url in start_urls]
            else:
                start_urls = [links[0][0]]
            handler.crawl(start_urls, callback=handler.index_page, validate_cert=False, fetch_type='js',
                       save={'level': 0,
                             'retry': True,
                             'tld_groups': tld_groups,
                             'hosts': hosts})
            self.logger.warn('index_page:handler  url:%s, level:%d, warn: home page is empty'
                             % (response.url, cur_level))
        elif cur_level + 1 <= max_crawl_depth:
            try:
                detector = PageTypeDetector(engine_type,
                                            config_path,
                                            hosts,
                                            tld_groups,
                                            True)
                detector.addUrlInfo(response.url, title, links, cur_level)
                detector.detect(onePageFlag=True)
                type_list = [PageType.TYPE_LIST_PAGE, PageType.TYPE_UNKNOWN]
                download_urls = detector.getUrlByType(type_list)
                if cur_level == 0 and len(download_urls) < 10:
                    download_urls = PageTypeDetector.filter_urls(links, tld_groups, hosts, response=response)
            except Exception as e:
                download_urls = PageTypeDetector.filter_urls(links, tld_groups, hosts)
                self.logger.error('index_page:handler  url:%s, level:%d, error:%s' % (response.url, cur_level, e))

            for item in download_urls:
                urls.append(item[0])
            if len(urls) > 0:
                self.logger.info('index_page:handler  url:%s, level:%d, download_urls:%s'
                                 % (response.url, cur_level, json.dumps(urls)))
                handler.crawl(urls,
                           headers={'Referer': response.url},
                           callback=handler.index_page,
                           validate_cert=False,
                           save={'level': cur_level + 1,
                                 'tld_groups': tld_groups,
                                 'hosts': hosts})
            else:
                self.logger.warn('index_page:handler  url:%s, level:%d, warn: download_urls is empty, links_in_page:%s'
                                 % (response.url, cur_level, json.dumps(links)))

        return {
            "url": response.url,
            "title": title,
            "level": cur_level,
            "links": links,
            "download_urls": urls
        }

    def on_end(self, response, task, start_urls, hosts, tld_groups, engine_type, config_path, workspace_path):
        result_path = workspace_path + 'result/'
        src_file_name = self.project_name + '.json'
        result_file_name = self.project_name + '.txt'
        data_file = result_path + src_file_name
        if self._download_result_file_ex(src_file_name, data_file, start_urls, hosts, tld_groups):
            result_file = result_path + result_file_name
            try:
                tld_groups = tld_groups if len(tld_groups) > 0 else self._get_tld_groups(start_urls)
                detector = PageTypeDetector(engine_type,
                                            config_path,
                                            hosts,
                                            tld_groups,
                                            True)
                result_count = detector.analyzeUrlTypeFromFile(data_file,
                                                [PageType.TYPE_LIST_PAGE],
                                                result_file,
                                                onePageFlag=False)
                if result_count > 1:
                    self._set_project_status('STOP')
#                    self._delete_project()
                    self.logger.info('on_finished:handler  urls:%s, finished!' % ','.join(start_urls))
                else:
                    self._set_project_group('E_0_RESULT')
                    self._set_project_status('STOP')
                    os.remove(result_file)
                    self.logger.error('on_finished:handler  urls:%s, error:0 result' % ','.join(start_urls))
            except Exception as e:
                self.logger.error('on_finished:handler  urls:%s, error:%s' % (','.join(start_urls), e))
                self._set_project_group('E_ANALYZE')
                self._set_project_status('STOP')
        else:
            self.logger.error('on_finished:handler  urls:%s, error:failed to download json file' % ','.join(start_urls))
            self._set_project_group('E_DOWNLOAD')
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
            self.logger.error("_set_project_status:handler  error: failed to set the status of project")
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
            self.logger.error("_set_project_group:handler  error: %s" % ret_text)
            return False
        else:
            return True

    def _delete_project(self):
        cmd_url = 'http://localhost:5000/delete'
        data = parse.urlencode({"project": self.project_name})
        if not self._send_pyspider_cmd(cmd_url, data):
            self.logger.error("_delete_project:handler  error: failed to delete the project")
            return False
        else:
            return True

    def _send_pyspider_cmd(self, cmd_url, data, headers=None):
        if headers is None:
            headers = {'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8'}
        r = requests.post(cmd_url, data=data, headers=headers)
        return r.status_code == requests.codes.ok, r.text

    def _download_result_file_ex(self, src_filename, result_filename, start_urls, hosts, tld_groups):
        dataUrl = 'http://127.0.0.1:5000/results/dump/' + src_filename
        res = requests.get(dataUrl)
        try:
            res.raise_for_status()
            project = {'project': self.project_name, 'start_urls': start_urls, 'hosts': hosts, 'tld_groups': tld_groups}
            with open(result_filename, "w", encoding='utf-8') as f:
                f.write("%s\n" % json.dumps(project))
                f.write(res.text)
            return True
        except Exception as e:
            self.logger.error("_download_result_file_ex:handler  error:%s" % e)
        return False

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
        except Exception as e:
            self.logger.error("_download_result_file:handler  error:%s" % e)
        return False

    def _zipfile(self, zip, *src):
        try:
            with zipfile.ZipFile(zip, mode="w") as f:
                for fname in src:
                    f.write(fname)
        except Exception as e:
            self.logger.error("_zipfile:handler  error:%s" % e)
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

    def _check_tld_host(self, url, tld_groups, hosts):
        tld, host = PageTypeDetector.getTldAndHost(url)
        if tld not in tld_groups:
            tld_groups.append(tld)
        if host not in hosts:
            hosts.append(host)
