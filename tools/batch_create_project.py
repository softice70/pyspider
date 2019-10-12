#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:
# Author: ryan
# Created on 2019-09-26 00:16:35


import sys
import json
import optparse
import inspect
from urllib import parse
import requests
import datetime
import time
import tldextract
from pyspider.libs import sample_handler


default_script = inspect.getsource(sample_handler)


def cli(urlListFile, rate, burst, depth):
    with open(urlListFile, 'r') as fp:
        projects = []
        for line in fp.readlines():
            project_info = gen_project_info(line.strip(), rate, burst, depth)
            projects.append(project_info)

        if create_projects(projects):
            time.sleep(10)
            for project_info in projects:
                project_name = project_info['name']
                if not run_project(project_name):
                    if set_project_status(project_name, 'CHECKING'):
                        print('%s\'s status is set to CHECKING' % project_name)
                    else:
                        print('%s\'s status failed to set CHECKING' % project_name)
                else:
                    print('%s is running...' % project_name)


def create_project(url):
    project_name, hosts, tld_groups = gen_proj_name_hosts_tldgroup(url)
    script = {'script': (default_script
                .replace('__DATE__', (datetime.datetime.now()
                                        .strftime("%Y-%m-%d %H:%M:%S")))
                .replace('__PROJECT_NAME__', project_name)
                .replace('__START_URL__', url)
                .replace('__MAIL_TO__', '')
                .replace('__HOSTS__', hosts)
                .replace('\'__DEPTH__\'', '1')
                .replace('__TLD_GROUPS__', tld_groups))}
    data = parse.urlencode(script)

    urlTemplate = 'http://localhost:5000/debug/__PROJECT_NAME__/save'
    cmd_url = urlTemplate.replace('__PROJECT_NAME__', project_name)
    ret, ret_text = send_pyspider_cmd(cmd_url, data)
    if not ret or ret_text != 'ok':
        print("%s failed to create project: %s" % (url, ret_text))
        return None
    else:
        return project_name


def gen_project_info(url, rate, burst, depth):
    project_name, hosts, tld_groups = gen_proj_name_hosts_tldgroup(url)
    project_info = {'name': project_name,
                    'rate': rate,
                    'burst': burst,
                    'script': (default_script
                                .replace('__DATE__', (datetime.datetime.now()
                                                        .strftime("%Y-%m-%d %H:%M:%S")))
                                .replace('__PROJECT_NAME__', project_name)
                                .replace('__START_URL__', url)
                                .replace('__MAIL_TO__', '')
                                .replace('__HOSTS__', hosts)
                                .replace('\'__DEPTH__\'', depth)
                                .replace('__TLD_GROUPS__', tld_groups))}
    return project_info


def gen_proj_name_hosts_tldgroup(url):
    tld_info = tldextract.extract(url)
    hosts = tld_info.fqdn if tld_info.subdomain != 'www' else ''
    project_name = (tld_info.subdomain + '_' + tld_info.domain
                    if tld_info.subdomain != 'www' else tld_info.domain)
    project_name = project_name.replace('-', '_').replace('.', '_')
    tld_groups = tld_info.registered_domain
    return project_name, hosts, tld_groups


def set_project_status(project_name, status):
    cmd_url = 'http://localhost:5000/update'
    data = parse.urlencode({'name': 'status',
                             'value': status, 
                             'pk': project_name})
    ret, ret_text = send_pyspider_cmd(cmd_url, data)
    if not ret or ret_text != 'ok':
        print("Project:%s\tfailed to update project status for the reason:%s" % (project_name, ret_text))
        return False
    else:
        return True


def set_project_group(project_name, group):
    cmd_url = 'http://localhost:5000/update'
    data = parse.urlencode({'name': 'group',
                             'value': group, 
                             'pk': project_name})
    ret, ret_text = send_pyspider_cmd(cmd_url, data)
    if not ret or ret_text != 'ok':
        print("Project:%s\tfailed to set project group for the reason:%s" % (project_name, ret_text))
        return False
    else:
        return True


def create_projects(projects):
    cmd_url = 'http://localhost:5000/create_projects'
    headers = {'Content-Type': 'application/json'}
    data = json.dumps(projects)
    ret, ret_text = send_pyspider_cmd(cmd_url, data, headers)
    if ret and ret_text == 'ok':
        print("projects are created successfully")
        return True
    else:
        print("failed to create projects for the reason:%s" % (ret_text))
        return False


def run_project(project_name):
    cmd_url = 'http://localhost:5000/run'
    data = parse.urlencode({"project": project_name})
    ret, ret_text = send_pyspider_cmd(cmd_url, data)
    if ret and json.loads(ret_text)['result']:
        return True
    else:
        print("Project:%s\tfailed to run for the reason:%s" % (project_name, ret_text))
        return False


def delete_project(project_name):
    cmd_url = 'http://localhost:5000/delete'
    data = parse.urlencode({"project": project_name})
    ret, ret_text = send_pyspider_cmd(cmd_url, data)
    if ret and json.loads(ret_text)['result']:
        return True
    else:
        print("Project:%s\tfailed to delete project for the reason:%s" % (project_name, ret_text))
        return False


def download_result(project_name, resultFileName):
    cmd_url = 'http://localhost:5000/results/dump/%s.json' % project_name
    r = requests.get(cmd_url, stream=True)
    try:
        r.raise_for_status()
        with open(resultFileName, "wb") as f:
            for chunk in r.iter_content(chunk_size = 512):
                if chunk:
                    f.write(chunk)
        f.close()
        return True
    except Exception as e:
        print('failed to download result: %s' % (e))
        return False


def send_pyspider_cmd(cmd_url, data, headers=None):
    if headers is None:
        headers = {'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8'}
    r = requests.post(cmd_url, data=data, headers=headers)
    return r.status_code == requests.codes.ok, r.text


def format_array(str):
    if str is not None and len(str) > 0:
        return ','.join(["\'" + s + "\'" for s in str.split(',')])
    else:
        return ''


def main():
    # 命令行解析
    usage = 'usage: %prog urlListFile\n'
    parser = optparse.OptionParser(usage, version="%prog 0.9.0")
    parser.add_option("-r", "--rate", action="store", type="float", dest="rate", default=0.1, help='rate of download, default=1')
    parser.add_option("-b", "--burst", action="store", type="float", dest="burst", default=3, help='burst of download, default=3')
    parser.add_option("-d", "--depth", action="store", type="string", dest="depth", default='1', help='depth to crawl the site, default=1')
    (options, args) = parser.parse_args()

    # 参数检查，不正确则退出
    if len(args) < 1:
        parser.print_help()
        sys.exit(1)

    cli(args[0], options.rate, options.burst, options.depth)


if __name__ == '__main__':
    main()
