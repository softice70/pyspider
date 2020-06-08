#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:
# Author: ryan
# Created on 2019-09-26 00:16:35


import inspect
import json
import optparse
import click
import sys
import time
from urllib import parse
import datetime
import requests
import tldextract
from urlparse2.urlparse1 import urlparse
from pyspider.libs import sample_handler
from progress.bar import Bar
from aio_downloader import AioDownloader

default_script = inspect.getsource(sample_handler)


@click.group()
def cli():
    pass


@click.command()
@click.option("-r", "--rate", type=float, default=0.1, help='rate of download, default=1')
@click.option("-b", "--burst", type=float, default=3, help='burst of download, default=3')
@click.option("-d", "--depth", type=int, default=0, help='depth to crawl the site, default=0')
@click.argument("list_file")
@click.argument("task_file")
def create_task(rate, burst, depth, list_file, task_file):
    with open(list_file, 'r') as fp:
        projects = {}
        url_list = []
        for line in fp.readlines():
            url = line.strip()
            if len(url) > 0:
                url_list.append(url)
        download_results = AioDownloader.aio_download(url_list, fetcher_count=50, show_process=True)
        for url, resp, e in download_results:
            if resp:
                if resp.status >= 400:
                    real_url = 'ERROR:%s' % resp.status
                else:
                    real_url = str(resp.real_url)
            else:
                real_url = 'ERROR:%s' % e if len(str(e)) > 0 else url
            project_name, hosts, tld_groups, url, real_url = gen_proj_name_hosts_tldgroup(url, real_url=real_url)
            add_project(projects, url, real_url, project_name, hosts, tld_groups)

        save_project_urls(projects, list_file + '.dbg')
        create_projects_info(projects, rate, burst, depth)
        save_task(projects, task_file)


@click.command()
@click.option("-s", "--group_size", type=int, default='100',
              help='number of projects in the same group, default=100')
@click.option("-m", "--max_running_count", type=int, default='20',
              help='number of projects running at the same time, default=20')
@click.argument("task_file")
def run_task(group_size, max_running_count, task_file):
    projects = load_task(task_file)
    try:
        bar = Bar('Processing', fill='@', suffix='[%(index)d/%(max)d - %(percent).1f%% - %(elapsed)ds]', max=len(projects))
        while True:
            finish_count = run(max_running_count)
            if finish_count > 0:
                update_project_status(projects)
                show_progress_info(bar, projects)
                rpc_delete_projects_in_db()
                rpc_restart_scheduler()
                time.sleep(5)
            pi_list = get_not_finished_projects(projects, group_size)
            if len(pi_list) > 0:
                rpc_create_projects(pi_list)
            else:
                show_progress_info(bar, projects)
                break
            save_task(projects, task_file)
        print('Done!')
    except KeyboardInterrupt:
        print('task is Interrupted!')
    save_task(projects, task_file)


@click.command()
@click.argument("task_file")
def save_task_status(task_file):
    _save_task_status(task_file)


def run(max_running_count):
    bar = None
    projects_cnt = 0
    while True:
        projects = get_projects()
        if not bar and projects:
            projects_cnt = len(projects)
            bar = Bar('Running', fill='#', suffix='[%(index)d/%(max)d - %(percent).1f%% - %(elapsed)ds]', max=projects_cnt)
        running_count = 0
        todo_list = []
        if projects:
            for project in projects:
                if project['status'] == 'RUNNING':
                    running_count += 1
                elif project['status'] == 'TODO':
                    todo_list.append(project)
        if bar:
            bar.goto(projects_cnt - len(todo_list) - running_count)
        if running_count + len(todo_list) == 0:
            break
        elif running_count < max_running_count and len(todo_list) > 0:
            pi_list = []
            for i in range(max_running_count - running_count):
                if i < len(todo_list):
                    pi_list.append(todo_list[i])
                else:
                    break
            if len(pi_list) > 0:
                rpc_set_projects_status(pi_list, 'RUNNING')
                time.sleep(5)
                rpc_run_projects(pi_list)
        time.sleep(30)
    if bar:
        bar.goto(projects_cnt)
        bar.finish()
    return projects_cnt


def wait_for_running(pi_queue, max_running_count=20):
    projects_cnt = pi_queue.qsize()
    bar = Bar('Running', fill='#', suffix='[%(index)d/%(max)d - %(percent).1f%% - %(elapsed)ds]', max=projects_cnt)
    while True:
        projects = get_projects()
        running_count = 0
        if projects:
            for project in projects:
                if project['status'] == 'RUNNING':
                    running_count += 1
        bar.goto(projects_cnt - pi_queue.qsize() - running_count)
        if running_count + pi_queue.qsize() == 0:
            break
        elif running_count < max_running_count and pi_queue.qsize() > 0:
            pi_list = []
            for i in range(max_running_count - running_count):
                if pi_queue.qsize() > 0:
                    pi_list.append(pi_queue.get())
                else:
                    break
            if len(pi_list) > 0:
                rpc_create_projects(pi_list)
                time.sleep(5)
                rpc_run_projects(pi_list)
        time.sleep(30)
    bar.goto(projects_cnt)
    bar.finish()


def create_project(url):
    project_name, hosts, tld_groups, url, real_url = gen_proj_name_hosts_tldgroup(url)
    if not real_url.startswith('ERROR'):
        script = {'script': (default_script
                             .replace('__DATE__', (datetime.datetime.now()
                                                   .strftime("%Y-%m-%d %H:%M:%S")))
                             .replace('__PROJECT_NAME__', project_name)
                             .replace('__START_URL__', real_url)
                             .replace('__MAIL_TO__', '')
                             .replace('__HOSTS__', hosts)
                             .replace('\'__DEPTH__\'', '1')
                             .replace('__TLD_GROUPS__', tld_groups))}
        data = parse.urlencode(script)

        urlTemplate = 'http://localhost:5000/debug/__PROJECT_NAME__/save'
        cmd_url = urlTemplate.replace('__PROJECT_NAME__', project_name)
        ret, ret_text = send_pyspider_cmd(cmd_url, data)
        if not ret or ret_text != 'ok':
            print("%s failed to create project: %s" % (real_url, ret_text))
            return None
        else:
            return project_name
    else:
        print("%s: Could not resolve host" % real_url)
        return None


def update_project_status(projects):
    projects_in_db = get_projects()
    if projects_in_db:
        for project in projects_in_db:
            if project['status'] == 'STOP':
                if project['group'] == 'detect':
                    projects[project['name']]['status'] = 'SUCCESS'
                else:
                    projects[project['name']]['status'] = project['group']
            else:
                projects[project['name']]['status'] = project['status']


def show_progress_info(bar, projects):
    finished_count = 0
    for project_name in projects:
        if projects[project_name]['status'] != 'TODO':
            finished_count += 1
    bar.goto(finished_count)


def add_project(projects, url, real_url, project_name, hosts, tld_groups):
    if project_name not in projects:
        projects[project_name] = (project_name, hosts, tld_groups, {url: real_url})
    else:
        pi = projects[project_name]
        if url not in pi[-1]:
            pi[-1][url] = real_url


def save_project_urls(projects, output_file):
    with open(output_file, 'w', encoding='utf-8') as fp:
        for project_name in projects:
            for url in projects[project_name][-1]:
                if url == projects[project_name][-1][url]:
                    fp.write('%s\n' % url)
                else:
                    fp.write('%s\t%s\n' % (url, projects[project_name][-1][url]))


def save_task(projects, output_file):
    with open(output_file, 'w', encoding='utf-8') as fp:
        fp.write(json.dumps(projects))
    _save_task_status(output_file)

def load_task(task_file):
    with open(task_file, 'r', encoding='utf-8') as fp:
        return json.loads(fp.readline())


def gen_project_info(projects, project_name, rate, burst, depth, init_status='TODO'):
    project_name, hosts, tld_groups, url_map = projects[project_name]
    depth = verify_depth(hosts, depth)
    url_list = []
    err_url_list = []
    for url in url_map:
        if not url_map[url].startswith('ERROR'):
            url_list.append(url_map[url])
        else:
            err_url_list.append(' - '.join((url, url_map[url])))
    if len(url_list) > 0:
        urls = ','.join(url_list)
    else:
        urls = ','.join(err_url_list)
        init_status = 'E-CREATE'

    project_info = {'name': project_name,
                    'urls': urls,
                    'rate': rate,
                    'burst': burst,
                    'status': init_status,
                    'group': 'detect',
                    'script': (default_script
                               .replace('__DATE__', (datetime.datetime.now()
                                                     .strftime("%Y-%m-%d %H:%M:%S")))
                               .replace('__PROJECT_NAME__', project_name)
                               .replace('__START_URL__', urls)
                               .replace('__MAIL_TO__', '')
                               .replace('__HOSTS__', hosts)
                               .replace('\'__DEPTH__\'', str(depth))
                               .replace('__TLD_GROUPS__', tld_groups))}
    return project_info


def gen_proj_name_hosts_tldgroup(url, real_url=None):
    if not real_url:
        real_url = get_real(url)
    if not real_url.startswith('ERROR'):
        is_redirect = real_url != url
        project_name, hosts, tld_groups = get_proj_name_hosts_tldgroup(real_url)
        p = urlparse(real_url)
        if p.path == '':
            real_url += '/'
            if not is_redirect:
                url = real_url
    else:
        project_name, hosts, tld_groups = get_proj_name_hosts_tldgroup(url)
    return project_name, hosts, tld_groups, url, real_url


def get_proj_name_hosts_tldgroup(real_url):
    tld_info = tldextract.extract(real_url)
    if not tld_info.fqdn.startswith('www.'):
        project_name = tld_info.fqdn
        hosts = tld_info.fqdn
        tld_groups = tld_info.registered_domain
    else:
        tld_groups = tld_info.registered_domain
        hosts = tld_info.fqdn[4:]
        project_name = hosts
    project_name = project_name.replace('-', '_').replace('.', '_')
    return project_name, hosts, tld_groups


def verify_depth(hosts, depth):
    if depth == 0:
        if len(hosts) == 1:
            depth = 2 if hosts[0].startswith('www.') else 1
        else:
            depth = 1
    return depth


def get_not_finished_projects(projects, max_size=100):
    projects_in_db = get_projects()
    pi_list = []
    for project_name in projects:
        if projects[project_name]['status'] == 'TODO':
            if projects_in_db:
                for project in projects_in_db:
                    if project['name'] == project_name:
                        continue
            pi_list.append(projects[project_name])
            if len(pi_list) >= max_size:
                break
    return pi_list


def create_projects_info(projects, rate, burst, depth):
    for project_name in projects:
        project_info = gen_project_info(projects, project_name, rate, burst, depth)
        projects[project_name] = project_info


def rpc_create_projects(projects_list):
    cmd_url = 'http://localhost:5000/create_projects'
    headers = {'Content-Type': 'application/json'}
    if len(projects_list) > 0:
        data = json.dumps(projects_list)
        ret, ret_text = send_pyspider_cmd(cmd_url, data, headers)
        return ret and ret_text == 'ok'
    else:
        return True


def rpc_run_projects(projects_list):
    for project in projects_list:
        if not rpc_run_project(project['name']):
            rpc_set_project_status(project['name'], 'TODO')


def rpc_run_project(project_name, retry_count=3):
    cmd_url = 'http://localhost:5000/run'
    data = parse.urlencode({"project": project_name})
    for i in range(retry_count):
        ret, ret_text = send_pyspider_cmd(cmd_url, data)
        if ret and json.loads(ret_text)['result']:
            return True
        else:
#            print("Project:%s\tfailed to run for the reason:%s" % (project_name, ret_text))
            time.sleep(5)
            continue
    return False


def rpc_set_projects_status(projects_list, status):
    for project in projects_list:
        rpc_set_project_status(project['name'], status)


def rpc_set_project_status(project_name, status, retry_count=3):
    cmd_url = 'http://localhost:5000/update'
    data = parse.urlencode({'name': 'status',
                            'value': status,
                            'pk': project_name})
    for i in range(retry_count):
        ret, ret_text = send_pyspider_cmd(cmd_url, data)
        if not ret or ret_text != 'ok':
#            print("Project:%s\tfailed to update project status for the reason:%s" % (project_name, ret_text))
            time.sleep(5)
            continue
        else:
            return True
    return False

def rpc_delete_projects_in_db():
    projects_in_db = get_projects()
    if projects_in_db:
        for project in projects_in_db:
            if project['status'] == 'STOP':
                rpc_set_project_group(project['name'], 'delete')
        time.sleep(2)
        for project in projects_in_db:
            if project['status'] == 'STOP':
                rpc_delete_project(project['name'])


def rpc_delete_project(project_name, retry_count=3):
    cmd_url = 'http://localhost:5000/delete'
    data = parse.urlencode({"project": project_name})
    for i in range(retry_count):
        ret, ret_text = send_pyspider_cmd(cmd_url, data)
        if ret and json.loads(ret_text)['result']:
            return True
        else:
#            print("Project:%s\tfailed to delete project for the reason:%s" % (project_name, ret_text))
            time.sleep(5)
            continue
    return False


def rpc_set_project_group(project_name, group, retry_count=3):
    cmd_url = 'http://localhost:5000/update'
    data = parse.urlencode({'name': 'group',
                            'value': group,
                            'pk': project_name})
    for i in range(retry_count):
        ret, ret_text = send_pyspider_cmd(cmd_url, data)
        if not ret or ret_text != 'ok':
#            print("Project:%s\tfailed to set project group for the reason:%s" % (project_name, ret_text))
            time.sleep(5)
            continue
        else:
            return True
    return False


def rpc_download_result(project_name, resultFileName):
    cmd_url = 'http://localhost:5000/results/dump/%s.json' % project_name
    r = requests.get(cmd_url, stream=True)
    try:
        r.raise_for_status()
        with open(resultFileName, "wb") as f:
            for chunk in r.iter_content(chunk_size=512):
                if chunk:
                    f.write(chunk)
        f.close()
        return True
    except Exception as e:
        print('failed to download result: %s' % (e))
        return False


def rpc_restart_scheduler():
    cmd_url = 'http://localhost:5000/restart_scheduler'
    r = requests.get(cmd_url, stream=True)
    print(r.text)


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


def download_result_file_ex(src_filename, result_filename):
    dataUrl = 'http://127.0.0.1:5000/results/dump/' + src_filename
    res = requests.get(dataUrl)
    try:
        res.raise_for_status()
        data = res.json()
        data['project'] = "abc"
        data['hosts'] = ["www.abc.com"]
        data['tld_groups'] = ["abc.com"]
        data['knowledge'] = {}
        with open(result_filename, "wb") as f:
            f.write(json.dumps(data))
        return True
    except Exception as exc:
        pass
    return False


def _save_task_status(task_file):
    projects = load_task(task_file)
    update_project_status(projects)
    with open(task_file + '.info', 'w', encoding='utf-8') as fp:
        for name in projects:
            fp.write('{}\t{}\t{}\n'.format(name, projects[name]['urls'], projects[name]['status']))


def get_projects():
    url = 'http://127.0.0.1:5000/get_projects'
    res = requests.get(url)
    try:
        res.raise_for_status()
        return res.json()
    except Exception as exc:
        pass
    return None


def get_real(o_url):
    '''重定向网址'''
    try:
        headers = {
            "Proxy-Connection": "keep-alive",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
            "Accept-Language": "zh-CN,zh;q=0.9",
            "User-Agent": "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.123 Safari/537.36",
            "Accept-Encoding": "gzip, deflate",
            "Connection": "Keep-Alive"
        }
        r = requests.get(o_url, allow_redirects=False, headers=headers)  # 禁止自动跳转
        if r.status_code == 301 or r.status_code == 302:
            try:
                location = r.headers['location']
                if location.startswith('http'):
                    return location
                else:
                    return parse.urljoin(o_url, location)
            except:
                return 'ERROR:%d' % r.status_code
        elif r.status_code != 200:
            return 'ERROR:%d' % r.status_code
        else:
            return o_url  # 返回源地址
    except Exception as e:
        return 'ERROR:%s' % e


def treat_real_url(resp):
    '''重定向网址'''
    try:
        if resp.status == 301 or resp.status == 302:
            try:
                if resp.real_url.startswith('http'):
                    return str(resp.real_url)
                else:
                    return parse.urljoin(str(resp.url), str(resp.real_url))
            except:
                return 'ERROR:%d' % resp.status
        elif resp.status != 200:
            return 'ERROR:%d' % resp.status
        else:
            return str(resp.real_url)  # 返回源地址
    except Exception as e:
        return 'ERROR:%s' % e


def main():
    #    download_result_file_ex('shiyebian', 'test.txt')
    # 命令行解析
    usage = 'usage: %prog urlListFile\n'
    parser = optparse.OptionParser(usage, version="%prog 0.9.0")
    parser.add_option("-r", "--rate", action="store", type="float", dest="rate", default=0.1,
                      help='rate of download, default=1')
    parser.add_option("-b", "--burst", action="store", type="float", dest="burst", default=3,
                      help='burst of download, default=3')
    parser.add_option("-d", "--depth", action="store", type="string", dest="depth", default='1',
                      help='depth to crawl the site, default=1')
    (options, args) = parser.parse_args()

    # 参数检查，不正确则退出
    if len(args) < 1:
        parser.print_help()
        sys.exit(1)

    create_task(options.rate, options.burst, options.depth, args[0])


if __name__ == '__main__':
    cli.add_command(create_task)
    cli.add_command(run_task)
    cli.add_command(save_task_status)
    cli()
