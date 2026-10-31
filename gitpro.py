# -*- coding: utf-8 -*-
"""
gitpro — GitHub 加速器
================================================
本地反向代理 + 多节点智能测速，为 GitHub 访问与 git clone 提速。

工作方式：
  gitpro 在本机 127.0.0.1 上监听一个代理端口，把收到的请求
  转发到当前延迟最低的公共加速节点。你可以：
    1) 把任意 GitHub 链接改写成本地加速链接直接下载
    2) 一键写入 git 的 url.insteadOf 配置，让 git clone 自动走加速
  所有流量、速度、请求都在界面上实时可见。

作者：为 宝宝 定制
"""

import base64
import concurrent.futures as futures
import ctypes
import json
import math
import os
import socket
import ssl
import subprocess
import sys
import tempfile
import threading
import time
import tkinter as tk
import tkinter.font as tkfont
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import unquote, urljoin, urlparse

import requests

VERSION = "2.0.0"
APP_TITLE = "gitpro"
DEFAULT_PORT = 8888
DEBUG = bool(os.environ.get("GITPRO_DEBUG"))
UA = "gitpro/%s (+local accelerator)" % VERSION

# ---------------------------------------------------------------- 节点池
# prefix 为空 = 直连官方，作为兜底
NODE_DEFS = [
    ("ghfast.top", "https://ghfast.top/"),
    ("ghproxy.net", "https://ghproxy.net/"),
    ("gh-proxy.com", "https://gh-proxy.com/"),
    ("ghproxy.imciel.com", "https://ghproxy.imciel.com/"),
    ("gh.llkk.cc", "https://gh.llkk.cc/"),
    ("github.moeyy.xyz", "https://github.moeyy.xyz/"),
    ("hub.gitmirror.com", "https://hub.gitmirror.com/"),
    ("直连官方", ""),
]
PROBE_URL = "https://raw.githubusercontent.com/octocat/Hello-World/master/README"
PROBE_DIRECT = "https://github.com/robots.txt"
PROBE_TOKEN = b"Hello"


class Node:
    __slots__ = ("name", "prefix", "latency", "ok", "checked")

    def __init__(self, name, prefix):
        self.name = name
        self.prefix = prefix
        self.latency = None      # ms
        self.ok = None           # True / False / None(未测)
        self.checked = 0.0

    @property
    def is_direct(self):
        return self.prefix == ""

    def build(self, target):
        return self.prefix + target


class NodePool:
    """并发测速 + 最优节点选择"""

    def __init__(self):
        self.nodes = [Node(n, p) for n, p in NODE_DEFS]
        self.lock = threading.Lock()
        self.locked_name = None
        self.scanning = False
        self.last_scan = 0.0

    # ---- 测速 -------------------------------------------------
    def _probe(self, node):
        if node.is_direct:
            url, token = PROBE_DIRECT, b"User-agent"
        else:
            url, token = node.build(PROBE_URL), PROBE_TOKEN
        t0 = time.perf_counter()
        try:
            with requests.get(url, timeout=7, stream=True,
                              headers={"User-Agent": UA,
                                       "Accept-Encoding": "identity"}) as r:
                ctype = r.headers.get("Content-Type", "")
                if r.status_code not in (200, 206) or "text/html" in ctype:
                    node.ok, node.latency = False, None
                    return
                buf = b""
                for chunk in r.iter_content(1024):
                    buf += chunk
                    if len(buf) >= 96:
                        break
            dt = (time.perf_counter() - t0) * 1000.0
            node.ok = token in buf
            node.latency = dt if node.ok else None
        except Exception:
            node.ok, node.latency = False, None
        finally:
            node.checked = time.time()

    def scan_async(self):
        if self.scanning:
            return
        self.scanning = True
        threading.Thread(target=self._scan, daemon=True).start()

    def _scan(self):
        try:
            with futures.ThreadPoolExecutor(max_workers=9) as ex:
                list(ex.map(self._probe, self.nodes))
            self.last_scan = time.time()
        finally:
            self.scanning = False

    # ---- 选择 -------------------------------------------------
    def best(self):
        with self.lock:
            if self.locked_name:
                for n in self.nodes:
                    if n.name == self.locked_name and n.ok:
                        return n
        pool = [n for n in self.nodes if n.ok]
        if pool:
            return min(pool, key=lambda n: n.latency or 9e9)
        return self.nodes[-1]          # 直连兜底

    def sorted_for_ui(self):
        def key(n):
            if n.ok:
                return (0, n.latency or 0)
            if n.ok is None:
                return (1, 0)
            return (2, 0)
        return sorted(self.nodes, key=key)

    @property
    def direct(self):
        return self.nodes[-1]


# 这些域名通常会解析到被干扰的 CDN，优先走加速节点
RAW_HOSTS = {
    "raw.githubusercontent.com",
    "gist.githubusercontent.com",
    "objects.githubusercontent.com",
    "avatars.githubusercontent.com",
    "user-images.githubusercontent.com",
    "private-user-images.githubusercontent.com",
    "media.githubusercontent.com",
    "github.githubassets.com",
    "camo.githubusercontent.com",
}


class Router:
    """按域名选择通道，失败自动回退并记住最优通道"""

    def __init__(self, pool):
        self.pool = pool
        self.lock = threading.Lock()
        self.pref = {}          # host -> 通道名
        self.stats = {}         # (host, 通道名) -> [成功, 失败]

    def order(self, host):
        with self.lock:
            pref = self.pref.get(host)
        pool = self.pool
        direct = pool.direct
        cands = [n for n in pool.nodes if n.ok and not n.is_direct]

        if pref:
            main = next((n for n in pool.nodes if n.name == pref), None)
        elif host in RAW_HOSTS:
            main = pool.best()
        else:
            main = direct

        if main is None:
            main = pool.best()

        seq = [main]
        if main is not direct:
            seq.extend(cands)
            seq.append(direct)
        else:
            seq.extend(cands)
        # 去重保序
        out, seen = [], set()
        for n in seq:
            if n and n.name not in seen:
                seen.add(n.name)
                out.append(n)
        return out[:4]

    def note(self, host, node, ok):
        with self.lock:
            key = (host, node.name)
            rec = self.stats.setdefault(key, [0, 0])
            rec[0 if ok else 1] += 1
            if ok:
                self.pref[host] = node.name
            elif self.pref.get(host) == node.name:
                self.pref.pop(host, None)


# ---------------------------------------------------------------- 统计
class Stats:
    def __init__(self):
        self.lock = threading.Lock()
        self.reqs = 0
        self.ok = 0
        self.fail = 0
        self.total_bytes = 0
        self.active = 0
        self.peak = 0
        self.total_ms = 0
        self.started = time.time()
        self._win = deque()
        self.recent = deque(maxlen=40)

    def begin(self):
        with self.lock:
            self.active += 1

    def end(self):
        with self.lock:
            self.active = max(0, self.active - 1)

    def add_bytes(self, n):
        if n <= 0:
            return
        now = time.monotonic()
        with self.lock:
            self.total_bytes += n
            self._win.append((now, n))
            while self._win and now - self._win[0][0] > 2.4:
                self._win.popleft()

    def push(self, entry):
        with self.lock:
            self.recent.appendleft(entry)

    def commit(self, entry, success):
        with self.lock:
            self.reqs += 1
            if success:
                self.ok += 1
            else:
                self.fail += 1
            self.total_ms += entry.get("ms", 0)

    def speed(self):
        now = time.monotonic()
        with self.lock:
            while self._win and now - self._win[0][0] > 2.4:
                self._win.popleft()
            total = sum(b for _, b in self._win)
        v = total / 2.4
        if v > self.peak:
            self.peak = v
        return v

    def snapshot(self):
        with self.lock:
            return dict(reqs=self.reqs, ok=self.ok, fail=self.fail,
                        total=self.total_bytes, active=self.active,
                        peak=self.peak, total_ms=self.total_ms,
                        recent=list(self.recent)[:3])


# ---------------------------------------------------------------- 代理
HOP_REQ = {
    "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
    "te", "trailer", "trailers", "transfer-encoding", "upgrade", "host",
    "accept-encoding",
}
SKIP_RESP = {
    "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
    "te", "trailer", "trailers", "transfer-encoding", "upgrade",
    "content-encoding", "date", "server",
}

_tls = threading.local()


def http_session():
    s = getattr(_tls, "s", None)
    if s is None:
        s = requests.Session()
        s.trust_env = False          # 忽略系统代理，避免绕圈
        _tls.s = s
    return s


def _hijacked(target, resp):
    """镜像返回 HTML 验证页却伪装成正常文件时返回 True"""
    host = (urlparse(target).hostname or "").lower()
    if host not in RAW_HOSTS and host not in ("codeload.github.com",):
        return False
    ctype = resp.headers.get("Content-Type", "").lower()
    if "text/html" not in ctype:
        return False
    path = urlparse(target).path.lower()
    return not path.endswith((".html", ".htm"))


HOME_PAGE = """<!doctype html>
<html lang="zh-CN"><head><meta charset="utf-8">
<title>gitpro · 加速中</title>
<style>
 html,body{height:100%%;margin:0}
 body{background:#07090D;color:#E9EFF7;display:flex;align-items:center;
      justify-content:center;font-family:"Microsoft YaHei UI",system-ui,sans-serif}
 .box{width:560px;padding:34px 38px;border-radius:18px;background:#0E1219;
      border:1px solid #1A212C}
 h1{margin:0 0 6px;font-size:22px;letter-spacing:.5px}
 .dot{display:inline-block;width:8px;height:8px;border-radius:50%%;
      background:#2DD4BF;margin-right:8px;vertical-align:middle}
 p{color:#69748A;font-size:13px;line-height:1.9;margin:10px 0}
 code{background:#131922;border:1px solid #1A212C;border-radius:8px;
      padding:10px 12px;display:block;color:#2DD4BF;font-size:12.5px;
      word-break:break-all;font-family:Consolas,monospace}
 b{color:#E9EFF7;font-weight:600}
</style></head><body><div class="box">
<h1><span class="dot"></span>gitpro 正在运行</h1>
<p>把任意 GitHub 链接前面加上本机地址，即可走加速通道。</p>
<code>http://127.0.0.1:%(port)d/https://github.com/owner/repo/archive/refs/heads/main.zip</code>
<p>在 gitpro 界面点击 <b>开启 Git 加速</b>，之后 <b>git clone</b> 会自动走加速。</p>
</div></body></html>"""


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "gitpro/" + VERSION
    sys_version = ""

    def log_message(self, fmt, *args):
        pass

    # ---- 入口 -------------------------------------------------
    def do_GET(self):
        self._proxy("GET")

    def do_HEAD(self):
        self._proxy("HEAD")

    def do_POST(self):
        self._proxy("POST")

    def do_PUT(self):
        self._proxy("PUT")

    def _target(self):
        p = self.path
        if p.startswith(("/http://", "/https://")):
            return p[1:]
        u = unquote(p)
        if u.startswith(("/http://", "/https://")):
            return u[1:]
        return None

    def _proxy(self, method):
        app = self.server.app
        target = self._target()

        if target is None:
            if self.path in ("/", "/index.html", ""):
                return self._send_html(HOME_PAGE % {"port": app.port})
            return self._send_text(404, "gitpro: 用法 http://127.0.0.1:%d/https://github.com/..."
                                  % app.port)

        stats = app.stats
        host = (urlparse(target).hostname or "").lower()
        cands = app.router.order(host)
        if method not in ("GET", "HEAD"):
            cands = cands[:1]           # 写操作不重试，避免重复提交

        stats.begin()
        t0 = time.perf_counter()
        got = 0
        started = False
        code = 0
        used = cands[0] if cands else app.pool.best()
        last_exc = None
        success = False
        entry = {
            "ts": time.strftime("%H:%M:%S"),
            "method": method, "code": 0, "size": 0, "ms": 0,
            "node": used.name, "chan": used.name, "url": target,
            "running": True,
        }
        stats.push(entry)

        try:
            headers = {k: v for k, v in self.headers.items()
                       if k.lower() not in HOP_REQ}
            headers.setdefault("User-Agent", UA)
            # 强制明文：GitHub 对 Range 请求返回的 gzip 流是残缺的，
            # 由 requests 解压会得到空 body。identity 也让长度信息准确。
            headers["Accept-Encoding"] = "identity"

            body = None
            clen = self.headers.get("Content-Length")
            if clen:
                try:
                    n = int(clen)
                    if n > 0:
                        body = self.rfile.read(n)
                except ValueError:
                    body = None

            for idx, node in enumerate(cands):
                try:
                    resp = self._fetch(method, target, node, headers, body)
                except requests.exceptions.RequestException as exc:
                    last_exc = exc
                    app.router.note(host, node, False)
                    if started:
                        break
                    continue

                if resp.status_code >= 500 and idx < len(cands) - 1:
                    app.router.note(host, node, False)
                    resp.close()
                    continue
                # 部分镜像会用验证页冒充 200，识别后换通道
                if idx < len(cands) - 1 and _hijacked(target, resp):
                    app.router.note(host, node, False)
                    resp.close()
                    continue

                used = node
                code = resp.status_code
                self.send_response(code)

                # 上游若压缩过，body 已被解压，长度信息作废
                resp_keys = {k.lower() for k in resp.headers}
                skip = SKIP_RESP if "content-encoding" not in resp_keys \
                    else SKIP_RESP | {"content-length"}
                for k, v in resp.headers.items():
                    if k.lower() in skip:
                        continue
                    self.send_header(k, v)
                self.send_header("Connection", "close")
                self.end_headers()
                started = True
                self.close_connection = True

                if method != "HEAD" and code not in (204, 304):
                    for chunk in resp.iter_content(65536):
                        if not chunk:
                            continue
                        self.wfile.write(chunk)
                        got += len(chunk)
                        entry["size"] = got
                        stats.add_bytes(len(chunk))
                resp.close()

                success = 200 <= code < 400
                app.router.note(host, node, success)
                break

            if not started:
                if isinstance(last_exc, requests.exceptions.Timeout):
                    self._send_text(504, "gitpro: 上游响应超时")
                    code = 504
                else:
                    self._send_text(502, "gitpro: 所有通道均不可用")
                    code = 502

        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
            success = False
        except Exception as exc:  # noqa: BLE001
            success = False
            if DEBUG:
                import traceback
                traceback.print_exc()
            if not started:
                self._send_text(502, "gitpro: 转发失败 — %s" % type(exc).__name__)
                code = 502
        finally:
            entry.update(code=code, size=got, running=False,
                         chan=used.name, node=used.name,
                         ms=int((time.perf_counter() - t0) * 1000))
            stats.commit(entry, success)
            stats.end()

    def _fetch(self, method, target, node, headers, body, hops=6):
        """手动跟随重定向，保证每一跳都经过同一个通道"""
        real = target
        resp = None
        for _ in range(hops):
            resp = http_session().request(
                method, node.build(real), headers=headers, data=body,
                stream=True, allow_redirects=False, timeout=(8, 150))
            if resp.status_code in (301, 302, 303, 307, 308):
                loc = resp.headers.get("Location")
                resp.close()
                if not loc:
                    return resp
                real = urljoin(real, loc)
                continue
            return resp
        return resp

    # ---- 小工具 -----------------------------------------------
    def _send_bytes(self, code, body, ctype="text/plain; charset=utf-8"):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Connection", "close")
        self.end_headers()
        self.close_connection = True
        try:
            self.wfile.write(body)
        except Exception:
            pass

    def _send_text(self, code, text):
        self._send_bytes(code, text.encode("utf-8"))

    def _send_html(self, html):
        self._send_bytes(200, html.encode("utf-8"), "text/html; charset=utf-8")


class ProxyServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, addr, app):
        self.app = app
        super().__init__(addr, Handler)


# ---------------------------------------------------------------- 核心
class Core:
    def __init__(self):
        self.pool = NodePool()
        self.router = Router(self.pool)
        self.dns = DnsAccel()
        self.stats = Stats()
        self.port = DEFAULT_PORT
        self.server = None
        self.thread = None
        self.running = False

    def free_port(self, want):
        for p in range(want, want + 40):
            with socket.socket() as s:
                s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                try:
                    s.bind(("127.0.0.1", p))
                    return p
                except OSError:
                    continue
        return want

    def start(self):
        if self.running:
            return True
        self.port = self.free_port(self.port)
        try:
            self.server = ProxyServer(("127.0.0.1", self.port), self)
        except Exception:
            return False
        self.thread = threading.Thread(target=self.server.serve_forever,
                                       kwargs={"poll_interval": 0.3}, daemon=True)
        self.thread.start()
        self.running = True
        return True

    def stop(self):
        if not self.running:
            return
        try:
            self.server.shutdown()
            self.server.server_close()
        except Exception:
            pass
        self.running = False
        self.server = None

    # ---- git 配置 ---------------------------------------------
    def _git(self, *args):
        try:
            r = subprocess.run(["git", *args], capture_output=True, text=True,
                               timeout=12, creationflags=0x08000000)
            return r.returncode == 0, (r.stdout or r.stderr).strip()
        except Exception as exc:  # noqa: BLE001
            return False, str(exc)

    def _pairs(self):
        base = "http://127.0.0.1:%d/" % self.port
        return [
            ("https://github.com/", base + "https://github.com/"),
            ("https://raw.githubusercontent.com/", base + "https://raw.githubusercontent.com/"),
            ("https://gist.githubusercontent.com/", base + "https://gist.githubusercontent.com/"),
            ("https://codeload.github.com/", base + "https://codeload.github.com/"),
            ("https://objects.githubusercontent.com/", base + "https://objects.githubusercontent.com/"),
        ]

    def git_enable(self):
        for old, new in self._pairs():
            self._git("config", "--global", "--replace-all",
                      'url."%s".insteadOf' % new, old)
        ok, _ = self._git("config", "--global", "--get-regexp",
                          r"url\..*insteadOf")
        return ok

    def git_disable(self):
        # 扫掉所有指向本机代理的 insteadOf，避免历史端口残留
        ok, out = self._git("config", "--global", "--get-regexp",
                            r"url\..*insteadOf")
        if ok:
            for line in out.splitlines():
                key = line.split(" ", 1)[0].strip()
                if "127.0.0.1" in key:
                    section = key.rsplit(".", 1)[0]
                    self._git("config", "--global", "--remove-section", section)
        ok2, out2 = self._git("config", "--global", "--get-regexp",
                              r"url\..*insteadOf")
        return not ("127.0.0.1" in out2)

    def git_status(self):
        ok, out = self._git("config", "--global", "--get-regexp",
                            r"url\..*insteadOf")
        return ("127.0.0.1" in out) if ok else False


# ---------------------------------------------------------------- 网页加速
# 思路：GitHub 每个域名解析出多个 IP，其中往往只有部分可连。
# 浏览器随机撞，撞到不通的就卡到超时，页面永远转圈。
# 这里逐个 IP 实测（TCP + TLS + HTTP），把最优的那个写进 hosts。

ACCEL_DOMAINS = [
    ("github.com", "主站"),
    ("api.github.com", "接口"),
    ("codeload.github.com", "下载"),
    ("raw.githubusercontent.com", "文件/图片"),
    ("avatars.githubusercontent.com", "头像"),
    ("objects.githubusercontent.com", "对象"),
    ("gist.githubusercontent.com", "Gist"),
    ("github.githubassets.com", "静态资源"),
    ("camo.githubusercontent.com", "外链图"),
]

FASTLY_IPS = ["185.199.108.133", "185.199.109.133",
              "185.199.110.133", "185.199.111.133"]

KNOWN_IPS = {
    "github.com": ["20.205.243.166", "20.27.177.113", "20.200.245.247",
                   "20.175.192.147", "20.29.134.23", "140.82.112.3",
                   "140.82.113.3", "140.82.114.3", "140.82.116.3",
                   "4.237.22.38"],
    "api.github.com": ["20.205.243.168", "20.27.177.116", "20.200.245.248",
                       "140.82.112.6", "140.82.113.6", "4.237.22.39"],
    "codeload.github.com": ["20.205.243.165", "20.27.177.114",
                            "20.200.245.246", "140.82.112.9", "140.82.113.9"],
    "github.githubassets.com": ["185.199.108.154", "185.199.109.154",
                                "185.199.110.154", "185.199.111.154"],
}

_SSL_CTX = None


def _ssl_ctx():
    global _SSL_CTX
    if _SSL_CTX is None:
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        _SSL_CTX = ctx
    return _SSL_CTX


def dns_candidates(domain):
    ips = set()
    try:
        for info in socket.getaddrinfo(domain, 443, socket.AF_INET):
            ips.add(info[4][0])
    except Exception:
        pass
    ips.update(KNOWN_IPS.get(domain, []))
    if domain.endswith("githubusercontent.com"):
        ips.update(FASTLY_IPS)
    return sorted(ips)


def probe_ip(domain, ip, timeout=5.0):
    """返回毫秒延迟；不可用返回 None"""
    t0 = time.time()
    try:
        raw = socket.create_connection((ip, 443), timeout=timeout)
        ss = _ssl_ctx().wrap_socket(raw, server_hostname=domain)
        req = ("HEAD / HTTP/1.1\r\nHost: %s\r\nUser-Agent: %s\r\n"
               "Connection: close\r\n\r\n" % (domain, UA)).encode()
        ss.sendall(req)
        buf = b""
        while b"\r\n\r\n" not in buf and len(buf) < 2048:
            chunk = ss.recv(512)
            if not chunk:
                break
            buf += chunk
        ss.close()
        if not buf or b"HTTP/" not in buf:
            return None
        return int((time.time() - t0) * 1000)
    except Exception:
        return None


class DnsAccel:
    def __init__(self):
        self.lock = threading.Lock()
        self.result = {}        # domain -> dict
        self.scanning = False
        self.last_scan = 0.0
        self.progress = 0

    def scan_async(self):
        if self.scanning:
            return False
        self.scanning = True
        self.progress = 0
        threading.Thread(target=self._scan, daemon=True).start()
        return True

    def _scan(self):
        try:
            total = len(ACCEL_DOMAINS)
            done = [0]
            lock = threading.Lock()

            def work(item):
                domain, _label = item
                ips = dns_candidates(domain)
                found = []
                with futures.ThreadPoolExecutor(12) as ex:
                    futs = {ex.submit(probe_ip, domain, ip): ip for ip in ips}
                    for f in futures.as_completed(futs):
                        ms = f.result()
                        if ms is not None:
                            found.append((futs[f], ms))
                found.sort(key=lambda x: x[1])
                with self.lock:
                    if found:
                        self.result[domain] = {
                            "ip": found[0][0], "ms": found[0][1],
                            "alts": [(i, m) for i, m in found[1:4]],
                            "ok": True, "cand": len(ips),
                        }
                    else:
                        self.result[domain] = {
                            "ip": None, "ms": None, "alts": [],
                            "ok": False, "cand": len(ips),
                        }
                with lock:
                    done[0] += 1
                    self.progress = int(100 * done[0] / total)

            with futures.ThreadPoolExecutor(total) as ex:
                list(ex.map(work, ACCEL_DOMAINS))
            self.last_scan = time.time()
        finally:
            self.scanning = False
            self.progress = 100

    def mapping(self):
        with self.lock:
            return {d: r["ip"] for d, r in self.result.items() if r.get("ip")}


HOSTS_PATH = os.path.join(os.environ.get("SystemRoot", r"C:\Windows"),
                          "System32", "drivers", "etc", "hosts")
MARK_START = "# >>> gitpro 加速开始（自动生成，删掉这段即还原）>>>"
MARK_END = "# <<< gitpro 加速结束 <<<"


def hosts_has_block(text):
    return MARK_START in text


def strip_block(text):
    if MARK_START not in text:
        return text
    out, skip = [], False
    for line in text.splitlines():
        if line.strip() == MARK_START:
            skip = True
            continue
        if line.strip() == MARK_END:
            skip = False
            continue
        if not skip:
            out.append(line)
    while out and not out[-1].strip():
        out.pop()
    return "\n".join(out) + "\n"


def build_block(mapping):
    lines = [MARK_START]
    for domain, _label in ACCEL_DOMAINS:
        ip = mapping.get(domain)
        if ip:
            lines.append("%-18s %s" % (ip, domain))
    lines.append(MARK_END)
    return "\n".join(lines)


def read_hosts():
    with open(HOSTS_PATH, "r", encoding="utf-8-sig", errors="replace") as f:
        return f.read()


def is_admin():
    try:
        return bool(ctypes.windll.shell32.IsUserAnAdmin())
    except Exception:
        return False


def apply_hosts_text(text):
    """真正落盘；调用者需具备管理员权限"""
    bak = HOSTS_PATH + ".gitpro.bak"
    if os.path.exists(HOSTS_PATH) and not os.path.exists(bak):
        try:
            with open(HOSTS_PATH, "rb") as src, open(bak, "wb") as dst:
                dst.write(src.read())
        except Exception:
            pass
    with open(HOSTS_PATH, "w", encoding="utf-8-sig", newline="\r\n") as f:
        f.write(text)


def flush_dns():
    try:
        subprocess.run(["ipconfig", "/flushdns"], capture_output=True,
                       timeout=15, creationflags=0x08000000)
    except Exception:
        pass


def hosts_state():
    """当前 hosts 是否处于 gitpro 加速状态；读不到返回 None"""
    try:
        return hosts_has_block(read_hosts())
    except Exception:
        return None


def elevate_apply_blob(blob):
    """以管理员身份重新拉起自己，参数走 base64，不依赖任何临时文件"""
    arg = "--apply-hosts-b64"
    if getattr(sys, "frozen", False):
        params = '%s %s' % (arg, blob)
    else:
        params = '"%s" %s %s' % (os.path.abspath(__file__), arg, blob)
    try:
        rc = ctypes.windll.shell32.ShellExecuteW(
            None, "runas", sys.executable, params, None, 1)
        return rc > 32
    except Exception:
        return False


def do_apply_hosts_data(data):
    """真正落盘；调用者需具备管理员权限"""
    cur = read_hosts()
    text = strip_block(cur).lstrip("\r\n")
    if data.get("mode") == "apply":
        # 必须放最前面：hosts 同一域名第一条生效，
        # 用户自己可能已经写过 github.com，放末尾会被它挡住。
        text = build_block(data.get("mapping", {})) + "\n\n" + text
    apply_hosts_text(text)
    flush_dns()


# ---------------------------------------------------------------- 主题
C = dict(
    bg="#07090D",
    card="#0E1219",
    card_hi="#141B25",
    line="#1A212C",
    text="#E9EFF7",
    sub="#8C99AD",
    dim="#69748A",
    dim2="#3A4453",
    accent="#2DD4BF",
    accent_soft="#0F3B37",
    blue="#3D7BFF",
    warn="#F5A524",
    bad="#F0506E",
)

F_LOGO = ("Microsoft YaHei UI", 16, "bold")
F_TITLE = ("Microsoft YaHei UI", 10, "bold")
F_BODY = ("Microsoft YaHei UI", 9)
F_SMALL = ("Microsoft YaHei UI", 8)
F_ROW = ("Microsoft YaHei UI", 9)
F_VAL = ("Microsoft YaHei UI", 12, "bold")
F_BIG = ("Segoe UI Light", 32)
F_UNIT = ("Microsoft YaHei UI", 9)
F_MONO = ("Consolas", 8)


def rr_points(x1, y1, x2, y2, r, steps=5):
    r = max(0.0, min(r, (x2 - x1) / 2.0, (y2 - y1) / 2.0))
    pts = []
    corners = ((x2 - r, y1 + r, -90, 0), (x2 - r, y2 - r, 0, 90),
               (x1 + r, y2 - r, 90, 180), (x1 + r, y1 + r, 180, 270))
    for cx, cy, a0, a1 in corners:
        for i in range(steps + 1):
            a = math.radians(a0 + (a1 - a0) * i / steps)
            pts.extend((cx + r * math.cos(a), cy + r * math.sin(a)))
    return pts


def rr(cv, x1, y1, x2, y2, r, **kw):
    return cv.create_polygon(rr_points(x1, y1, x2, y2, r), **kw)


def human(n):
    n = float(n)
    if n < 1024:
        return "%d B" % n
    for unit in ("KB", "MB", "GB", "TB"):
        n /= 1024.0
        if n < 1024:
            return "%.1f %s" % (n, unit)
    return "%.1f PB" % n


def speed_pair(v):
    if v >= 1048576:
        return "%.1f" % (v / 1048576.0), "MB/s"
    if v >= 1024:
        return "%.0f" % (v / 1024.0), "KB/s"
    return "%.0f" % v, "B/s"


def short_target(u, limit=52):
    try:
        p = urlparse(u)
        parts = [x for x in p.path.split("/") if x]
        tail = "/".join(parts[-2:]) if parts else ""
        s = (p.netloc + "/" + tail).replace("https://", "").replace("http://", "")
    except Exception:
        s = u
    return s if len(s) <= limit else s[:limit - 1] + "…"


# ---------------------------------------------------------------- 控件
class Card(tk.Frame):
    def __init__(self, master, radius=14, pad=17, fill=None):
        super().__init__(master, bg=C["bg"], bd=0, highlightthickness=0)
        self.radius = radius
        self._fill = fill or C["card"]
        self._cv = tk.Canvas(self, bg=C["bg"], highlightthickness=0, bd=0,
                             width=1, height=1)
        self._cv.pack(fill="both", expand=True)
        self.body = tk.Frame(self, bg=self._fill, bd=0, highlightthickness=0)
        self.body.place(x=pad, y=pad, relwidth=1, relheight=1,
                        width=-2 * pad, height=-2 * pad)
        self._cv.bind("<Configure>", self._redraw)

    def _redraw(self, _e=None):
        w = self._cv.winfo_width()
        h = self._cv.winfo_height()
        if w < 4 or h < 4:
            return
        self._cv.delete("bgshape")
        rr(self._cv, 0.5, 0.5, w - 0.5, h - 0.5, self.radius,
           fill=self._fill, outline=C["line"], width=1, tags="bgshape")
        self._cv.tag_lower("bgshape")


class RoundButton(tk.Canvas):
    def __init__(self, master, text, command, width=96, height=32,
                 fill=C["accent"], fg="#04211E", bg=None, radius=9,
                 font=None, outline=""):
        bg = bg or C["card"]
        super().__init__(master, width=width, height=height, bg=bg,
                         highlightthickness=0, bd=0, cursor="hand2")
        self._text = text
        self._cmd = command
        self._bw, self._bh = width, height
        self._fill, self._fg = fill, fg
        self._radius = radius
        self._font = font or F_BODY
        self._outline = outline
        self._hover = mix(fill, "#FFFFFF", 0.12) if outline == "" else C["card_hi"]
        self._draw(self._fill)
        self.bind("<Button-1>", self._click)
        self.bind("<Enter>", lambda e: self._draw(self._hover))
        self.bind("<Leave>", lambda e: self._draw(self._fill))

    def _draw(self, color):
        self.delete("all")
        rr(self, 0, 0, self._bw, self._bh, self._radius,
           fill=color, outline=self._outline or color,
           width=1 if self._outline else 0)
        self.create_text(self._bw / 2, self._bh / 2 + 0.5, text=self._text,
                         fill=self._fg, font=self._font)

    def _click(self, _e):
        try:
            self._cmd()
        except Exception:
            pass

    def configure_text(self, text):
        self._text = text
        self._draw(self._fill)


def mix(c1, c2, t):
    a = tuple(int(c1[i:i + 2], 16) for i in (1, 3, 5))
    b = tuple(int(c2[i:i + 2], 16) for i in (1, 3, 5))
    return "#%02X%02X%02X" % tuple(int(round(a[i] + (b[i] - a[i]) * t)) for i in range(3))


class TabBtn(tk.Canvas):
    def __init__(self, master, text, command, width=96, height=30):
        super().__init__(master, width=width, height=height, bg=C["bg"],
                         highlightthickness=0, bd=0, cursor="hand2")
        self._text = text
        self._cmd = command
        self._bw, self._bh = width, height
        self.active = False
        self._draw()
        self.bind("<Button-1>", lambda e: self._cmd())
        self.bind("<Enter>", lambda e: self._draw(hover=True))
        self.bind("<Leave>", lambda e: self._draw())

    def set_active(self, value):
        self.active = value
        self._draw()

    def _draw(self, hover=False):
        self.delete("all")
        if self.active:
            rr(self, 0, 0, self._bw, self._bh, 9,
               fill=C["accent_soft"], outline=mix(C["accent"], C["accent_soft"], 0.45),
               width=1)
            fg = C["accent"]
        elif hover:
            rr(self, 0, 0, self._bw, self._bh, 9,
               fill=C["card_hi"], outline=C["line"], width=1)
            fg = C["sub"]
        else:
            rr(self, 0, 0, self._bw, self._bh, 9,
               fill=C["card"], outline=C["line"], width=1)
            fg = C["dim"]
        self.create_text(self._bw / 2, self._bh / 2 + 0.5, text=self._text,
                         fill=fg, font=F_SMALL)


# ---------------------------------------------------------------- 界面
class UI:
    TICK = 250
    HISTORY = 96

    def __init__(self, core):
        self.core = core
        self.root = tk.Tk()
        self.root.title(APP_TITLE)
        self.root.configure(bg=C["bg"])
        self._set_icon()
        self._center(972, 680)
        self.root.minsize(900, 620)
        dark_titlebar(self.root)

        self.history = deque([0.0] * self.HISTORY, maxlen=self.HISTORY)
        self._fm = tkfont.Font(family="Consolas", size=8)
        self._f_row = tkfont.Font(family="Microsoft YaHei UI", size=9)
        self._f_sm = tkfont.Font(family="Microsoft YaHei UI", size=8)
        self._t = 0
        self._peak = 0.0
        self._toast_job = None
        self._git_on = core.git_status()
        self._hosts_on = False
        self._tab = "web"

        self._build()
        self.root.protocol("WM_DELETE_WINDOW", self._close)
        self.root.after(200, self._boot)

    # ---- 基础 -------------------------------------------------
    def _set_icon(self):
        ico = resource_path("icon.ico")
        try:
            self.root.iconbitmap(ico)
        except Exception:
            pass

    def _center(self, w, h):
        sw = self.root.winfo_screenwidth()
        sh = self.root.winfo_screenheight()
        w = min(w, sw - 60)
        h = min(h, sh - 90)
        x = max(0, (sw - w) // 2)
        y = max(0, (sh - h) // 2 - 20)
        self.root.geometry("%dx%d+%d+%d" % (w, h, x, y))

    def _build(self):
        main = tk.Frame(self.root, bg=C["bg"])
        main.pack(fill="both", expand=True, padx=16, pady=(12, 14))

        self._build_topbar(main)
        self._build_tabs(main)

        wrap = tk.Frame(main, bg=C["bg"])
        wrap.pack(fill="both", expand=True, pady=(10, 0))

        self.page_web = tk.Frame(wrap, bg=C["bg"])
        self.page_dl = tk.Frame(wrap, bg=C["bg"])
        self._build_web(self.page_web)
        self._build_download(self.page_dl)
        self._switch("web")

    # ---- 页签 -------------------------------------------------
    def _build_tabs(self, parent):
        bar = tk.Frame(parent, bg=C["bg"], height=30)
        bar.pack(fill="x", pady=(10, 0))
        bar.pack_propagate(False)
        self.tab_web = TabBtn(bar, "网页加速", lambda: self._switch("web"), 92)
        self.tab_web.pack(side="left")
        self.tab_dl = TabBtn(bar, "下载 / Git", lambda: self._switch("dl"), 96)
        self.tab_dl.pack(side="left", padx=(8, 0))

    def _switch(self, name):
        self._tab = name
        for page in (self.page_web, self.page_dl):
            page.pack_forget()
        page = self.page_web if name == "web" else self.page_dl
        page.pack(fill="both", expand=True)
        self.tab_web.set_active(name == "web")
        self.tab_dl.set_active(name == "dl")
        if name == "web":
            self._draw_domains()

    def _build_download(self, parent):
        body = tk.Frame(parent, bg=C["bg"])
        body.pack(fill="both", expand=True)
        body.columnconfigure(0, weight=5, uniform="c")
        body.columnconfigure(1, weight=4, uniform="c")
        body.rowconfigure(0, weight=0, minsize=200)
        body.rowconfigure(1, weight=1, minsize=180)
        body.rowconfigure(2, weight=0, minsize=96)

        self._build_speed(body)
        self._build_metrics(body)
        self._build_nodes(body)
        self._build_tools(body)
        self._build_log(body)

    # ---- 网页加速页 -------------------------------------------
    def _build_web(self, parent):
        card = Card(parent)
        card.pack(fill="both", expand=True)
        b = card.body

        head = tk.Frame(b, bg=C["card"])
        head.pack(fill="x")
        tk.Label(head, text="加速域名", bg=C["card"], fg=C["dim"],
                 font=F_SMALL).pack(side="left")
        self.lbl_dns = tk.Label(head, text="准备检测…", bg=C["card"],
                                fg=C["dim2"], font=F_SMALL)
        self.lbl_dns.pack(side="right")

        self.dom_cv = tk.Canvas(b, bg=C["card"], highlightthickness=0, bd=0)
        self.dom_cv.pack(fill="both", expand=True, pady=(12, 0))
        self.dom_cv.bind("<Configure>", lambda e: self._draw_domains())

        act = tk.Frame(b, bg=C["card"])
        act.pack(fill="x", pady=(10, 0))
        self.btn_accel = RoundButton(act, "一键加速", self._apply_accel,
                                     width=142, height=31)
        self.btn_accel.pack(side="left")
        RoundButton(act, "还原 hosts", self._revert_accel, width=96,
                    height=31, fill=C["card"], fg=C["sub"], outline=C["line"],
                    font=F_SMALL).pack(side="left", padx=(8, 0))
        RoundButton(act, "重新检测", self._rescan_dns, width=88, height=31,
                    fill=C["card"], fg=C["sub"], outline=C["line"],
                    font=F_SMALL).pack(side="left", padx=(8, 0))
        self.lbl_accel = tk.Label(act, text="", bg=C["card"], fg=C["accent"],
                                  font=F_SMALL)
        self.lbl_accel.pack(side="right")

        tip = tk.Frame(b, bg=C["card"])
        tip.pack(fill="x", pady=(12, 0))
        self.lbl_tip = tk.Label(
            tip,
            text="原理：GitHub 每个域名会解析出好几个 IP，其中一部分根本连不通。"
                 "浏览器撞上坏 IP 就会一直转圈等超时——这就是主页能开、图片却一直加载的原因。"
                 "这里把每个 IP 挨个实测，只把真正能连的写进 hosts，浏览器内核立即生效，"
                 "不用装证书、也不用改代理。",
            bg=C["card"], fg=C["dim2"], font=F_SMALL, wraplength=840,
            justify="left")
        self.lbl_tip.pack(anchor="w")
        tk.Label(tip,
                 text="应用时需管理员权限（弹一次 UAC）；原 hosts 自动备份为 hosts.gitpro.bak，"
                      "随时可一键还原。",
                 bg=C["card"], fg=C["dim2"], font=F_SMALL, wraplength=840,
                 justify="left").pack(anchor="w", pady=(4, 0))

    def _draw_domains(self):
        cv = getattr(self, "dom_cv", None)
        if cv is None:
            return
        cv.delete("all")
        w, h = cv.winfo_width(), cv.winfo_height()
        if w < 120 or h < 40:
            return

        res = self.core.dns.result
        n = len(ACCEL_DOMAINS)
        rh = max(20, min(38, (h - 26) // n))
        fm_row, fm_sm, fm_mono = self._f_row, self._f_sm, self._fm

        x_ip = w - 330
        x_ms = w - 156
        x_cnt = w - 8

        cv.create_text(36, 8, text="域名", anchor="w",
                       fill=C["dim2"], font=F_SMALL)
        cv.create_text(x_ip, 8, text="优选 IP", anchor="w",
                       fill=C["dim2"], font=F_SMALL)
        cv.create_text(x_ms, 8, text="延迟", anchor="w",
                       fill=C["dim2"], font=F_SMALL)
        cv.create_text(x_cnt, 8, text="可用", anchor="e",
                       fill=C["dim2"], font=F_SMALL)

        c_dom = fm_row.measure("objects.githubusercontent.com") + 14

        for i, (domain, label) in enumerate(ACCEL_DOMAINS):
            y = 22 + i * rh
            cy = y + rh / 2 - 3
            r = res.get(domain)
            if r is None:
                color, ip_txt, ms_txt, ms_fg = C["dim2"], "待测", "—", C["dim2"]
                cnt_txt = ""
            elif r.get("ok"):
                lat = r["ms"]
                color = C["accent"] if lat < 900 else (
                    C["warn"] if lat < 1800 else "#E0567A")
                ip_txt = r["ip"]
                ms_txt = "%dms" % lat
                ms_fg = color
                cnt_txt = "1/%d" % r.get("cand", 0)
            else:
                color, ip_txt = C["bad"], "无可用 IP"
                ms_txt, ms_fg = "失败", C["bad"]
                cnt_txt = "0/%d" % r.get("cand", 0)

            cv.create_oval(10, cy - 3.5, 17, cy + 3.5, fill=color, outline="")
            cv.create_text(30, cy, text=domain, anchor="w",
                           fill=C["text"] if r and r.get("ok") else C["dim"],
                           font=F_ROW)
            cv.create_text(30 + c_dom, cy, text=label, anchor="w",
                           fill=C["dim2"], font=F_SMALL)
            cv.create_text(x_ip, cy, text=ip_txt, anchor="w",
                           fill=C["sub"], font=F_MONO)
            cv.create_text(x_ms, cy, text=ms_txt, anchor="w",
                           fill=ms_fg, font=F_MONO)
            if cnt_txt:
                cv.create_text(x_cnt, cy, text=cnt_txt, anchor="e",
                               fill=C["dim2"], font=F_MONO)

    def _rescan_dns(self):
        if self.core.dns.scanning:
            self._toast2("正在检测中…")
            return
        self.core.dns.scan_async()
        self._toast2("开始检测…")

    def _apply_accel(self):
        mapping = self.core.dns.mapping()
        if not mapping:
            self._toast2("还没有结果，先点「重新检测」")
            return
        self._hosts_op("apply", mapping)

    def _revert_accel(self):
        self._hosts_op("revert", {})

    def _hosts_op(self, mode, mapping):
        blob = base64.b64encode(
            json.dumps({"mode": mode, "mapping": mapping},
                       ensure_ascii=False).encode("utf-8")).decode("ascii")
        before = hosts_state()
        if before is None:
            self._toast2("读不到 hosts 文件")
            return

        if is_admin():
            self._toast2("正在写入 hosts…")
            try:
                do_apply_hosts_data({"mode": mode, "mapping": mapping})
            except Exception as exc:  # noqa: BLE001
                self._toast2("写入失败：%s" % str(exc)[:50])
                return
            self._wait_hosts_state(mode, before, 0)
            return

        self._toast2("等待管理员确认…")
        if not elevate_apply_blob(blob):
            self._toast2("已取消")
            return
        self.root.after(400, lambda: self._wait_hosts_state(mode, before, 0))

    def _wait_hosts_state(self, mode, before, tries):
        """轮询 hosts 是否真的变了，不依赖任何回传文件"""
        now = hosts_state()
        if now is not None and now != before:
            self._toast2("已生效，刷新浏览器试试" if mode == "apply"
                         else "hosts 已还原")
            self._refresh_hosts_state()
            return
        if tries < 60:
            self.root.after(250,
                            lambda: self._wait_hosts_state(mode, before, tries + 1))
        else:
            self._toast2("没检测到变化，可能取消了 UAC")

    def _refresh_hosts_state(self):
        try:
            on = hosts_has_block(read_hosts())
        except Exception:
            on = False
        self._hosts_on = on
        self.btn_accel._fill = C["accent_soft"] if on else C["accent"]
        self.btn_accel._fg = C["accent"] if on else "#04211E"
        self.btn_accel.configure_text("已加速 · 重新应用" if on else "一键加速")

    def _toast2(self, msg):
        if not hasattr(self, "lbl_accel"):
            return
        self.lbl_accel.configure(text=msg)
        job = getattr(self, "_toast2_job", None)
        if job:
            try:
                self.root.after_cancel(job)
            except Exception:
                pass
        self._toast2_job = self.root.after(
            4500, lambda: self.lbl_accel.configure(text=""))

    # ---- 顶栏 -------------------------------------------------
    def _build_topbar(self, parent):
        bar = tk.Frame(parent, bg=C["bg"], height=52)
        bar.pack(fill="x")
        bar.pack_propagate(False)

        logo = tk.Canvas(bar, width=34, height=34, bg=C["bg"],
                         highlightthickness=0, bd=0)
        logo.pack(side="left", pady=4)
        rr(logo, 1, 1, 33, 33, 10, fill=C["accent"], outline="")
        logo.create_polygon([19, 7, 11, 19, 16.5, 19, 14, 27, 23, 15, 17.5, 15],
                            fill="#04211E", outline="")

        box = tk.Frame(bar, bg=C["bg"])
        box.pack(side="left", padx=(11, 0), pady=6)
        tk.Label(box, text="gitpro", bg=C["bg"], fg=C["text"],
                 font=F_LOGO).pack(anchor="w")
        tk.Label(box, text="GitHub 加速器", bg=C["bg"], fg=C["dim"],
                 font=F_SMALL).pack(anchor="w")

        self.btn_power = RoundButton(bar, "启动服务", self._toggle,
                                     width=104, height=34, fill=C["accent"])
        self.btn_power.pack(side="right", pady=9)

        self.pill = tk.Canvas(bar, width=210, height=30, bg=C["bg"],
                              highlightthickness=0, bd=0)
        self.pill.pack(side="right", padx=(0, 12), pady=9)
        self._draw_pill()

    def _draw_pill(self):
        cv = self.pill
        cv.delete("all")
        running = self.core.running
        color = C["accent"] if running else C["dim2"]
        txt = ("运行中 · 127.0.0.1:%d" % self.core.port) if running else "已停止"
        rr(cv, 0, 0, 210, 30, 15, fill=C["card"], outline=C["line"], width=1)
        cv.create_oval(13, 12, 21, 20, fill=color, outline="")
        if running:
            cv.create_oval(13, 12, 21, 20, outline=mix(color, C["card"], 0.55),
                           width=3)
        cv.create_text(30, 15.5, text=txt, anchor="w",
                       fill=C["text"] if running else C["dim"], font=F_BODY)

    # ---- 速度卡 -----------------------------------------------
    def _build_speed(self, parent):
        card = Card(parent)
        card.grid(row=0, column=0, sticky="nsew", padx=(0, 6), pady=(0, 6))
        b = card.body

        head = tk.Frame(b, bg=C["card"])
        head.pack(side="top", fill="x")
        tk.Label(head, text="实时速度", bg=C["card"], fg=C["dim"],
                 font=F_SMALL).pack(side="left")
        self.lbl_node = tk.Label(head, text="节点 —", bg=C["card"],
                                 fg=C["accent"], font=F_SMALL)
        self.lbl_node.pack(side="right")

        val = tk.Frame(b, bg=C["card"])
        val.pack(side="top", fill="x", pady=(2, 0))
        self.lbl_num = tk.Label(val, text="0", bg=C["card"], fg=C["text"],
                                font=F_BIG)
        self.lbl_num.pack(side="left")
        self.lbl_unit = tk.Label(val, text="B/s", bg=C["card"], fg=C["dim"],
                                 font=F_UNIT)
        self.lbl_unit.pack(side="left", padx=(7, 0), pady=(0, 9))
        self.lbl_peak = tk.Label(val, text="峰值 0 B/s", bg=C["card"],
                                 fg=C["dim2"], font=F_SMALL)
        self.lbl_peak.pack(side="right", pady=(0, 10))

        self.chart = tk.Canvas(b, bg=C["card"], highlightthickness=0, bd=0,
                               height=76)
        self.chart.pack(side="bottom", fill="x", pady=(4, 0))
        self.chart.bind("<Configure>", lambda e: self._draw_chart())

    def _draw_chart(self):
        cv = self.chart
        cv.delete("all")
        w, h = cv.winfo_width(), cv.winfo_height()
        if DEBUG:
            print("[chart] size=%dx%d max=%.0f" % (w, h, max(self.history) if self.history else -1),
                  flush=True)
        if w < 20 or h < 12:
            return
        for i in (0.33, 0.66):
            y = h * i
            cv.create_line(0, y, w, y, fill=C["line"], dash=(2, 5))

        data = list(self.history)
        n = len(data)
        pad = 4.0
        if max(data) < 512:
            cv.create_line(pad, h - 3, w - pad, h - 3, fill=C["line"],
                           width=2, capstyle="round")
            return
        mx = max(max(data), self._peak_scale(), 64.0)
        pts = []
        for i, v in enumerate(data):
            x = pad + (w - pad * 2) * i / (n - 1)
            y = h - 3 - (h - 8) * (v / mx)
            pts.append((x, y))
        if len(pts) < 2:
            return
        poly = [pts[0][0], h] + [c for p in pts for c in p] + [pts[-1][0], h]
        cv.create_polygon(poly, fill=C["accent_soft"], outline="")
        cv.create_line([c for p in pts for c in p], fill=C["accent"],
                       width=2, smooth=True, splinesteps=6)
        ex, ey = pts[-1]
        cv.create_oval(ex - 3.5, ey - 3.5, ex + 3.5, ey + 3.5,
                       fill=C["accent"], outline=C["card"], width=1.5)

    def _peak_scale(self):
        return max(self.history) * 1.15 if any(self.history) else 64.0

    # ---- 指标卡 -----------------------------------------------
    def _build_metrics(self, parent):
        card = Card(parent)
        card.grid(row=0, column=1, sticky="nsew", padx=(6, 0), pady=(0, 6))
        b = card.body

        tk.Label(b, text="运行统计", bg=C["card"], fg=C["dim"],
                 font=F_SMALL).pack(anchor="w")

        grid = tk.Frame(b, bg=C["card"])
        grid.pack(fill="both", expand=True, pady=(6, 0))
        for i in range(2):
            grid.columnconfigure(i, weight=1, uniform="m")
            grid.rowconfigure(i, weight=1, uniform="m")

        self.m_val = {}
        cells = [("total", "下载总量"), ("reqs", "请求数"),
                 ("rate", "成功率"), ("active", "活跃连接")]
        for idx, (key, label) in enumerate(cells):
            cell = tk.Frame(grid, bg=C["card"])
            cell.grid(row=idx // 2, column=idx % 2, sticky="nsew",
                      padx=(0, 8) if idx % 2 == 0 else (0, 0))
            v = tk.Label(cell, text="—", bg=C["card"], fg=C["text"],
                         font=F_VAL, anchor="w")
            v.pack(anchor="w")
            tk.Label(cell, text=label, bg=C["card"], fg=C["dim"],
                     font=F_SMALL, anchor="w").pack(anchor="w")
            self.m_val[key] = v

    # ---- 节点卡 -----------------------------------------------
    def _build_nodes(self, parent):
        card = Card(parent)
        card.grid(row=1, column=0, sticky="nsew", padx=(0, 6), pady=(6, 6))
        b = card.body

        head = tk.Frame(b, bg=C["card"])
        head.pack(fill="x")
        tk.Label(head, text="加速节点", bg=C["card"], fg=C["dim"],
                 font=F_SMALL).pack(side="left")
        self.lbl_pick = tk.Label(head, text="自动选择", bg=C["card"],
                                 fg=C["dim2"], font=F_SMALL)
        self.lbl_pick.pack(side="left", padx=(8, 0))
        RoundButton(head, "重新测速", self._rescan, width=68, height=22,
                    fill=C["card"], fg=C["sub"], radius=7, font=F_SMALL,
                    outline=C["line"]).pack(side="right")

        self.node_cv = tk.Canvas(b, bg=C["card"], highlightthickness=0, bd=0)
        self.node_cv.pack(fill="both", expand=True, pady=(6, 0))
        self.node_cv.bind("<Button-1>", self._node_click)
        self.node_cv.bind("<Configure>", lambda e: self._draw_nodes())

    def _node_click(self, event):
        rows = self._node_rows
        if not rows:
            return
        idx = int(event.y // rows[0][2]) if rows[0][2] else -1
        if 0 <= idx < len(rows):
            node = rows[idx][1]
            if self.core.pool.locked_name == node.name:
                self.core.pool.locked_name = None
            elif node.ok:
                self.core.pool.locked_name = node.name
            self._draw_nodes()
            self._update_node_label()

    def _update_node_label(self):
        lock = self.core.pool.locked_name
        if lock:
            self.lbl_pick.configure(text="已锁定 · " + lock, fg=C["accent"])
        else:
            self.lbl_pick.configure(text="自动选择", fg=C["dim2"])

    def _draw_nodes(self):
        cv = self.node_cv
        cv.delete("all")
        w, h = cv.winfo_width(), cv.winfo_height()
        if w < 40 or h < 20:
            return
        nodes = self.core.pool.sorted_for_ui()[:8]
        rh = max(20, min(30, (h - 2) // max(1, len(nodes))))
        self._node_rows = []
        active = self.core.pool.best()

        for i, n in enumerate(nodes):
            y = i * rh
            self._node_rows.append((i, n, rh))
            selected = (n is active)
            if selected:
                rr(cv, 2, y + 1, w - 2, y + rh - 2, 8,
                   fill=C["card_hi"], outline=C["line"], width=1)

            if n.ok:
                lat = n.latency or 0
                color = C["accent"] if lat < 900 else (
                    C["warn"] if lat < 1800 else "#E0567A")
            elif n.is_direct:
                color = C["dim"]
            elif n.ok is None:
                color = C["dim2"]
            else:
                color = C["bad"]
            cy = y + rh / 2.0
            cv.create_oval(10, cy - 3.5, 17, cy + 3.5, fill=color, outline="")

            name_fg = C["text"] if n.ok else C["dim2"]
            cv.create_text(25, cy, text=n.name, anchor="w", fill=name_fg,
                           font=F_ROW)

            # 延迟条
            bx2 = w - 46
            bx1 = max(150, bx2 - 78)
            cv.create_line(bx1, cy, bx2, cy, fill=C["line"], width=4,
                           capstyle="round")
            if n.ok and n.latency:
                ratio = max(0.12, min(1.0, 1.0 - (n.latency - 120) / 2400.0))
                cv.create_line(bx1, cy, bx1 + (bx2 - bx1) * ratio, cy,
                               fill=color, width=4, capstyle="round")
                ms = "%dms" % n.latency
                ms_fg = color
            elif n.is_direct:
                ms, ms_fg = "备用", C["dim"]
            elif n.ok is None:
                ms, ms_fg = "待测", C["dim2"]
            else:
                ms, ms_fg = "不可用", C["bad"]
            cv.create_text(w - 6, cy, text=ms, anchor="e", fill=ms_fg,
                           font=F_MONO)

    # ---- 工具卡 -----------------------------------------------
    def _build_tools(self, parent):
        card = Card(parent)
        card.grid(row=1, column=1, sticky="nsew", padx=(6, 0), pady=(6, 6))
        b = card.body

        tk.Label(b, text="Git 一键加速", bg=C["card"], fg=C["dim"],
                 font=F_SMALL).pack(anchor="w")

        row = tk.Frame(b, bg=C["card"])
        row.pack(fill="x", pady=(6, 0))
        self.btn_git = RoundButton(
            row, "开启 Git 加速" if not self._git_on else "关闭 Git 加速",
            self._toggle_git, width=112, height=28,
            fill=C["accent_soft"] if self._git_on else C["card"],
            fg=C["accent"] if self._git_on else C["sub"],
            radius=8, font=F_SMALL, outline=C["line"])
        self.btn_git.pack(side="left")

        row2 = tk.Frame(b, bg=C["card"])
        row2.pack(fill="x", pady=(8, 0))
        tk.Label(row2, text="链接", bg=C["card"], fg=C["dim2"],
                 font=F_SMALL).pack(side="left", padx=(0, 6))
        self.entry = tk.Entry(row2, bg=C["card_hi"], fg=C["text"],
                              insertbackground=C["accent"], relief="flat",
                              bd=0, font=F_SMALL, highlightthickness=1,
                              highlightbackground=C["line"],
                              highlightcolor=C["line"])
        self.entry.pack(side="left", fill="x", expand=True, ipady=4)
        self._placeholder = "粘贴 GitHub 链接…"
        self._ph_on = False
        self._show_placeholder()
        self.entry.bind("<FocusIn>", self._clear_placeholder)
        self.entry.bind("<FocusOut>", self._show_placeholder)
        self.entry.bind("<Return>", lambda e: self._convert())

        row3 = tk.Frame(b, bg=C["card"])
        row3.pack(fill="x", pady=(7, 0))
        RoundButton(row3, "转换", self._convert, width=56, height=25,
                    fill=C["card"], fg=C["sub"], radius=7, font=F_SMALL,
                    outline=C["line"]).pack(side="left")
        RoundButton(row3, "复制", self._copy, width=56, height=25,
                    fill=C["card"], fg=C["sub"], radius=7, font=F_SMALL,
                    outline=C["line"]).pack(side="left", padx=(7, 0))
        self.lbl_msg = tk.Label(row3, text="", bg=C["card"], fg=C["accent"],
                                font=F_SMALL)
        self.lbl_msg.pack(side="right")

        tk.Label(b, text="开启后 git clone 自动走加速通道，不用改仓库地址；"
                         "粘贴链接可转成加速下载地址。",
                 bg=C["card"], fg=C["dim2"], font=F_SMALL,
                 wraplength=330, justify="left").pack(anchor="w", pady=(10, 0))

    # ---- 日志卡 -----------------------------------------------
    def _build_log(self, parent):
        card = Card(parent, pad=14)
        card.grid(row=2, column=0, columnspan=2, sticky="nsew",
                  padx=0, pady=(6, 0))
        b = card.body

        head = tk.Frame(b, bg=C["card"])
        head.pack(fill="x")
        tk.Label(head, text="实时请求", bg=C["card"], fg=C["dim"],
                 font=F_SMALL).pack(side="left")
        self.lbl_cnt = tk.Label(head, text="等待流量", bg=C["card"],
                                fg=C["dim2"], font=F_SMALL)
        self.lbl_cnt.pack(side="right")

        self.log_cv = tk.Canvas(b, bg=C["card"], highlightthickness=0, bd=0,
                                height=44)
        self.log_cv.pack(fill="both", expand=True, pady=(4, 0))

    # ---- 交互 -------------------------------------------------
    def _boot(self):
        self.core.start()
        self.core.pool.scan_async()
        self.core.dns.scan_async()
        self._draw_pill()
        self.btn_power.configure_text("停止服务")
        self._refresh_hosts_state()
        self._toast2("正在检测各域名可用 IP…")
        self.root.after(self.TICK, self._tick)

    def _toggle(self):
        if self.core.running:
            self.core.stop()
            self.btn_power.configure_text("启动服务")
            self._toast("服务已停止")
        else:
            if not self.core.start():
                self._toast("端口被占用")
                return
            self._git_on = self.core.git_status()
            self._sync_git_btn()
            self.btn_power.configure_text("停止服务")
            self._toast("服务已启动 · 端口 %d" % self.core.port)
        self._draw_pill()

    def _rescan(self):
        if self.core.pool.scanning:
            self._toast("正在测速…")
            return
        self.core.pool.scan_async()
        self._toast("正在测速…")

    def _toggle_git(self):
        if not self.core.running:
            self._toast("请先启动服务")
            return
        if self._git_on:
            ok = self.core.git_disable()
            self._git_on = not ok
            self._toast("已关闭 Git 加速" if ok else "关闭失败")
        else:
            ok = self.core.git_enable()
            self._git_on = ok
            self._toast("Git 加速已开启，clone 会自动走本地代理" if ok else "写入失败")
        self._sync_git_btn()

    def _sync_git_btn(self):
        on = self._git_on
        self.btn_git._fill = C["accent_soft"] if on else C["card"]
        self.btn_git._fg = C["accent"] if on else C["sub"]
        self.btn_git.configure_text("关闭 Git 加速" if on else "开启 Git 加速")
        self.btn_git._draw(self.btn_git._fill)

    def _show_placeholder(self, _e=None):
        if not self.entry.get():
            self.entry.insert(0, self._placeholder)
            self.entry.configure(fg=C["dim2"])
            self._ph_on = True

    def _clear_placeholder(self, _e=None):
        if getattr(self, "_ph_on", False):
            self.entry.delete(0, "end")
            self.entry.configure(fg=C["text"])
            self._ph_on = False

    def _entry_value(self):
        return "" if getattr(self, "_ph_on", False) else self.entry.get().strip()

    def _set_entry(self, text):
        self.entry.delete(0, "end")
        self.entry.insert(0, text)
        self.entry.configure(fg=C["text"])
        self._ph_on = False

    def _convert(self):
        raw = self._entry_value()
        if not raw:
            self._toast("请粘贴 GitHub 链接")
            return
        if not raw.startswith(("http://", "https://")):
            raw = "https://" + raw
        if not self.core.running:
            self._toast("请先启动服务")
            return
        url = "http://127.0.0.1:%d/%s" % (self.core.port, raw)
        self._set_entry(url)
        self._toast("已转换")

    def _copy(self):
        txt = self._entry_value()
        if not txt:
            self._toast("没有内容")
            return
        self.root.clipboard_clear()
        self.root.clipboard_append(txt)
        self._toast("已复制")

    def _toast(self, msg):
        if not hasattr(self, "lbl_msg"):
            return
        self.lbl_msg.configure(text=msg)
        if self._toast_job:
            try:
                self.root.after_cancel(self._toast_job)
            except Exception:
                pass
        self._toast_job = self.root.after(2600,
                                         lambda: self.lbl_msg.configure(text=""))

    # ---- 刷新 -------------------------------------------------
    def _tick(self):
        self._t += 1
        stats = self.core.stats
        v = stats.speed()

        if self._t % 2 == 0:
            self.history.append(v)
            num, unit = speed_pair(v)
            self.lbl_num.configure(text=num)
            self.lbl_unit.configure(text=unit)
            if self._peak < v:
                self._peak = v
                self.lbl_peak.configure(text="峰值 " + human(self._peak) + "/s")
            self._draw_chart()

        if self._t % 3 == 0:
            snap = stats.snapshot()
            self.m_val["total"].configure(text=human(snap["total"]))
            self.m_val["reqs"].configure(text="%d" % snap["reqs"])
            rate = (100.0 * snap["ok"] / snap["reqs"]) if snap["reqs"] else 0.0
            self.m_val["rate"].configure(
                text=("%.0f%%" % rate) if snap["reqs"] else "—",
                fg=C["accent"] if snap["reqs"] and rate >= 90 else C["text"])
            self.m_val["active"].configure(text="%d" % snap["active"])
            self._draw_log(snap["recent"], snap["reqs"])

        if self._t % 4 == 0 and getattr(self, "_tab", "web") == "web":
            d = self.core.dns
            if d.scanning:
                self.lbl_dns.configure(text="检测中 %d%%" % d.progress,
                                       fg=C["accent"])
            else:
                ok = sum(1 for r in d.result.values() if r.get("ok"))
                stamp = (time.strftime("%H:%M:%S", time.localtime(d.last_scan))
                         if d.last_scan else "未检测")
                self.lbl_dns.configure(
                    text="%d/%d 可用 · %s" % (ok, len(ACCEL_DOMAINS), stamp),
                    fg=C["accent"] if ok else C["bad"])
            self._draw_domains()

        if self._t % 5 == 0:
            try:
                chan = self.core.stats.recent[0]["chan"]
            except (IndexError, KeyError, TypeError):
                chan = self.core.pool.best().name
            self.lbl_node.configure(text="通道 " + chan)
            self._draw_nodes()
            self._update_node_label()
            if (not self.core.pool.scanning
                    and time.time() - self.core.pool.last_scan > 45):
                self.core.pool.scan_async()

        self.root.after(self.TICK, self._tick)

    def _draw_log(self, items, total):
        cv = self.log_cv
        cv.delete("all")
        w = cv.winfo_width()
        if w < 40:
            return
        if total:
            self.lbl_cnt.configure(text="活跃 %d · 累计 %d"
                                   % (self.core.stats.active, total))
        else:
            self.lbl_cnt.configure(text="等待流量")
        if not items:
            cv.create_text(2, 10, text="暂无请求 · 转换一个 GitHub 链接试试",
                           anchor="w", fill=C["dim2"], font=F_SMALL)
            return
        rh = 15
        fm = self._fm
        c_time = fm.measure("00:00:00") + 11
        c_method = fm.measure("POST") + 14
        c_code = fm.measure("200") + 14
        c_size = fm.measure("999.9 MB") + 14
        c_ms = fm.measure("99999ms") + 14
        c_chan = fm.measure("ghproxy.imciel.com") + 14

        for i, it in enumerate(items):
            y = 4 + i * rh
            code = it["code"]
            running = it.get("running")
            if running:
                dot = C["warn"]
            elif 200 <= code < 300 or code == 0:
                dot = C["accent"]
            elif 300 <= code < 400:
                dot = C["blue"]
            else:
                dot = C["bad"]
            cv.create_oval(1, y + 4, 7, y + 10, fill=dot, outline="")
            x = 13
            cv.create_text(x, y + 7, text=it["ts"], anchor="w",
                           fill=C["dim2"], font=F_MONO)
            x += c_time
            cv.create_text(x, y + 7, text=it["method"], anchor="w",
                           fill=C["dim"], font=F_MONO)
            x += c_method
            cv.create_text(x, y + 7, text="···" if running else str(code),
                           anchor="w", fill=dot, font=F_MONO)
            x += c_code
            cv.create_text(x, y + 7, text=human(it["size"]), anchor="w",
                           fill=C["dim"], font=F_MONO)
            x += c_size
            cv.create_text(x, y + 7, text="%dms" % it["ms"], anchor="w",
                           fill=C["dim"], font=F_MONO)
            x += c_ms
            chan = it.get("chan", it.get("node", ""))
            cv.create_text(x, y + 7, text=chan[:18], anchor="w",
                           fill=C["accent"], font=F_MONO)
            x += c_chan
            room = max(60, w - x - 6)
            txt = short_target(it["url"], limit=max(16, int(room / 5.2)))
            cv.create_text(x, y + 7, text=txt, anchor="w",
                           fill=C["sub"], font=F_MONO)

    def _close(self):
        try:
            if getattr(self, "_git_on", False):
                self.core.git_disable()
        except Exception:
            pass
        try:
            self.core.stop()
        except Exception:
            pass
        self.root.destroy()

    def run(self):
        self.root.mainloop()


def resource_path(name):
    base = getattr(sys, "_MEIPASS", os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(base, name)


def dark_titlebar(win):
    try:
        win.update_idletasks()
        hwnd = ctypes.windll.user32.GetParent(win.winfo_id())
        for attr in (20, 19):
            val = ctypes.c_int(1)
            ctypes.windll.dwmapi.DwmSetWindowAttribute(
                hwnd, attr, ctypes.byref(val), ctypes.sizeof(val))
    except Exception:
        pass


def enable_dpi():
    try:
        ctypes.windll.shcore.SetProcessDpiAwareness(1)
    except Exception:
        try:
            ctypes.windll.user32.SetProcessDPIAware()
        except Exception:
            pass


def main():
    # 管理员子进程入口：只写 hosts，然后退出。
    # 参数用 base64 直接带过来，不碰临时文件，避免权限/路径差异。
    if "--apply-hosts-b64" in sys.argv:
        idx = sys.argv.index("--apply-hosts-b64")
        if idx + 1 < len(sys.argv):
            try:
                data = json.loads(
                    base64.b64decode(sys.argv[idx + 1]).decode("utf-8"))
                do_apply_hosts_data(data)
            except Exception:  # noqa: BLE001
                pass          # 子进程绝不弹异常框，静默退出
        return
    enable_dpi()
    core = Core()
    UI(core).run()


if __name__ == "__main__":
    main()
