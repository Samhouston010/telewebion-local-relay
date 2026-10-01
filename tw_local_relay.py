#!/usr/bin/env python3
"""Telewebion local relay -- runs on YOUR OWN always-on device (PC, Raspberry Pi, old Android box with Termux).

Put ONE address into your IPTV player (TiviMate, VLC, Kodi, ...):

        http://<this-device-ip>:8765/playlist.m3u

The relay fetches the public playlist, points every Telewebion live channel at itself, and then does what makes
Telewebion play without freezing from outside Iran: keeps warm connections to Telewebion, cuts the 325 KB live
playlist down to ~2 KB, and downloads the newest video segments ahead of the player (see tw_proxy.py).
Quality is chosen by your player automatically (adaptive), exactly like the official app.

No dependencies, no account, no token. Only devices on your own network can reach it unless you open the port yourself.
Run:   python tw_local_relay.py            (Python 3.8+)
Env:   TW_PORT=8765   TW_PLAYLIST=<url of the m3u to convert>   TW_BIND=0.0.0.0
"""
import os, re, socket, sys, threading, time, urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import tw_proxy

__version__ = "1.0.0"
HERE = os.path.dirname(os.path.abspath(__file__))
UPDATE_BASE = os.environ.get("TW_UPDATE_URL", "https://raw.githubusercontent.com/Samhouston010/telewebion-local-relay/main")
AUTOUPDATE = os.environ.get("TW_AUTOUPDATE", "1") != "0"
PORT = int(os.environ.get("TW_PORT", "8765"))
BIND = os.environ.get("TW_BIND", "0.0.0.0")
UPSTREAM = os.environ.get("TW_PLAYLIST", "https://raw.githubusercontent.com/Samhouston010/persian-tv/master/playlist.m3u")
TTL = 30 * 60                                   # re-read the public playlist every 30 minutes
LINK = re.compile(r"https://(?:ncdn|cdn|cdna)\.telewebion\.(?:ir|net)/([a-z0-9_]{1,40})/live/playlist\.m3u8")

_cache = {"t": 0.0, "text": ""}
_lock = threading.Lock()


def upstream_text():
    with _lock:
        if time.time() - _cache["t"] > TTL or not _cache["text"]:
            try:
                req = urllib.request.Request(UPSTREAM, headers={"User-Agent": "tw-local-relay"})
                _cache["text"] = urllib.request.urlopen(req, timeout=60).read().decode("utf-8", "replace")
                _cache["t"] = time.time()
            except Exception as e:  # keep serving the last good copy
                if not _cache["text"]:
                    raise
                print("playlist refresh failed, using the old copy:", e, flush=True)
        return _cache["text"]


def lan_ip():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("10.255.255.255", 1))
        return s.getsockname()[0]
    except OSError:
        return "127.0.0.1"
    finally:
        try:
            s.close()
        except Exception:  # noqa: BLE001
            pass


def tag_catchup(text):
    """TiviMate/GSE: mark every Telewebion live channel `catchup="shift"` (2 days) -- the player then appends utc=/lutc= to the
    live url when you pick a past programme, and tw_proxy turns that into the archived episode."""
    nl = chr(10)
    lines = text.split(nl)
    for i, l in enumerate(lines):
        if l.startswith("#EXTINF") and "catchup=" not in l:
            for u in lines[i + 1:i + 12]:
                if u.startswith("http"):
                    if "/tw/" in u and "/master.m3u8" in u and "?t=local" in u:
                        lines[i] = l.replace("#EXTINF:-1", '#EXTINF:-1 catchup="shift" catchup-days="2"', 1)
                    break
    return nl.join(lines)


def _vtuple(v):
    return tuple(int(x) for x in re.findall(r"\d+", v))


def check_update():
    """Newer release on GitHub? Download every file listed in version.json, verify its SHA-256 and that it compiles, keep a .bak of
    the old one, replace, then restart. Any failure leaves the running version untouched."""
    import hashlib, json, py_compile, tempfile
    try:
        info = json.loads(urllib.request.urlopen(UPDATE_BASE + "/version.json", timeout=30).read().decode("utf-8"))
        if _vtuple(info["version"]) <= _vtuple(__version__):
            return False
        tmp = tempfile.mkdtemp(prefix="twrelay-")
        for name, sha in info["files"].items():
            if "/" in name or "\\" in name or not name.endswith(".py"):
                raise ValueError("unexpected file name " + name)
            data = urllib.request.urlopen(UPDATE_BASE + "/" + name, timeout=60).read()
            if hashlib.sha256(data).hexdigest() != sha:
                raise ValueError("hash mismatch for " + name)
            open(os.path.join(tmp, name), "wb").write(data)
            py_compile.compile(os.path.join(tmp, name), doraise=True)
        for name in info["files"]:
            cur = os.path.join(HERE, name)
            if os.path.exists(cur):
                os.replace(cur, cur + ".bak")
            os.replace(os.path.join(tmp, name), cur)
        print("updated to version %s -- restarting" % info["version"], flush=True)
        return True
    except Exception as e:  # noqa: BLE001
        print("update check failed (keeping the current version):", e, flush=True)
        return False


def update_loop():
    time.sleep(30)
    while True:
        if check_update():
            os.execv(sys.executable, [sys.executable] + sys.argv)
        time.sleep(6 * 3600)


MAIN = ("tv1", "tv2", "tv3", "tv4", "tehran", "irinn", "ifilm", "jahanbin")     # channels that get VOD catch-up tiles
PER_CHANNEL = 40
MARK = "تلوبیون Catch-up VOD"
_cu = {"t": 0.0, "text": ""}


def catchup_m3u(playlist_text, host):
    """One group per main channel, newest first: every recent programme (>= 2 min) as its own playable entry that goes
    through this relay (/tw/a/<channel>/<episode>/master.m3u8). Programmes come from Telewebion's anonymous API."""
    import datetime, json, urllib.parse
    if time.time() - _cu["t"] < 600 and _cu["text"]:
        return _cu["text"].replace("@@H@@", host)
    nl = chr(10)
    names = {}
    for l, u in zip(playlist_text.split(nl), playlist_text.split(nl)[1:]):
        pass
    lines = playlist_text.split(nl)
    for i, l in enumerate(lines):
        if l.startswith("#EXTINF"):
            for u in lines[i + 1:i + 12]:
                m = re.search(r"/tw/([a-z0-9_]+)/master\.m3u8", u)
                if m and m.group(1) in MAIN:
                    names[m.group(1)] = (l.rsplit(",", 1)[-1].strip(), (re.search(r'tvg-logo="([^"]*)"', l) or [None, ""])[1])
                    break
    out = ["#EXTM3U"]
    now = datetime.datetime.now(datetime.timezone.utc)
    tz = datetime.timedelta(hours=3, minutes=30)
    for slug in MAIN:
        if slug not in names:
            continue
        q = urllib.parse.urlencode({"ChannelDescriptor": slug, "IsClip": "false", "First": 300, "Offset": 0,
                                    "FromDate": (now - datetime.timedelta(days=2)).date(), "ToDate": (now + datetime.timedelta(days=1)).date()})
        try:
            text, _ = tw_proxy._get(tw_proxy.API + "/kandoo/channel/getChannelEpisodesByDate/?" + q)
            eps = json.loads(text)["body"]["queryChannel"][0]["episodes"]
        except Exception as e:  # noqa: BLE001
            print("catch-up: skipped", slug, str(e)[:60], flush=True)
            continue
        n = 0
        for e in sorted(eps, key=lambda e: e["started_at"], reverse=True):
            try:
                a = datetime.datetime.strptime(e["started_at"][:19], "%Y-%m-%dT%H:%M:%S")
                z = datetime.datetime.strptime(e["ended_at"][:19], "%Y-%m-%dT%H:%M:%S")
            except Exception:  # noqa: BLE001
                continue
            if z.replace(tzinfo=datetime.timezone.utc) > now or (z - a).total_seconds() < 120 or n >= PER_CHANNEL:     # only programmes that have FINISHED airing
                continue
            p = e.get("program") or {}
            title = ((p.get("title") or "").strip() or (e.get("title") or "").strip()).replace(",", "،").replace('"', "'")
            sub = (e.get("title") or "").strip().replace(",", "،").replace('"', "'")
            if not title:
                continue
            n += 1
            img = ("https://static.telewebion.net/episodeImages/%s/default" % e["image"]) if e.get("image") else names[slug][1]
            out.append('#EXTINF:-1 tvg-logo="%s" group-title="%s: %s",%s%s • %s' % (
                img, MARK, names[slug][0], title, (" - " + sub) if sub and sub != title else "", (a + tz).strftime("%m/%d %H:%M")))
            out.append("http://@@H@@/tw/a/%s/%s/master.m3u8?t=local" % (slug, e["EpisodeID"]))
    _cu["text"] = nl.join(out) + nl
    _cu["t"] = time.time()
    return _cu["text"].replace("@@H@@", host)


class H(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "tw-local-relay"

    def log_message(self, *a):          # quiet
        pass

    def _send(self, code, body, ctype):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        path = self.path.split("?")[0]
        if path in ("/playlist.m3u", "/playlist.m3u8"):
            host = self.headers.get("Host") or "%s:%d" % (lan_ip(), PORT)
            try:
                text = upstream_text()
            except Exception as e:  # noqa: BLE001
                return self._send(502, ("cannot read the public playlist: %s" % e).encode(), "text/plain")
            body = LINK.sub(lambda m: "http://%s/tw/%s/master.m3u8?t=local" % (host, m.group(1)), text)
            body = tag_catchup(body)
            try:
                body = body.rstrip() + chr(10) + chr(10) + catchup_m3u(body, host).split(chr(10), 1)[1]    # drop the 2nd #EXTM3U header
            except Exception as e:  # noqa: BLE001
                print("catch-up tiles skipped:", e, flush=True)
            return self._send(200, body.encode("utf-8"), "audio/x-mpegurl; charset=utf-8")
        if path.startswith("/tw/"):
            return tw_proxy.handle(self, lambda t: True)
        page = ("Telewebion local relay is running.\n\nPut this address in your IPTV player:\n    http://%s:%d/playlist.m3u\n\n"
                "Health: /tw/stats?t=local\n" % (lan_ip(), PORT))
        return self._send(200, page.encode(), "text/plain; charset=utf-8")


def main():
    srv = ThreadingHTTPServer((BIND, PORT), H)
    srv.daemon_threads = True
    if AUTOUPDATE:
        threading.Thread(target=update_loop, daemon=True).start()
    print("Telewebion local relay on port %d\nPlaylist address for your player:  http://%s:%d/playlist.m3u"
          % (PORT, lan_ip(), PORT), flush=True)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
