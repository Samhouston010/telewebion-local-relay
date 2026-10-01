"""Telewebion live HLS, made light enough to play from outside Iran (owner 2026-10-01: channels froze).

Why they froze: every rendition's media playlist lists the last 1800 segments (325 KB) and the player re-reads it every
2 s -- that alone eats ~1.3 Mbit/s of a 1-3 Mbit/s link. Here the playlist is cut to the newest KEEP segments (~2 KB);
the video segments still come straight from Telewebion's edge, nothing but playlists passes through this server.

  /tw/<channel>/master.m3u8?t=<device token>   master (240p/480p/720p; 1080p dropped, the link can't carry it)
  /tw/<channel>/<240p|480p|720p>.m3u8?t=...    trimmed live playlist
  /tw/<channel>/s/<quality>/<segment>?t=...    video segment, relayed from memory (RELAY = True)

Segment relay (2026-10-01, after the playlist trim alone was not enough -- Streamer still re-buffered every 10-15 s):
the segments are not slow because the US<->Iran link lacks bandwidth (a 517 KB 720p segment arrives at ~320 KB/s, a
115 KB 240p one at only ~100 KB/s -- bigger files go FASTER, so the time is lost per request, not per byte). Every
segment was a fresh TCP + TLS handshake to Iran (~200 ms RTT, 3-4 round trips) and the player fetches them strictly
one after another, so a 2 s segment cost ~2 s and the buffer never grew. Now this server:
  * keeps warm keep-alive connections to the Telewebion edge (no handshake per request) and asks for gzip playlists;
  * sticks to one edge host per channel instead of letting it rotate every 30 s (re-resolved on any error);
  * downloads every new segment of the renditions being watched the moment it appears, several in parallel;
  * lists a segment in the playlist only once it is already in memory here (falls back to plain on-demand relay
    when the prefetch is behind), so the player always gets it US->US at full speed.
Cost: live delay grows by ~2-6 s, and the video now flows through this server (~1-2.5 Mbit/s per viewer).
Roll back: RELAY = False (playlists stay trimmed, segments go direct like before). Stats: /tw/stats?t=<token>
"""
import urllib.request, gzip, http.client, re, threading, time, urllib.parse
from concurrent.futures import ThreadPoolExecutor

UA = "Telewebion-AndroidTV-2.3.5(135)-TELEWEBION_TV-_release"
KEEP = 8
QUALITIES = ("240p", "480p", "720p")
LIVE_ENTRY = "https://ncdn.telewebion.net/%s/live/playlist.m3u8"   # .ir 301s here first -- skip that hop
_cache, _lock, _busy = {}, threading.Lock(), set()

# --- segment relay settings ---
RELAY = True
PREFETCH = 5          # newest segments of a watched rendition kept downloaded ahead
MAX_HOLD = 3          # list only downloaded segments while the prefetch is at most this many segments behind the edge
ACTIVE_SECS = 20      # a rendition counts as "watched" this long after its playlist was last polled
SEG_TTL = 120         # seconds a downloaded segment stays in memory
SEG_MAX_BYTES = 300 * 2**20
WORKERS = 6           # parallel segment downloads (all channels together)

_HDRS = {"User-Agent": UA, "X-APP-VERSION": "135", "X-OS": "AndroidTV", "Connection": "keep-alive"}
_idle, _plock = {}, threading.Lock()     # (scheme, host) -> idle keep-alive connections, shared by all threads


def _http(url, timeout=15, gz=False, hops=5):
    """GET over a shared keep-alive connection pool: a warm connection to the edge skips the TCP + TLS
    handshake (3-4 round trips to Iran, ~0.6-0.8 s) that every urllib request used to pay.
    -> (body bytes, final url after redirects)"""
    for attempt in (0, 1):
        u = urllib.parse.urlsplit(url)
        key = (u.scheme, u.netloc)
        with _plock:
            c = _idle.get(key, []).pop() if _idle.get(key) else None
        reused = c is not None
        if c is None:
            cls = http.client.HTTPSConnection if u.scheme == "https" else http.client.HTTPConnection
            c = cls(u.netloc, timeout=timeout)
        c.timeout = timeout
        if c.sock is not None:
            c.sock.settimeout(timeout)
        h = dict(_HDRS)
        if gz:
            h["Accept-Encoding"] = "gzip"
        try:
            c.request("GET", (u.path or "/") + ("?" + u.query if u.query else ""), headers=h)
            r = c.getresponse()
            body = r.read()
        except (http.client.HTTPException, OSError):
            c.close()
            if reused and attempt == 0:      # the edge closed an idle keep-alive connection -- once more on a fresh one
                continue
            raise
        if r.will_close:
            c.close()
        else:
            with _plock:
                lst = _idle.setdefault(key, [])
                if len(lst) < WORKERS + 4:
                    lst.append(c)
                else:
                    c.close()
        if r.status in (301, 302, 303, 307, 308) and hops:
            return _http(urllib.parse.urljoin(url, r.getheader("Location", "")), timeout, gz, hops - 1)
        if r.status != 200:
            raise IOError("HTTP %d %s" % (r.status, url))
        if (r.getheader("Content-Encoding") or "").lower() == "gzip":
            body = gzip.decompress(body)
        return body, url


def _get(url, timeout=15):
    body, final = _http(url, timeout, gz=True)
    return body.decode("utf-8", "replace"), final


def _cached(key, ttl, fn, stale=0):
    """cache for ttl seconds; with stale>0 an entry up to ttl+stale old is served at once while ONE background thread refreshes it
    (the upstream playlist takes ~1.5 s to download -- the player must not wait for that on every poll)"""
    now = time.time()
    with _lock:
        hit = _cache.get(key)
        if hit and now - hit[0] < ttl:
            return hit[1]
        if hit and stale and now - hit[0] < ttl + stale:
            if key not in _busy:
                _busy.add(key)
                threading.Thread(target=_refresh, args=(key, fn), daemon=True).start()
            return hit[1]
    return _refresh(key, fn)


def _refresh(key, fn):
    try:
        val = fn()
        with _lock:
            _cache[key] = (time.time(), val)
            if len(_cache) > 400:
                for k in sorted(_cache, key=lambda k: _cache[k][0])[:100]:
                    _cache.pop(k, None)
        return val
    finally:
        with _lock:
            _busy.discard(key)


def _base(desc):
    """the edge that Telewebion's redirector picks for us, e.g. https://live-xxx.telewebion.net/ek/tv1/live.
    Kept 10 min (was 30 s): a new edge host means new connections, i.e. new handshakes to Iran. Any failed
    playlist fetch drops it (_drop_base) so a dead edge is replaced at once."""
    def go():
        # .ir itself 301s to .net before reaching the real edge (owner noticed 2026-10-01) -- go straight to .net.
        _, final = _get(LIVE_ENTRY % desc)
        return final.split("/playlist.m3u8")[0]
    return _cached(("b", desc), 600, go)


def _drop_base(desc):
    with _lock:
        _cache.pop(("b", desc), None)


def master(desc, qs):
    def go():
        text, _ = _get("%s/playlist.m3u8" % _base(desc))
        out, pending, height = [], None, 0
        for ln in text.splitlines():
            if ln.startswith("#EXT-X-STREAM-INF"):
                pending = ln
                m = re.search(r"RESOLUTION=\d+x(\d+)", ln)
                height = int(m.group(1)) if m else 0
                continue
            if ln and not ln.startswith("#"):
                q = ln.split("/")[0]
                # up to 720p: the owner's home link can't sustain 1080p (freeze + quality thrash tested 2026-10-01 -- reverted)
                # owner 2026-10-01: tried 1080p again after the .net direct-connect fix -- still froze /
                # cut out completely within minutes. Capped at 720p for good, do not raise this again.
                if pending is not None and re.fullmatch(r"\d{3,5}p\d*", q) and 0 < height <= 720 or q == "1080p":   # 1080p allowed again 2026-10-01 now that segments are relayed (not the 50fps 99 Mbit placeholder)
                    out += [pending, "%s.m3u8%s" % (q, qs)]
                pending = None
                continue
            if not ln.startswith("#EXT-X-STREAM-INF"):
                out.append(ln)
        if not any(not l.startswith("#") for l in out):
            raise RuntimeError("no rendition up to 720p")
        return "\n".join(out) + "\n"
    body = _cached(("m", desc, qs), 20, go)
    if RELAY:
        # warm the top rendition (the one ExoPlayer usually opens first on a fast home link) while the player
        # is still parsing the master -- by its first segment request the newest segments are on their way
        top = [l.split(".m3u8")[0] for l in body.splitlines() if l and not l.startswith("#")]
        if top:
            threading.Thread(target=_warm, args=(desc, top[-1]), daemon=True).start()
    return body


def _parse_live(desc, q):
    """upstream rendition playlist -> (head tags, [(media sequence, tags, absolute url)] newest last).
    Cached ~1.5 s, then served stale for up to 5 s while one thread refreshes it; every refresh of a watched
    rendition immediately queues its new segments for download."""
    def go():
        base = "%s/%s/" % (_base(desc), q)
        try:
            text, final = _get(base + "index.m3u8")
        except Exception:
            _drop_base(desc)
            raise
        head, entries, pend, seq = [], [], [], 0
        for ln in text.splitlines():
            if ln.startswith("#EXT-X-MEDIA-SEQUENCE:"):
                seq = int(ln.split(":", 1)[1])
            elif ln.startswith(("#EXTINF", "#EXT-X-PROGRAM-DATE-TIME", "#EXT-X-DISCONTINUITY")):
                pend.append(ln)
            elif ln.startswith("#"):
                if not entries and not pend:
                    head.append(ln)
            elif ln.strip():
                entries.append((seq + len(entries), pend, urllib.parse.urljoin(final, ln.strip())))
                pend = []
        if not entries:
            raise RuntimeError("empty playlist")
        entries = entries[-(KEEP + PREFETCH + MAX_HOLD + 4):]
        if RELAY and _is_active(desc, q):
            _prefetch(entries)
        return head, entries
    return _cached(("p", desc, q), 1.5, go, stale=5)


def _render(head, keep, desc=None, q=None, t=""):
    out = head + ["#EXT-X-MEDIA-SEQUENCE:%d" % keep[0][0]]
    for _, tags, url in keep:
        uri = url
        if desc:
            name = url.partition("?")[0].rsplit("/", 1)[-1]
            if _NAME.fullmatch(name):
                with _slock:
                    _segmap[(desc, q, name)] = url
                uri = "s/%s/%s?t=%s" % (q, name, t)
        out += tags + [uri]
    return "\n".join(out) + "\n"


def media(desc, q, t=""):
    if not RELAY:
        head, entries = _parse_live(desc, q)
        return _render(head, entries[-KEEP:])
    key = (desc, q)
    with _slock:
        cold = not _is_active_locked(desc, q)
        _active[key] = time.time()
    head, entries = _parse_live(desc, q)
    if cold:
        _prefetch(entries)          # the cached copy may have been fetched while nobody was watching
    behind = 0                      # newest segments not downloaded here yet
    for _, _, url in reversed(entries):
        if _ready(url):
            break
        behind += 1
    # hold back the not-yet-downloaded tail -- unless the prefetch has fallen too far behind (cold start, slow
    # edge): then list everything and let the player wait on the on-demand relay instead of a stuck playlist
    end = len(entries) - behind if behind <= MAX_HOLD else len(entries)
    with _slock:
        last = _listed.get(key, -1)
        if last > entries[-1][0] or last < entries[0][0] - 50:   # stream restarted / long gap: forget it
            last = -1
        for i, e in enumerate(entries):     # never list less than before: ExoPlayer would see a stuck playlist
            if e[0] == last:
                end = max(end, i + 1)
                break
        end = max(end, 1)
        _listed[key] = entries[end - 1][0]
    return _render(head, entries[max(0, end - KEEP):end], desc, q, t)


# --- segment relay -----------------------------------------------------------------------------------------------

_NAME = re.compile(r"[A-Za-z0-9._\-]{1,160}")
_slock = threading.Lock()
_segs = {}          # upstream url -> _Seg
_segmap = {}        # (channel, quality, file name) -> upstream url
_active = {}        # (channel, quality) -> last playlist poll
_listed = {}        # (channel, quality) -> newest media sequence number handed to the player
_pool = ThreadPoolExecutor(WORKERS, thread_name_prefix="tw-seg")
_stats = {"since": time.time(), "fetched": 0, "bytes": 0, "secs": 0.0, "failed": 0, "hit": 0, "waited": 0,
          "recent": []}
_last_gc = [0.0]


class _Seg:
    __slots__ = ("t", "data", "ev", "claimed")

    def __init__(self):
        self.t, self.data, self.ev, self.claimed = time.time(), None, threading.Event(), False


def _is_active_locked(desc, q):
    return time.time() - _active.get((desc, q), 0) < ACTIVE_SECS


def _is_active(desc, q):
    with _slock:
        return _is_active_locked(desc, q)


def _ready(url):
    s = _segs.get(url)
    return s is not None and s.data is not None


def _warm(desc, q):
    try:
        with _slock:
            if not _is_active_locked(desc, q):
                _active[(desc, q)] = time.time() - ACTIVE_SECS + 8   # counts as watched for 8 s unless polled
        _prefetch(_parse_live(desc, q)[1])
    except Exception:  # noqa: BLE001
        pass


def _prefetch(entries):
    for _, _, url in entries[-PREFETCH:]:
        with _slock:
            if url in _segs:
                continue
            s = _segs[url] = _Seg()
        _pool.submit(_run, url, s)


def _run(url, s):
    with _slock:
        if s.claimed:
            return
        s.claimed = True
    _fetch_into(url, s)


def _fetch_into(url, s):
    t0, data = time.time(), None
    for i in range(3):
        try:
            data, _ = _http(url, timeout=15)
            break
        except Exception:  # noqa: BLE001
            with _slock:
                _stats["failed"] += 1
            time.sleep(0.3)
    took = time.time() - t0
    with _slock:
        if data is None:
            if _segs.get(url) is s:
                del _segs[url]
        else:
            s.data, s.t = data, time.time()
            _stats["fetched"] += 1
            _stats["bytes"] += len(data)
            _stats["secs"] += took
            _stats["recent"] = (_stats["recent"] + ["%s/%s  %d KB  %.2f s" % (
                url.rsplit("/", 2)[-2], url.rsplit("/", 1)[-1].partition("?")[0], len(data) // 1024, took)])[-20:]
        _gc_locked()
    s.ev.set()


def _gc_locked():
    now = time.time()
    if now - _last_gc[0] < 5:
        return
    _last_gc[0] = now
    for u in [u for u, s in _segs.items() if now - s.t > (SEG_TTL if s.data is not None else 60)]:
        del _segs[u]
    total = sum(len(s.data) for s in _segs.values() if s.data is not None)
    for u in sorted((u for u, s in _segs.items() if s.data is not None), key=lambda u: _segs[u].t):
        if total <= SEG_MAX_BYTES:
            break
        total -= len(_segs[u].data)
        del _segs[u]
    for k in [k for k, v in _active.items() if now - v > 600]:
        _active.pop(k, None)
        _listed.pop(k, None)
    if len(_segmap) > 5000:
        for k in list(_segmap)[:2500]:
            del _segmap[k]


def segment(desc, q, name):
    """bytes of one segment: from memory if the prefetch already has it, else wait for / do the download"""
    url = _segmap.get((desc, q, name)) or "%s/%s/%s" % (_base(desc), q, name)
    return _seg_data(url)


def _seg_data(url):
    with _slock:
        s = _segs.get(url)
        if s is not None and s.data is not None:
            _stats["hit"] += 1
            return s.data
        if s is None:
            s = _segs[url] = _Seg()
        mine = not s.claimed
        s.claimed = True
        _stats["waited"] += 1
    if mine:
        _fetch_into(url, s)
    else:
        s.ev.wait(25)
    return s.data


def stats():
    import json
    with _slock:
        st = dict(_stats)
        now = time.time()
        st["avg_seconds_per_segment"] = round(st["secs"] / st["fetched"], 2) if st["fetched"] else None
        st["avg_kbyte_per_s_from_iran"] = round(st["bytes"] / 1024 / st["secs"], 1) if st["secs"] else None
        st["watched_now"] = ["%s/%s" % k for k, v in _active.items() if now - v < ACTIVE_SECS]
        st["in_memory_mb"] = round(sum(len(s.data) for s in _segs.values() if s.data is not None) / 2**20, 1)
        st["uptime_min"] = round((now - st.pop("since")) / 60, 1)
        st.pop("secs")
    return json.dumps(st, indent=1).encode()


def _get_retry(url, tries=4):
    """Telewebion's archive redirector sometimes lands on an edge that answers 403/500 -- ask again"""
    err = None
    for _ in range(tries):
        try:
            return _get(url)
        except Exception as e:  # noqa: BLE001
            err = e
            time.sleep(0.3)
    raise err


def _filter_master(text, qs, prefix=""):
    out, pending, height = [], None, 0
    for ln in text.splitlines():
        if ln.startswith("#EXT-X-STREAM-INF"):
            pending = ln
            m = re.search(r"RESOLUTION=\d+x(\d+)", ln)
            height = int(m.group(1)) if m else 0
        elif ln and not ln.startswith("#"):
            q = ln.split("/")[0]
            if pending is not None and re.fullmatch(r"\d{3,5}p\d*", q) and 0 < height <= 720:
                out += [pending, "%s.m3u8%s" % (q, qs)]
            pending = None
        elif not ln.startswith("#EXT-X-STREAM-INF"):
            out.append(ln)
    if not any(not l.startswith("#") for l in out):
        raise RuntimeError("no rendition up to 720p")
    return "\n".join(out) + "\n"


def arch_master(ch, ep, qs):
    def go():
        text, _ = _get_retry("https://cdna.telewebion.net/%s/episode/%s/playlist.m3u8" % (ch, ep))   # .ir 301s to .net first, skip it
        return _filter_master(text, qs)
    return _cached(("am", ch, ep, qs), 300, go)


def arch_media(ch, ep, q, relay=False):
    """whole VOD playlist (a few hundred KB, read once). relay=False: absolute segment URLs on the edge that answered.
    relay=True (/tw/a/ route): segments point at our own /s/ route (token placeholder @@T@@) so the next ARCH_AHEAD
    segments are prefetched over warm connections instead of one fresh handshake to Iran per 2 s segment (= freezes)."""
    def go():
        text, final = _get_retry("https://cdna.telewebion.net/%s/episode/%s/%s/index.m3u8" % (ch, ep, q))   # same
        base = final.partition("?")[0].rsplit("/", 1)[0] + "/"
        out, urls = [], []
        for l in text.splitlines():
            if not l.strip() or l.startswith("#"):
                out.append(l)
                continue
            u = base + l.strip()
            name = u.partition("?")[0].rsplit("/", 1)[-1]
            if relay and _NAME.fullmatch(name):
                urls.append(u)
                out.append("/tw/a/%s/%s/s/%s/%s?t=@@T@@" % (ch, ep, q, name))
            else:
                out.append(u)
        if relay:
            with _slock:
                if len(_alist) > 40:
                    _alist.pop(next(iter(_alist)))
                _alist[(ch, ep, q)] = (urls, {u.partition("?")[0].rsplit("/", 1)[-1]: i for i, u in enumerate(urls)})
        return chr(10).join(out) + chr(10)
    return _cached(("ap", ch, ep, q, relay), 600, go)


ARCH_AHEAD = 8
_alist = {}         # (channel, episode, quality) -> ([segment urls in order], {file name: index})


def segment_arch(ch, ep, q, name):
    ent = _alist.get((ch, ep, q))
    if ent is None:
        arch_media(ch, ep, q, True)
        ent = _alist.get((ch, ep, q))
    if ent is None or name not in ent[1]:
        return None
    urls, idx = ent
    i = idx[name]
    for u in urls[i + 1:i + 1 + ARCH_AHEAD]:
        with _slock:
            if u in _segs:
                continue
            sg = _segs[u] = _Seg()
        _pool.submit(_run, u, sg)
    return _seg_data(urls[i])


API = "https://gateway.telewebion.net"


def _episodes(desc, ts):
    """recent programmes of one channel from Telewebion's own anonymous API (cached 10 min): [(start_unix, end_unix, EpisodeID)]"""
    def go():
        import calendar, datetime, json
        d = datetime.datetime.utcfromtimestamp(ts)
        q = urllib.parse.urlencode({"ChannelDescriptor": desc, "IsClip": "false", "First": 300, "Offset": 0,
                                    "FromDate": (d - datetime.timedelta(days=1)).date(), "ToDate": (d + datetime.timedelta(days=1)).date()})
        text, _ = _get(API + "/kandoo/channel/getChannelEpisodesByDate/?" + q)
        eps = json.loads(text)["body"]["queryChannel"][0]["episodes"]
        out = []
        for e in eps:
            try:
                a = calendar.timegm(time.strptime(e["started_at"][:19], "%Y-%m-%dT%H:%M:%S"))
                z = calendar.timegm(time.strptime(e["ended_at"][:19], "%Y-%m-%dT%H:%M:%S"))
            except Exception:  # noqa: BLE001
                continue
            out.append((a, z, e["EpisodeID"]))
        return out
    return _cached(("eps", desc, ts // 43200), 600, go)


def _find_episode(desc, ts):
    """channel descriptor + a utc unix timestamp (what TiviMate/GSE `catchup="shift"` sends) -> the archive episode id
    covering that moment, or None (nothing aired then / outside the ~2-day archive)"""
    try:
        eps = sorted(_episodes(desc, ts))
        for a, z, e in eps:
            if a <= ts < z:
                return e
        for a, z, e in reversed(eps):           # TiviMate sends the programme START it read from the guide; allow a small clock mismatch
            if a - 120 <= ts < a + 1800 and a <= ts + 120:
                return e
    except Exception:  # noqa: BLE001
        pass
    return None


def handle(h, token_ok):
    path, _, query = h.path.partition("?")
    params = dict(kv.split("=", 1) for kv in query.split("&") if "=" in kv)
    t = params.get("t", "")
    if not token_ok(t):
        return _send(h, 403, b"unknown device", "text/plain")
    s = re.fullmatch(r"/tw/([a-z0-9_]{1,40})/s/(\d{3,5}p\d*)/([A-Za-z0-9._\-]{1,160})", path)
    if s:
        try:
            data = segment(*s.groups())
        except Exception:  # noqa: BLE001
            data = None
        if data is None:
            return _send(h, 502, b"upstream", "text/plain")
        ext = s.group(3).rsplit(".", 1)[-1].lower()
        ctype = {"ts": "video/mp2t", "aac": "audio/aac", "m4s": "video/iso.segment", "mp4": "video/mp4"}.get(ext, "video/mp2t")
        return _send(h, 200, data, ctype, "public, max-age=60")
    if path == "/tw/stats":
        return _send(h, 200, stats(), "application/json")
    a = re.fullmatch(r"/tw/a/([a-z0-9_]{1,40})/(0x[0-9a-f]{3,12})/(master|\d{3,5}p\d*)\.m3u8", path)
    if a:
        ch, ep, f = a.groups()
        try:
            body = arch_master(ch, ep, "?t=" + t) if f == "master" else arch_media(ch, ep, f, RELAY).replace("@@T@@", t)
        except Exception:
            return _send(h, 502, b"upstream", "text/plain")
        return _send(h, 200, body.encode(), "application/vnd.apple.mpegurl")
    sa = re.fullmatch(r"/tw/a/([a-z0-9_]{1,40})/(0x[0-9a-f]{3,12})/s/(\d{3,5}p\d*)/([A-Za-z0-9._\-]{1,160})", path)
    if sa and RELAY:
        try:
            data = segment_arch(*sa.groups())
        except Exception:  # noqa: BLE001
            data = None
        if data is None:
            return _send(h, 502, b"upstream", "text/plain")
        return _send(h, 200, data, "video/mp2t", "public, max-age=60")
    m = re.fullmatch(r"/tw/([a-z0-9_]{1,40})/(master|\d{3,5}p\d*)\.m3u8", path)
    if not m:
        return _send(h, 404, b"not found", "text/plain")
    desc, f = m.groups()
    # catchup="shift" players (TiviMate, GSE) append utc=<program start>&lutc=<now> to the LIVE url for
    # time-shifted playback. Resolve it to an archived episode and serve that instead of live; the
    # trimmed-playlist renditions below keep "?" + query (utc included) so the quality follow-up request
    # resolves back to the same episode via plain relative-URL resolution -- no path rewrite needed.
    utc = params.get("utc")
    ep = _find_episode(desc, int(utc)) if utc and utc.isdigit() else None
    try:
        if ep:
            body = arch_master(desc, ep, "?" + query) if f == "master" else arch_media(desc, ep, f, RELAY).replace("@@T@@", t)
        else:
            body = master(desc, "?t=" + t) if f == "master" else media(desc, f, t)
    except Exception:
        return _send(h, 502, b"upstream", "text/plain")
    _send(h, 200, body.encode(), "application/vnd.apple.mpegurl")


def _send(h, code, body, ctype, cache="no-store"):
    h.send_response(code)
    h.send_header("Content-Type", ctype)
    h.send_header("Cache-Control", cache)
    h.send_header("Content-Length", str(len(body)))
    h.end_headers()
    h.wfile.write(body)
