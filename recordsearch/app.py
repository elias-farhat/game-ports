#!/usr/bin/env python3
"""Record Search: containers of indexed files, local web UI.
usage: app.py [-D data_dir] [-p 8000]   then open http://localhost:8000"""
import argparse, glob, multiprocessing, socket, sys, threading, webbrowser, json, os, re, shutil, sqlite3, time, uuid
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs, unquote
from ingest import ingest_file
from readers import is_junk

HERE = getattr(sys, "_MEIPASS", os.path.dirname(os.path.abspath(__file__)))
FROZEN = getattr(sys, "frozen", False)
DATA = "data"
POOL = None
SEARCH_POOL = ThreadPoolExecutor(16)
CAP = 20000


# ---------- containers ----------
def cdir(cid):
    if not re.fullmatch(r"[a-z0-9-]+", cid or ""):
        raise ValueError("bad container id")
    return os.path.join(DATA, cid)


def read_json(p, default=None):
    try:
        with open(p) as f: return json.load(f)
    except (OSError, ValueError): return default


def list_files(cid):
    out = []
    for p in sorted(glob.glob(os.path.join(cdir(cid), "files", "*.json"))):
        s = read_json(p)
        if not s: continue
        s["id"] = os.path.basename(p)[:-5]
        if s.get("state") in ("running", "indexing", "queued") and s["id"] not in ACTIVE:
            s["state"] = "failed"; s["error"] = "interrupted (server restarted)"
        out.append(s)
    return out


ACTIVE = set()


def container_info(cid):
    meta = read_json(os.path.join(cdir(cid), "meta.json"), {})
    files = list_files(cid)
    return {"id": cid, "label": meta.get("label", cid), "created": meta.get("created", 0),
            "passwords": meta.get("passwords", []), "files": files, "rows": sum(f.get("rows", 0) for f in files if f.get("state") == "done")}


def all_containers():
    return sorted((container_info(os.path.basename(d)) for d in glob.glob(os.path.join(DATA, "*")) if os.path.isdir(d)),
                  key=lambda c: c["created"])


def new_container(label):
    label = label.strip()[:60] or "Untitled"
    cid = (re.sub(r"[^a-z0-9]+", "-", label.lower()).strip("-") or "c")[:30] + "-" + uuid.uuid4().hex[:6]
    os.makedirs(os.path.join(cdir(cid), "files")); os.makedirs(os.path.join(cdir(cid), "shards"))
    os.makedirs(os.path.join(cdir(cid), "uploads"))
    with open(os.path.join(cdir(cid), "meta.json"), "w") as f: json.dump({"label": label, "created": time.time()}, f)
    return cid


def submit(cid, src, name, delete_src, fid=None):
    fid = fid or uuid.uuid4().hex[:10]
    d = cdir(cid)
    pws = read_json(os.path.join(d, "meta.json"), {}).get("passwords", [])
    sp = os.path.join(d, "files", fid + ".json")
    with open(sp, "w") as f: json.dump({"state": "queued", "name": name, "rows": 0, "kind": "?"}, f)
    ACTIVE.add(fid)
    fut = POOL.submit(ingest_file, src, os.path.join(d, "shards", fid + ".db"), sp, name, delete_src, True, tuple(pws))
    fut.add_done_callback(lambda _f: ACTIVE.discard(fid))
    return fid


def retry(cid, fid):
    st = read_json(os.path.join(cdir(cid), "files", fid + ".json"), {})
    if fid in ACTIVE: return 1
    if not st.get("src") or not os.path.exists(st["src"]):
        raise ValueError("Original file is gone (drive unplugged or upload removed) - add it again.")
    submit(cid, st["src"], st["name"], st.get("upload", False), fid)
    return 1


def retry_failed(cid):
    return sum(retry(cid, f["id"]) for f in list_files(cid) if f.get("state") == "failed")


def add_path(cid, p):
    p = p.strip().strip('"').strip("'")
    if os.path.isdir(p):
        base = os.path.dirname(os.path.abspath(p))
        paths = [os.path.join(r, f) for r, ds, fs in os.walk(p) for f in fs if not is_junk(f)
                 if not any(part.startswith(".") for part in os.path.relpath(r, p).split(os.sep) if part != ".")]
        return [submit(cid, x, os.path.relpath(x, base), False) for x in sorted(paths)]
    if os.path.isfile(p):
        return [submit(cid, p, os.path.basename(p), False)]
    raise ValueError(f"not found on this computer: {p}")


# ---------- search ----------
def search_shard(path, term, mode, need):
    db = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    has_fts = db.execute("select 1 from sqlite_master where name='fts'").fetchone()
    lo = term.lower()
    sel = "select t.name,t.cols,r.data from {} join records r on r.id=fts.rowid join tables t on t.tid=r.tid where fts match ?"
    phrase = '"' + term.replace('"', '""') + '"'
    if len(term) >= 3 and has_fts:
        if mode == "contains":
            total = db.execute("select count(*) from (select 1 from fts where fts match ? limit 100001)", (phrase,)).fetchone()[0]
            rows = db.execute(sel.format("fts") + " limit ?", (phrase, need)).fetchall()
            return total, rows
        cand = db.execute(sel.format("fts") + f" limit {CAP}", (phrase,)).fetchall()
    else:
        if mode == "contains":
            raise ValueError("'Contains' needs at least 3 characters.")
        cand = db.execute("select t.name,t.cols,r.data from records r join tables t on t.tid=r.tid limit 2000000").fetchall()
    ok = (lambda vs: any(v.lower() == lo for v in vs)) if mode == "exact" else (lambda vs: any(v.lower().startswith(lo) for v in vs))
    rows = [c for c in cand if ok(c[2].split("\t"))]
    return len(rows), rows[:need]


def search(cids, term, mode, offset, limit):
    term = term.strip()
    if not term: return {"total": 0, "results": []}
    jobs = []
    for cid in cids:
        label = read_json(os.path.join(cdir(cid), "meta.json"), {}).get("label", cid)
        names = {os.path.basename(p)[:-5]: (read_json(p) or {}).get("name", "") for p in glob.glob(os.path.join(cdir(cid), "files", "*.json"))}
        for p in sorted(glob.glob(os.path.join(cdir(cid), "shards", "*.db"))):
            fid = os.path.basename(p)[:-3]
            jobs.append((label, names.get(fid, fid), SEARCH_POOL.submit(search_shard, p, term, mode, offset + limit)))
    total, merged = 0, []
    for label, fname, fut in jobs:
        t, rows = fut.result()
        total += t
        merged += [(label, fname, *r) for r in rows]
    res = []
    for label, fname, tname, cols, data in merged[offset:offset + limit]:
        cn = cols.split("\t")
        res.append({"container": label, "file": fname, "table": tname,
                    "fields": [[cn[i] if i < len(cn) else f"col{i+1}", v] for i, v in enumerate(data.split("\t")) if v]})
    return {"total": total, "capped": total >= 100001 or total >= CAP, "results": res}


# ---------- http ----------
class H(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    def log_message(self, *a): pass

    def send(self, obj, code=200, ctype="application/json"):
        b = obj if isinstance(obj, bytes) else (obj if isinstance(obj, str) else json.dumps(obj)).encode()
        self.send_response(code); self.send_header("Content-Type", ctype + "; charset=utf-8")
        self.send_header("Content-Length", str(len(b))); self.end_headers(); self.wfile.write(b)

    def handle_any(self, method):
        u = urlparse(self.path); p = parse_qs(u.query); parts = [unquote(x) for x in u.path.strip("/").split("/")]
        g = lambda k, d="": p.get(k, [d])[0]
        try:
            if method == "GET" and u.path == "/":
                return self.send(open(os.path.join(HERE, "index.html"), "rb").read(), ctype="text/html")
            if parts[:2] == ["api", "containers"]:
                if len(parts) == 2:
                    if method == "GET": return self.send(all_containers())
                    if method == "POST":
                        body = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0))) or b"{}")
                        return self.send({"id": new_container(body.get("label", ""))})
                cid = parts[2]; cdir(cid)
                if len(parts) == 3:
                    if method == "GET": return self.send(container_info(cid))
                    if method == "DELETE": shutil.rmtree(cdir(cid)); return self.send({"ok": 1})
                    if method == "PATCH":
                        body = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0))))
                        mp = os.path.join(cdir(cid), "meta.json"); m = read_json(mp, {})
                        if "label" in body: m["label"] = body["label"].strip()[:60]
                        if "passwords" in body: m["passwords"] = [x for x in body["passwords"] if x]
                        json.dump(m, open(mp, "w")); return self.send({"ok": 1})
                if parts[3:] == ["upload"] and method == "POST":
                    name = g("name", "file").replace("\\", "/").strip("/") or "file"
                    tmp = os.path.join(cdir(cid), "uploads", uuid.uuid4().hex[:8] + "_" + re.sub(r"[^\w.\-]", "_", os.path.basename(name)))
                    left = int(self.headers.get("Content-Length", 0))
                    with open(tmp, "wb") as f:
                        while left:
                            chunk = self.rfile.read(min(1 << 20, left))
                            if not chunk: break
                            f.write(chunk); left -= len(chunk)
                    return self.send({"id": submit(cid, tmp, name, True)})
                if parts[3:] == ["retry"] and method == "POST":
                    return self.send({"retried": retry_failed(cid)})
                if parts[3:] == ["addpath"] and method == "POST":
                    body = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0))))
                    return self.send({"ids": add_path(cid, body["path"])})
                if len(parts) == 6 and parts[3] == "files" and parts[5] == "retry" and method == "POST":
                    return self.send({"ok": retry(cid, re.sub(r"[^a-z0-9]", "", parts[4]))})
                if len(parts) == 5 and parts[3] == "files" and method == "DELETE":
                    fid = re.sub(r"[^a-z0-9]", "", parts[4])
                    st = read_json(os.path.join(cdir(cid), "files", fid + ".json"), {})
                    if st.get("upload") and st.get("src"):
                        try: os.remove(st["src"])
                        except OSError: pass
                    for ext in ("files/%s.json", "shards/%s.db"):
                        try: os.remove(os.path.join(cdir(cid), ext % fid))
                        except OSError: pass
                    return self.send({"ok": 1})
            if u.path == "/api/search" and method == "GET":
                c = g("c", "all")
                cids = [x["id"] for x in all_containers()] if c == "all" else [c]
                return self.send(search(cids, g("q"), g("mode", "contains"), int(g("offset", "0")), min(int(g("limit", "50")), 500)))
            self.send({"error": "not found"}, 404)
        except Exception as e:
            self.send({"error": str(e)}, 400)

    do_GET = lambda s: s.handle_any("GET")
    do_POST = lambda s: s.handle_any("POST")
    do_DELETE = lambda s: s.handle_any("DELETE")
    do_PATCH = lambda s: s.handle_any("PATCH")


def free_port(start):
    for port in range(start, start + 50):
        with socket.socket() as sk:
            if sk.connect_ex(("127.0.0.1", port)) != 0:
                return port
    raise SystemExit("no free port")


if __name__ == "__main__":
    multiprocessing.freeze_support()
    default_data = os.path.join(os.path.dirname(sys.executable), "RecordSearch-data") if FROZEN else "data"
    ap = argparse.ArgumentParser()
    ap.add_argument("-D", "--data", default=default_data); ap.add_argument("-p", "--port", type=int, default=8000)
    ap.add_argument("-w", "--workers", type=int, default=os.cpu_count() or 2)
    ap.add_argument("--no-browser", action="store_true")
    a = ap.parse_args(); DATA = a.data; os.makedirs(DATA, exist_ok=True)
    keep = {(read_json(p) or {}).get("src") for p in glob.glob(os.path.join(DATA, "*", "files", "*.json"))}
    for up in glob.glob(os.path.join(DATA, "*", "uploads", "*")):
        if up in keep: continue
        try: os.remove(up)
        except OSError: pass
    POOL = ProcessPoolExecutor(a.workers)
    port = free_port(a.port); url = f"http://localhost:{port}"
    srv = ThreadingHTTPServer(("127.0.0.1", port), H)
    print(f"Record Search is running at {url}\nData folder: {os.path.abspath(DATA)}\n"
          f"{a.workers} parallel indexers. Keep this window open; close it to stop.")
    if not a.no_browser:
        threading.Timer(0.8, lambda: webbrowser.open(url)).start()
    srv.serve_forever()
