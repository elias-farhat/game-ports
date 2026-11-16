#!/usr/bin/env python3
"""Local web UI.  usage: server.py [-d index.db] [-p 8000]   then open http://localhost:8000"""
import argparse, json, os, sqlite3
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

HERE = os.path.dirname(os.path.abspath(__file__))
DB = "index.db"


def q(db, sql, args=()):
    return db.execute(sql, args).fetchall()


def search(term, mode, file_id, limit, offset):
    db = sqlite3.connect(f"file:{DB}?mode=ro", uri=True)
    has_fts = bool(q(db, "select 1 from sqlite_master where name='fts'"))
    term = term.strip()
    fj = " and r.file_id=%d" % int(file_id) if file_id else ""
    lo = term.lower()
    rows = []
    if len(term) >= 3 and has_fts:
        phrase = '"' + term.replace('"', '""') + '"'
        base = f"from fts join records r on r.id=fts.rowid where fts match ?{fj}"
        cand = q(db, f"select r.id,r.file_id,r.tbl,r.cols,r.data {base} limit 20000", (phrase,))
    elif mode == "contains" or not term:
        return {"error": "Need at least 3 characters for contains search (or build the index with FTS)."}
    else:  # short exact/prefix: scan
        cand = q(db, f"select r.id,r.file_id,r.tbl,r.cols,r.data from records r where 1{fj} limit 2000000")
    out = []
    for id_, f, tbl, cols, data in cand:
        vals = data.split("\t")
        if mode == "contains":
            if lo not in data.lower(): continue
        elif mode == "exact":
            if not any(v.lower() == lo for v in vals): continue
        elif mode == "prefix":
            if not any(v.lower().startswith(lo) for v in vals): continue
        out.append((id_, f, tbl, cols, vals))
    total = len(out)
    names = dict(q(db, "select id,path from files"))
    page = out[offset:offset + limit]
    res = []
    for id_, f, tbl, cols, vals in page:
        cn = cols.split("\t")
        res.append({"file": os.path.basename(names.get(f, "")), "table": tbl,
                    "fields": [[cn[i] if i < len(cn) else f"col{i+1}", v] for i, v in enumerate(vals) if v]})
    return {"total": total, "capped": len(cand) >= 20000, "results": res}


class H(BaseHTTPRequestHandler):
    def log_message(self, *a): pass

    def send(self, body, ctype="application/json"):
        b = body if isinstance(body, bytes) else body.encode()
        self.send_response(200); self.send_header("Content-Type", ctype + "; charset=utf-8")
        self.send_header("Content-Length", str(len(b))); self.end_headers(); self.wfile.write(b)

    def do_GET(self):
        u = urlparse(self.path); p = parse_qs(u.query)
        g = lambda k, d="": p.get(k, [d])[0]
        if u.path == "/":
            return self.send(open(os.path.join(HERE, "index.html"), "rb").read(), "text/html")
        if u.path == "/api/files":
            db = sqlite3.connect(f"file:{DB}?mode=ro", uri=True)
            return self.send(json.dumps([{"id": i, "name": os.path.basename(n), "kind": k, "rows": r}
                                         for i, n, k, r in q(db, "select id,path,kind,rows from files")]))
        if u.path == "/api/search":
            try:
                r = search(g("q"), g("mode", "contains"), g("file"), min(int(g("limit", "50")), 500), int(g("offset", "0")))
            except Exception as e:
                r = {"error": str(e)}
            return self.send(json.dumps(r))
        self.send_response(404); self.end_headers()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("-d", "--db", default="index.db"); ap.add_argument("-p", "--port", type=int, default=8000)
    a = ap.parse_args(); DB = a.db
    print(f"Open http://localhost:{a.port}")
    ThreadingHTTPServer(("127.0.0.1", a.port), H).serve_forever()
