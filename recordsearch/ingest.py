"""Index ONE file into its own shard database (runs in a worker process)."""
import json, os, sqlite3, time
from readers import read_any

BATCH = 100000


def clean(v):
    if v is None:
        return ""
    if isinstance(v, bytes):
        v = v.decode("latin-1", "replace")
    return str(v).replace("\t", " ").replace("\n", " ").replace("\r", " ").strip()


def write_status(path, **kw):
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(kw, f)
    os.replace(tmp, path)


def ingest_file(src, shard_path, status_path, name, delete_src=False, fts=True):
    """Creates shard_path (atomically, only when complete) and keeps status_path updated."""
    tmp = shard_path + ".building"
    st = dict(state="running", name=name, kind="?", rows=0, started=time.time())
    try:
        if os.path.exists(tmp):
            os.remove(tmp)
        write_status(status_path, **st)
        kind, tables = read_any(src)
        st["kind"] = kind
        db = sqlite3.connect(tmp)
        db.executescript("""pragma journal_mode=off; pragma synchronous=off; pragma cache_size=-300000;
          create table tables(tid integer primary key, name text, cols text);
          create table records(id integer primary key, tid int, data text);""")
        n, last = 0, time.time()
        for tname, cols, rows in tables:
            tid = db.execute("insert into tables(name,cols) values(?,?)",
                             (tname, "\t".join(clean(c) for c in cols))).lastrowid
            buf = []
            for row in rows:
                data = "\t".join([clean(v) for v in row])
                if data.strip("\t"):
                    buf.append((tid, data))
                if len(buf) >= BATCH:
                    db.executemany("insert into records(tid,data) values(?,?)", buf)
                    n += len(buf); buf = []
                    if time.time() - last > 0.5:
                        st["rows"] = n; write_status(status_path, **st); last = time.time()
            if buf:
                db.executemany("insert into records(tid,data) values(?,?)", buf); n += len(buf)
        st.update(rows=n, state="indexing")
        write_status(status_path, **st)
        if fts:
            db.executescript("""create virtual table fts using fts5(data, content='records', content_rowid='id',
                                tokenize='trigram case_sensitive 0');
                                insert into fts(fts) values('rebuild');""")
        db.commit(); db.close()
        os.replace(tmp, shard_path)
        st.update(state="done", seconds=round(time.time() - st["started"], 1))
        write_status(status_path, **st)
    except Exception as e:
        st.update(state="failed", error=str(e))
        write_status(status_path, **st)
        if os.path.exists(tmp):
            os.remove(tmp)
    finally:
        if delete_src:
            try: os.remove(src)
            except OSError: pass
