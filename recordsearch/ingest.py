#!/usr/bin/env python3
"""Index files/folders into one SQLite database.
usage: ingest.py [-d index.db] PATH [PATH ...]   (folders are scanned recursively)"""
import argparse, os, sqlite3, sys, time
from readers import read_any

BATCH = 50000


def clean(v):
    if v is None:
        return ""
    if isinstance(v, bytes):
        v = v.decode("latin-1", "replace")
    return str(v).replace("\t", " ").replace("\n", " ").replace("\r", " ").strip()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("paths", nargs="+")
    ap.add_argument("-d", "--db", default="index.db")
    ap.add_argument("--no-fts", action="store_true", help="skip substring index (smaller, exact/prefix only)")
    a = ap.parse_args()

    db = sqlite3.connect(a.db)
    db.executescript("""
      pragma journal_mode=off; pragma synchronous=off; pragma cache_size=-500000;
      create table if not exists files(id integer primary key, path text unique, kind text, rows integer);
      create table if not exists records(id integer primary key, file_id int, tbl text, cols text, data text);
    """)
    files = []
    for p in a.paths:
        if os.path.isdir(p):
            for r, _, fs in os.walk(p):
                files += [os.path.join(r, f) for f in fs]
        else:
            files.append(p)

    total = 0
    t0 = time.time()
    for path in sorted(files):
        if db.execute("select 1 from files where path=?", (os.path.abspath(path),)).fetchone():
            print("skip (already indexed):", path); continue
        try:
            kind, tables = read_any(path)
            n = 0
            fid = db.execute("insert into files(path,kind,rows) values(?,?,0)", (os.path.abspath(path), kind)).lastrowid
            for tbl, cols, rows in tables:
                cj = "\t".join(clean(c) for c in cols)
                buf = []
                for row in rows:
                    data = "\t".join(clean(v) for v in row)
                    if data.strip("\t"):
                        buf.append((fid, tbl, cj, data))
                    if len(buf) >= BATCH:
                        db.executemany("insert into records(file_id,tbl,cols,data) values(?,?,?,?)", buf)
                        n += len(buf); buf = []
                        print(f"\r{path}: {n:,} rows", end="", flush=True)
                if buf:
                    db.executemany("insert into records(file_id,tbl,cols,data) values(?,?,?,?)", buf); n += len(buf)
            db.execute("update files set rows=? where id=?", (n, fid))
            db.commit()
            total += n
            print(f"\r{path} [{kind}]: {n:,} rows        ")
        except Exception as e:
            db.rollback()
            print(f"\nFAILED {path}: {e}", file=sys.stderr)

    if not a.no_fts:
        print("Building substring index (slow for tens of millions of rows)...")
        db.executescript("""
          drop table if exists fts;
          create virtual table fts using fts5(data, content='records', content_rowid='id',
                                              tokenize='trigram case_sensitive 0');
          insert into fts(fts) values('rebuild');
        """)
    db.commit()
    print(f"Done: {total:,} new rows in {time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()
