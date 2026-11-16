"""Readers: each yields (table_name, [column names], iterator of row tuples).
Format is detected from the file's first bytes, not just its extension."""
import csv, io, os, shutil, sqlite3, subprocess, sys, zipfile


class Unsupported(Exception):
    pass


JUNK = {"desktop.ini", "thumbs.db", ".ds_store"}


def is_junk(name):
    n = os.path.basename(name).lower()
    return n in JUNK or n.startswith((".", "~$"))

csv.field_size_limit(min(sys.maxsize, 2**31 - 1))


def sniff(path):
    with open(path, "rb") as f:
        head = f.read(4096)
    if head.startswith(b"SQLite format 3\x00"):
        return "sqlite"
    if head[4:19] in (b"Standard Jet DB", b"Standard ACE DB"):
        return "access"
    if head.startswith(b"PK") and zipfile.is_zipfile(path):
        return "xlsx"
    if head[:8] == b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1":
        return "xls"
    ext = os.path.splitext(path)[1].lower()
    if ext == ".dbf" or (head[:1] in (b"\x03", b"\x83", b"\x8b", b"\xf5", b"\x30") and ext in (".dbf", ".dp", ".dr")):
        return "dbf"
    if b"\x00" in head[:2048] and ext not in (".txt", ".csv", ".tsv"):
        return "binary"
    return "text"


def read_sqlite(path):
    con = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    con.text_factory = lambda b: b.decode("utf-8", "replace")
    tables = [r[0] for r in con.execute("select name from sqlite_master where type='table' and name not like 'sqlite_%'")]
    for t in tables:
        cur = con.execute(f'select * from "{t}"')
        cols = [d[0] for d in cur.description]
        yield t, cols, iter(cur)


def read_access(path):
    if shutil.which("mdb-tables"):
        tables = subprocess.run(["mdb-tables", "-1", path], capture_output=True, text=True, check=True).stdout.split()
        for t in tables:
            p = subprocess.Popen(["mdb-export", "-D", "%Y-%m-%d %H:%M:%S", path, t], stdout=subprocess.PIPE)
            rd = csv.reader(io.TextIOWrapper(p.stdout, encoding="utf-8", errors="replace", newline=""))
            cols = next(rd, [])
            yield t, cols, rd
        return
    try:
        import pyodbc
    except ImportError:
        raise RuntimeError("Access file: install mdbtools (Linux/Mac: apt/brew install mdbtools) "
                           "or pip install pyodbc on Windows with the Access ODBC driver.")
    con = pyodbc.connect(r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=" + os.path.abspath(path) + ";")
    for r in con.cursor().tables(tableType="TABLE"):
        cur = con.cursor().execute(f"select * from [{r.table_name}]")
        yield r.table_name, [d[0] for d in cur.description], iter(cur)


def read_dbf(path):
    from dbfread import DBF
    t = DBF(path, encoding="latin-1", ignore_missing_memofile=True, char_decode_errors="replace", raw=False)
    cols = list(t.field_names)
    yield os.path.basename(path), cols, ((r.get(c) for c in cols) for r in t)


def read_xlsx(path):
    import openpyxl
    wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
    for ws in wb.worksheets:
        rows = ws.iter_rows(values_only=True)
        hdr = next(rows, None)
        if hdr is None:
            continue
        cols = [str(h) if h is not None else f"col{i+1}" for i, h in enumerate(hdr)]
        yield ws.title, cols, rows


def _open_text(path):
    for enc in ("utf-8-sig", "utf-16", "cp1256", "cp1252", "latin-1"):
        try:
            with open(path, encoding=enc) as f:
                f.read(1 << 20)
            return enc
        except (UnicodeError, OSError):
            continue
    return "latin-1"


def read_text(path):
    enc = _open_text(path)
    with open(path, encoding=enc, errors="replace", newline="") as f:
        sample = f.read(65536)
    first = sample.splitlines()[0] if sample else ""
    try:
        d = csv.Sniffer().sniff(sample, delimiters=",;\t|:")
        delim = d.delimiter
    except csv.Error:
        delim = None
    if delim and first.count(delim) == 0:
        delim = None
    f = open(path, encoding=enc, errors="replace", newline="")
    if delim:
        rd = csv.reader((l.replace("\x00", "") for l in f), delimiter=delim)
        cols = next(rd, [])
        yield os.path.basename(path), cols, rd
    else:  # free-form lines: one column
        yield os.path.basename(path), ["line"], ((l.rstrip("\r\n"),) for l in f if l.strip())


READERS = {"sqlite": read_sqlite, "access": read_access, "dbf": read_dbf, "xlsx": read_xlsx, "text": read_text}


def read_any(path):
    kind = sniff(path)
    if kind not in READERS:
        raise Unsupported(f"unsupported format '{kind}' (old .xls: save as .xlsx)")
    return kind, READERS[kind](path)
