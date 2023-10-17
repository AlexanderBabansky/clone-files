import os
import hashlib
import sqlite3


class FileBackuped:
    def __init__(self):
        self.filepath = None
        self.mod_timestamp = None
        self.timestamp = None
        self.hash = None


def create_empty_db(filepath: str):
    sqlcon = sqlite3.connect(filepath)
    cur = sqlcon.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS "file_history" (
            "id"	INTEGER NOT NULL,
            "filepath"	TEXT NOT NULL,
            "timestamp"	INTEGER NOT NULL,
            "hash"	TEXT NOT NULL,
            "mod_timestamp"	INTEGER,
            PRIMARY KEY("id" AUTOINCREMENT)
        );
        """)
    cur.close()
    sqlcon.commit()
    sqlcon.close()


def get_latest_hash_for_file(sqlcon, file: FileBackuped) -> FileBackuped:
    cur = sqlcon.cursor()
    data = ({"filepath": file.filepath})
    res = cur.execute(
        "SELECT hash, mod_timestamp FROM file_history WHERE filepath=:filepath ORDER BY timestamp DESC", data)
    last_hash_row = res.fetchone()
    cur.close()
    if last_hash_row == None:
        return None
    file2 = FileBackuped()
    file2.filepath = file.filepath
    file2.timestamp = file.timestamp
    file2.hash = last_hash_row[0]
    file2.mod_timestamp = last_hash_row[1]
    return file2


def insert_file_backup(sqlcon, file: FileBackuped):
    cur = sqlcon.cursor()
    data = ({"filepath": file.filepath, "timestamp": file.timestamp,
            "hash": file.hash, "mod_timestamp": file.mod_timestamp})
    cur.execute("INSERT INTO file_history (filepath, timestamp, hash, mod_timestamp) VALUES (:filepath, :timestamp, :hash, :mod_timestamp)", data)
    cur.close()
    sqlcon.commit()


def copy_file_locked(filepath_src, filepath_dst):
    f_src = open(filepath_src, "r+b")
    f_dst = open(filepath_dst, "wb")
    f_dst.write(f_src.read())
    f_src.close()
    f_dst.close()


def search_files(directory, root_directory) -> list[FileBackuped]:
    files_arr = []
    for filename in os.listdir(os.path.join(root_directory, directory)):
        fabs = os.path.join(root_directory, directory, filename)
        if os.path.isfile(fabs):
            file = FileBackuped()
            file.filepath = os.path.join(directory, filename)
            file.mod_timestamp = os.stat(fabs).st_mtime_ns
            files_arr.append(file)
        if os.path.isdir(fabs):
            files_arr += search_files(os.path.join(directory,
                                      filename), root_directory)
    return files_arr


def md5_of_file(filepath, filename):
    f = open(filepath, "rb")
    result = hashlib.md5(filename.encode()+f.read())
    hashres = result.hexdigest()
    f.close()
    return hashres


def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


def get_all_files_in_db(sqlcon):
    files = []
    sqlcon.row_factory = dict_factory
    cur = sqlcon.cursor()
    res = cur.execute(
        "SELECT filepath, hash, mod_timestamp FROM file_history").fetchall()
    cur.close()
    for f in res:
        file = FileBackuped()
        file.filepath = f["filepath"]
        file.hash = f["hash"]
        file.mod_timestamp = f["mod_timestamp"]
        files.append(file)
    sqlcon.row_factory = None
    return files


def get_changed_files(sqlcon, files: list[FileBackuped], rootdir) -> list[FileBackuped]:
    changed_files = []
    for f in files:
        latest_hash = get_latest_hash_for_file(sqlcon, f)
        if latest_hash == None:
            f.hash = md5_of_file(os.path.join(rootdir, f.filepath), f.filepath)
            changed_files.append(f)
        elif latest_hash.mod_timestamp != f.mod_timestamp:
            hash = md5_of_file(os.path.join(rootdir, f.filepath), f.filepath)
            if hash != latest_hash.hash:
                f.hash = hash
                changed_files.append(f)
    return changed_files
