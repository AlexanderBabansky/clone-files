import unittest
import tempfile
import sqlite3
import threading
import time

from main import *


class TestStringMethods(unittest.TestCase):
    def test_create_db(self):
        with tempfile.TemporaryDirectory() as dir:
            db_path = os.path.join(dir, "db.db")
            create_empty_db(db_path)
            sqlcon = sqlite3.connect(db_path)
            cur = sqlcon.cursor()
            cur.execute(
                "SELECT id, filepath, timestamp, hash, mod_timestamp FROM file_history")
            cur.close()
            sqlcon.close()

    def test_latest_file(self):
        with tempfile.TemporaryDirectory() as dir:
            db_path = os.path.join(dir, "db.db")
            create_empty_db(db_path)
            sqlcon = sqlite3.connect(db_path)
            self.assertEqual(get_latest_hash_for_file(sqlcon, "myfile"), None)
            insert_file_backup(sqlcon, "myfile", "123",
                               timestamp=5, mod_timestamp=6)
            self.assertEqual(get_latest_hash_for_file(sqlcon, "myfile"), {
                             "hash": "123", "mod_timestamp": 6})
            sqlcon.close()

    def test_copy_file(self):
        with tempfile.TemporaryDirectory() as dir:
            file_path_src = os.path.join(dir, "file")
            file_path_dst = os.path.join(dir, "file_dst")
            f_desc = open(file_path_src, "wb")
            f_desc.write("hello".encode())
            f_desc.close()
            copy_file_locked(file_path_src, file_path_dst)
            f_desc = open(file_path_dst, "r")
            self.assertEqual(f_desc.read(), "hello")
            f_desc.close()

    def test_search_files(self):
        with tempfile.TemporaryDirectory() as dir:
            file_desc = open(os.path.join(dir, "file1"), "x")
            file_desc.close()
            file_desc = open(os.path.join(dir, "file2"), "x")
            file_desc.close()
            os.mkdir(os.path.join(dir, "dir"))
            file_desc = open(os.path.join(dir, "dir", "file3"), "x")
            file_desc.close()
            files = search_files("", dir)
            self.assertEqual(files[0]["filepath"],
                             os.path.join("dir", "file3"))
            self.assertEqual(files[1]["filepath"], "file1")
            self.assertEqual(files[2]["filepath"], "file2")
            self.assertNotEqual(files[0]["mod_timestamp"], 0)
            self.assertNotEqual(files[1]["mod_timestamp"], 0)
            self.assertNotEqual(files[2]["mod_timestamp"], 0)

    def test_md5_file(self):
        with tempfile.TemporaryDirectory() as dir:
            filepath = os.path.join(dir, "file1")
            file_desc = open(filepath, "wb")
            file_desc.write("hello".encode())
            file_desc.close()
            self.assertEqual(md5_of_file(filepath),
                             "5d41402abc4b2a76b9719d911017c592")

    def test_get_files_db(self):
        with tempfile.TemporaryDirectory() as dir:
            db_path = os.path.join(dir, "db.db")
            create_empty_db(db_path)
            sqlcon = sqlite3.connect(db_path)
            insert_file_backup(sqlcon, "myfile", "hash1",
                               timestamp=5, mod_timestamp=6)
            insert_file_backup(sqlcon, "myfile2", "hash2",
                               timestamp=7, mod_timestamp=8)
            res = get_all_files_in_db(sqlcon)
            self.assertEqual(len(res), 2)
            self.assertEqual(res[0]["filepath"], "myfile")
            self.assertEqual(res[0]["hash"], "hash1")
            self.assertEqual(res[0]["mod_timestamp"], 6)
            self.assertEqual(res[1]["filepath"], "myfile2")
            self.assertEqual(res[1]["hash"], "hash2")
            self.assertEqual(res[1]["mod_timestamp"], 8)
            sqlcon.close()
    def test_changed_files(self):
        with tempfile.TemporaryDirectory() as dir:
            db_path = os.path.join(dir, "db.db")
            files_path = os.path.join(dir, "files")
            os.mkdir(files_path)
            file_desc = open(os.path.join(files_path, "file1"), "x")
            file_desc.write("hello")
            file_desc.close()
            create_empty_db(db_path)
            sqlcon = sqlite3.connect(db_path)
            files = search_files("", files_path)
            print(files)
            changed_files = get_changed_files(sqlcon, files, files_path)
            self.assertEqual(len(changed_files), 1)
            for f in changed_files:
                insert_file_backup(sqlcon, f["filepath"], f["hash"],
                                timestamp=1, mod_timestamp=f["mod_timestamp"])
            changed_files = get_changed_files(sqlcon, files, files_path)
            self.assertEqual(len(changed_files), 0)
            file_desc = open(os.path.join(files_path, "file1"), "wb")
            file_desc.write("hello2".encode())
            file_desc.close()
            files = search_files("", files_path)
            changed_files = get_changed_files(sqlcon, files, files_path)
            print(changed_files)
            print(files)
            sqlcon.close()

unittest.main()
