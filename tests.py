import unittest
import tempfile
import sqlite3
import threading
import time

from main import *


def elements_in_list_equal(l, v):
    res = 0
    for e in l:
        if e == v:
            res += 1
    return res


class TestBackupFile:
    def __init__(self):
        self.filepath = None
        self.content = None


def fill_directory(files_path, files: list[TestBackupFile]):
    for f in files:
        splited_path = os.path.split(f.filepath)
        os.makedirs(os.path.join(files_path, splited_path[0]), exist_ok=True)
        file_desc = open(os.path.join(files_path, f.filepath), "w")
        file_desc.write(f.content)
        file_desc.close()


def get_test_files1() -> list[TestBackupFile]:
    files = []
    f1 = TestBackupFile()
    f1.content = "hello1"
    f1.filepath = "file1"
    f2 = TestBackupFile()
    f2.content = "hello2"
    f2.filepath = "file2"
    f3 = TestBackupFile()
    f3.content = "hello2"
    f3.filepath = "dir/file2"
    files.append(f1)
    files.append(f2)
    files.append(f3)
    return files


def get_test_files2() -> list[TestBackupFile]:
    files = []
    f2 = TestBackupFile()
    f2.content = "hello3"
    f2.filepath = "file2"
    files.append(f2)
    return files


def backup_temp_twice(dir, db_path, backup_path, files_path):
    file_orig_path = os.path.join(files_path, "file1")
    file_orig_path2 = os.path.join(files_path, "file2")
    os.mkdir(files_path)
    os.mkdir(backup_path)
    file_desc = open(file_orig_path, "x")
    file_desc.write("hello")
    file_desc.close()
    file_desc = open(file_orig_path2, "x")
    file_desc.write("hello")
    file_desc.close()
    create_empty_db(db_path)
    files = search_files("", files_path)
    sqlcon = sqlite3.connect(db_path)
    backup_changed_files(sqlcon, files, files_path, backup_path)
    file_desc = open(file_orig_path, "w")
    file_desc.write("hello2")
    file_desc.close()
    files = search_files("", files_path)
    backup_changed_files(sqlcon, files, files_path, backup_path)
    sqlcon.close()


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
            file = FileBackuped()
            file.filepath = "myfile"
            file.mod_timestamp = 6
            file.hash = "123"
            file.timestamp = 5
            self.assertEqual(get_latest_hash_for_file(
                sqlcon, file.filepath), None)
            insert_file_backup(sqlcon, file)
            latest_hash = get_latest_hash_for_file(sqlcon, file.filepath)
            self.assertEqual(latest_hash.hash, "123")
            self.assertEqual(latest_hash.mod_timestamp, 6)
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
            self.assertEqual(len(files), 3)
            filepathes_test = ["file1", "file2", os.path.join("dir", "file3")]
            for f in files:
                self.assertEqual(elements_in_list_equal(
                    filepathes_test, f.filepath), 1)
            self.assertNotEqual(files[0].mod_timestamp, 0)
            self.assertNotEqual(files[1].mod_timestamp, 0)
            self.assertNotEqual(files[2].mod_timestamp, 0)

    def test_md5_file(self):
        with tempfile.TemporaryDirectory() as dir:
            filepath = os.path.join(dir, "file1")
            file_desc = open(filepath, "wb")
            file_desc.write("hello".encode())
            file_desc.close()
            self.assertEqual(md5_of_file(filepath, "file1"),
                             "730a1649974bb9908f1298fa37e9afbd")
            file_desc = open(filepath, "wb")
            file_desc.close()
            self.assertEqual(md5_of_file(filepath, "file1"),
                             "826e8142e6baabe8af779f5f490cf5f5")

    def test_get_files_db(self):
        with tempfile.TemporaryDirectory() as dir:
            db_path = os.path.join(dir, "db.db")
            create_empty_db(db_path)
            sqlcon = sqlite3.connect(db_path)
            file = FileBackuped()
            file.filepath = "myfile"
            file.hash = "hash1"
            file.timestamp = 5
            file.mod_timestamp = 6
            insert_file_backup(sqlcon, file)
            file.filepath = "myfile2"
            file.hash = "hash2"
            file.timestamp = 7
            file.mod_timestamp = 8
            insert_file_backup(sqlcon, file)
            res = get_all_files_in_db(sqlcon)
            self.assertEqual(len(res), 2)
            self.assertEqual(res[0].filepath, "myfile")
            self.assertEqual(res[0].hash, "hash1")
            self.assertEqual(res[0].mod_timestamp, 6)
            self.assertEqual(res[1].filepath, "myfile2")
            self.assertEqual(res[1].hash, "hash2")
            self.assertEqual(res[1].mod_timestamp, 8)
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
            changed_files = get_changed_files(sqlcon, files, files_path)
            self.assertEqual(len(changed_files), 1)
            for f in changed_files:
                f.timestamp = 1
                insert_file_backup(sqlcon, f)
            changed_files = get_changed_files(sqlcon, files, files_path)
            self.assertEqual(len(changed_files), 0)
            file_desc = open(os.path.join(files_path, "file1"), "wb")
            file_desc.write("hello".encode())
            file_desc.close()
            files = search_files("", files_path)
            changed_files = get_changed_files(sqlcon, files, files_path)
            self.assertEqual(len(changed_files), 0)
            file_desc = open(os.path.join(files_path, "file1"), "wb")
            file_desc.write("hello2".encode())
            file_desc.close()
            files = search_files("", files_path)
            changed_files = get_changed_files(sqlcon, files, files_path)
            self.assertEqual(len(changed_files), 1)
            sqlcon.close()

    def test_backup(self):
        with tempfile.TemporaryDirectory() as dir:
            files_path = os.path.join(dir, "files")
            backup_path = os.path.join(dir, "backup")
            file_orig_path = os.path.join(files_path, "file1")
            os.mkdir(files_path)
            os.mkdir(backup_path)
            file_desc = open(file_orig_path, "x")
            file_desc.write("hello")
            file_desc.close()
            files = search_files("", files_path)
            db_path = os.path.join(dir, "db.db")
            create_empty_db(db_path)
            sqlcon = sqlite3.connect(db_path)
            backup_changed_files(sqlcon, files, files_path, backup_path)
            sqlcon.close()
            file_backup_path = os.path.join(
                backup_path, md5_of_file(file_orig_path, "file1"), "file1")
            f_desc = open(file_backup_path, "r")
            self.assertEqual(f_desc.read(), "hello")
            f_desc.close()

    def test_check_integrity(self):
        with tempfile.TemporaryDirectory() as dir:
            db_path = os.path.join(dir, "db.db")
            backup_path = os.path.join(dir, "backup")
            files_path = os.path.join(dir, "files")
            test_files = get_test_files1()
            fill_directory(files_path, test_files)
            os.mkdir(backup_path)
            create_empty_db(db_path)
            files = search_files("", files_path)
            sqlcon = sqlite3.connect(db_path)
            backup_changed_files(sqlcon, files, files_path, backup_path)
            problems = check_intergrity(sqlcon, backup_path)
            self.assertEqual(len(problems), 0)
            fill_directory(files_path, get_test_files2())
            problems = check_intergrity(sqlcon, backup_path)
            self.assertEqual(len(problems), 1)
            sqlcon.close()

    def test_restore(self):
        with tempfile.TemporaryDirectory() as dir:
            db_path = os.path.join(dir, "db.db")
            backup_path = os.path.join(dir, "backup")
            files_path = os.path.join(dir, "files")
            backup_temp_twice(dir, db_path, backup_path, files_path)
            sqlcon = sqlite3.connect(db_path)
            files_to_restore = get_newest_files_older_timestamp(
                sqlcon, 3000000000000000000)
            self.assertEqual(len(files_to_restore), 2)
            self.assertEqual(
                files_to_restore[0].hash, "205f20308510ac85df567e7ba1d542b3")
            self.assertEqual(
                files_to_restore[1].hash, "3f931558348ea36e6ef947a48e8c86c2")
            sqlcon.close()


if __name__ == '__main__':
    unittest.main()
