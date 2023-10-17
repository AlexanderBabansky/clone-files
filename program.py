import argparse
from main import *
import time

parser = argparse.ArgumentParser(
    prog='backup',
    description='Iterative backup')

parser.add_argument('-d', '--database', required=True, type=str)
parser.add_argument('-f', '--sourceFolder', required=True, type=str)
parser.add_argument('-b', '--backupFolder', required=True, type=str)

args = parser.parse_args()

create_empty_db(args.database)
sqlcon = sqlite3.connect(args.database)

sourceFolder = args.sourceFolder
backupFolder = args.backupFolder

files = search_files("", sourceFolder)
changed_files = get_changed_files(sqlcon, files, sourceFolder)

if len(changed_files) == 0:
    print("Latest state")
else:
    print("Backuping ", len(changed_files), " files")

for c in changed_files:
    print("File: ", c.filepath)
    try:
        if not os.path.exists(os.path.join(backupFolder, c.hash)):
            os.mkdir(os.path.join(backupFolder, c.hash))
            copy_file_locked(os.path.join(sourceFolder, c.filepath), os.path.join(
                backupFolder, c.hash, os.path.basename(c.filepath)))
        c.timestamp = time.time_ns()
        insert_file_backup(sqlcon, c)
    except PermissionError:
        os.rmdir(os.path.join(backupFolder, c.hash))
        print("File ", c.filepath, " is locked")

sqlcon.close()
