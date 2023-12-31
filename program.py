import argparse
from main import *
import time

parser = argparse.ArgumentParser(
    prog='backup',
    description='Iterative backup')

parser.add_argument('-d', '--database', required=True, type=str)
parser.add_argument('-f', '--sourceFolder', required=False, type=str)
parser.add_argument('-b', '--backupFolder', required=True, type=str)
parser.add_argument('-r', '--restoreFolder', required=False, type=str)
parser.add_argument('-a', '--action', required=True,
                    type=str, choices=["backup", "integrity", "restore"])

args = parser.parse_args()

if args.action == "backup":
    create_empty_db(args.database)
    sqlcon = sqlite3.connect(args.database)

    sourceFolder = args.sourceFolder
    if not sourceFolder:
        print("No source folder")
        exit(1)
    backupFolder = args.backupFolder

    files = search_files("", sourceFolder)
    if len(files) == 0:
        print("No files")
    else:
        print("Found", len(files), "files")

    backup_changed_files(sqlcon, files, sourceFolder, backupFolder, True)
    sqlcon.close()
elif args.action == "integrity":
    sqlcon = sqlite3.connect(args.database)
    backupFolder = args.backupFolder
    problems = check_intergrity(sqlcon, backupFolder)
    if len(problems) == 0:
        print("Integrity is OK")
    else:
        print("Problems with next files:")
        for p in problems:
            print(p)
    sqlcon.close()
elif args.action == "restore":
    sqlcon = sqlite3.connect(args.database)
    if not args.restoreFolder:
        print("No restore folder set")
        exit(1)
    backupFolder = args.backupFolder
    restoreFolder = args.restoreFolder
    latest_timestamp = time.time_ns()
    files_to_restore = get_newest_files_older_timestamp(
        sqlcon, latest_timestamp)
    print("Restore files:", len(files_to_restore))
    restore_files(files_to_restore, backupFolder, restoreFolder, True)
    sqlcon.close()
