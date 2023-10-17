import argparse

parser = argparse.ArgumentParser(
                    prog='backup',
                    description='Iterative backup')

parser.add_argument('-c', '--count', required=True)

args = parser.parse_args()
print(args.count)