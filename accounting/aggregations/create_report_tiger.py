# a simple wrapper script to separate tiger host queries
# from accounting3000 queries (without having to use the commandline arg)
#
# to avoid copying all the code this script just calls the other

import sys
import subprocess

def main():
    subprocess.run(["python3", "create_report_accounting3000.py", "--es-use-tiger"] + sys.argv[1:])

if __name__ == "__main__":
    main()
