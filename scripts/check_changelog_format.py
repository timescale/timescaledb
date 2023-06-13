#!/usr/bin/env python3

import sys
import re
import os


# Check if a line matches any of the specified patterns
def is_valid_line(line):
    patterns = [r"^Fixes:\s*.*$", r"^Implements:\s*.*$", r"^Thanks:\s*.*$"]
    for pattern in patterns:
        if re.match(pattern, line):
            return True
    return False


def main():
    # Get the file name from the command line argument
    if len(sys.argv) > 1:
        file_name = sys.argv[1]
        pr_number_seen = False
        pr_num_str = f'#{os.environ["PR_NUMBER"]} '
        # Read the file and check non-empty lines
        with open(file_name, "r", encoding="utf-8") as file:
            for line in file:
                line = line.strip()
                pr_number_seen |= pr_num_str in line
                if line and not is_valid_line(line):
                    print(f'Invalid entry in change log: "{line}"')
                    sys.exit(1)
        if not pr_number_seen:
            print(
                f'Expected that the changelog contains a reference to the PR: "{pr_num_str}"'
            )
            sys.exit(1)
    else:
        print("Please provide a file name as a command-line argument.")
        sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    main()
