#!/usr/bin/env python3

import sys
import re


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
        # Read the file and check non-empty lines
        with open(file_name, "r", encoding="utf-8") as file:
            for line in file:
                line = line.strip()
                if line and not is_valid_line(line):
                    print(f'Invalid entry in change log: "{line}"')
                    sys.exit(1)
    else:
        print("Please provide a file name as a command-line argument.")
        sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    main()
