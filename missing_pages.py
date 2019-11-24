"""Summarize missing pages
"""
from pprint import pprint

FILENAME = "missing_pages_2019-11-22.csv"

def main():
    """Print summary to console
    """
    missing = {} # keys = missing page URLs, values = list of pages linking to it
    with open(FILENAME) as fhandle:
        for line in fhandle.readlines():
            destination, source = line.strip().split(",")
            if destination in missing:
                missing[destination].append(source)
            else:
                missing[destination] = [source]

    destinations = sorted(missing.keys())
    for destination in destinations:
        print(f"\nThis page does not exist: {destination}")
        print(f"But there are links to it from the following pages:")
        for source in sorted(missing[destination]):
            print(source)

if __name__ == "__main__":
    main()
