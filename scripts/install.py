import sys
import argparse
import os
from string import Template


def main():
    parser = argparse.ArgumentParser(
        description="Interpolates and installs configuration files.")
    parser.add_argument("--source", help="The source template path", type=str)
    parser.add_argument("--target", help="The target file path", type=str)
    args = parser.parse_args(sys.argv[1:])

    with open(args.source, "r") as src:
        tmpl = Template(src.read())
        results = tmpl.substitute(os.environ)

    with open(args.target, "w") as dest:
        dest.write(results)

if __name__ == "__main__":
    main()
