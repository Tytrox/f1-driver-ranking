#!/usr/bin/env python3
import sys
from venv import create
from subprocess import run
from os.path import abspath


def create_venv(result_dir, requirements):
    create(result_dir, with_pip=True)

    run(["bin/pip3", "install", "--upgrade", "pip"], cwd=result_dir)
    run(["bin/pip3", "install", "-r", abspath(requirements)], cwd=result_dir)


def usage():
    print(f"Usage: {sys.argv[0]} <venv_name> <requirements_file>")


if __name__ == "__main__":
    if len(sys.argv) < 3:
        usage()
    else:
        create_venv(sys.argv[1], sys.argv[2])
