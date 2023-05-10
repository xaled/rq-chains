#!/usr/bin/env python3
import os

from setup import VERSION

os.system(f"python3 setup.py sdist "
          f"&& twine upload dist/rq-chains-{VERSION}.tar.gz")
