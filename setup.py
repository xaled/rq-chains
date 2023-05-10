#!/usr/bin/env python3
from distutils.core import setup

# import os

VERSION = '0.1.5'  # major.minor.fix

# def package_files(directory):
#     paths = []
#     for (path, directories, filenames) in os.walk(directory):
#         for filename in filenames:
#             if not path.endswith('__pycache__') and not filename.endswith(".pyc"):
#                 paths.append(os.path.relpath(os.path.join(path, filename), directory))
#     return paths


with open('README.md', 'r') as f:
    long_description = f.read()

with open('LICENSE', 'r') as f:
    license_text = f.read()

# error: does not get copied to the package tar.gz
# with open('requirements.txt') as f:
#     required = f.read().splitlines()

if __name__ == "__main__":
    # extra_files = package_files('rq_chains/')

    # print extra_files

    setup(
        name='rq-chains',
        version=VERSION,
        description='RQ Chains is a Python library that extends RQ (Redis Queue) with a publisher-subscriber model'
                    ' for job chains. By Khalid Grandi (github.com/xaled).',
        long_description=long_description,
        long_description_content_type='text/markdown',
        keywords='library rq redis pubsub',
        author='Khalid Grandi',
        author_email='kh.grandi@gmail.com',
        classifiers=[
            'License :: OSI Approved :: MIT License',
            'Operating System :: OS Independent',
            'Programming Language :: Python',
        ],
        license='MIT',
        url='https://github.com/xaled/rq-chains',
        install_requires=['rq>1.10,<2', 'redis>=3.5.0'],
        python_requires='>=3',
        packages=['rq_chains'],
        package_data={'': ['LICENSE', 'requirements.txt', 'README.md']},
    )
