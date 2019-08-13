import os.path as op

from setuptools import setup


with open(op.join(op.dirname(__file__), './README.md')) as fin:
    long_description = fin.read()


setup(
        name='wcpan.drive.google',
        version='4.0.7',
        author='Wei-Cheng Pan',
        author_email='legnaleurc@gmail.com',
        description='Asynchronous Google Drive API.',
        long_description=long_description,
        long_description_content_type='text/markdown',
        url='https://github.com/legnaleurc/wcpan.drive.google',
        packages=[
            'wcpan.drive.google',
        ],
        python_requires='>= 3.7',
        install_requires=[
            'PyYAML >= 3.12',
            'aiohttp >= 3.4.4',
            'arrow >= 0.12.1',
            'wcpan.logger >= 1.2.3',
            'wcpan.worker >= 4.1.2',
        ],
        classifiers=[
            'Programming Language :: Python :: 3 :: Only',
            'Programming Language :: Python :: 3.7',
        ])
