from setuptools import setup


setup(
        name='wcpan.drive.google',
        version='2.0.0.dev1',
        author='Wei-Cheng Pan',
        author_email='legnaleurc@gmail.com',
        description='Asynchronous Google Drive API.',
        url='https://github.com/legnaleurc/wcpan.drive.google',
        packages=[
            'wcpan.drive.google',
        ],
        install_requires=[
            'PyYAML',
            'aiohttp >= 3',
            'wcpan.logger',
            'wcpan.worker >= 1.3',
        ],
        classifiers=[
            'Programming Language :: Python :: 3 :: Only',
            'Programming Language :: Python :: 3.5',
        ])
