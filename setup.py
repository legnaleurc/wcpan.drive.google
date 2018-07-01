from setuptools import setup


setup(
        name='wcpan.drive.google',
        version='2.1.4',
        author='Wei-Cheng Pan',
        author_email='legnaleurc@gmail.com',
        description='Asynchronous Google Drive API.',
        url='https://github.com/legnaleurc/wcpan.drive.google',
        packages=[
            'wcpan.drive.google',
        ],
        python_requires='>= 3.6',
        install_requires=[
            'PyYAML ~= 3.12',
            'aiohttp ~= 3.3.2',
            'async-exit-stack ~= 1.0.1',
            'wcpan.logger ~= 1.2.3',
        ],
        extras_require={
            'tests': [
                'wcpan.worker ~= 3.0.1',
            ],
        },
        classifiers=[
            'Programming Language :: Python :: 3 :: Only',
            'Programming Language :: Python :: 3.6',
        ])
