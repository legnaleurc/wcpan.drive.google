from setuptools import setup


setup(
        name='wcpan.drive.google',
        version='2.1.0',
        author='Wei-Cheng Pan',
        author_email='legnaleurc@gmail.com',
        description='Asynchronous Google Drive API.',
        url='https://github.com/legnaleurc/wcpan.drive.google',
        packages=[
            'wcpan.drive.google',
        ],
        python_requires='>= 3.6',
        install_requires=[
            'PyYAML ~= 3.0',
            'aiohttp ~= 3.0.0',
            'async_exit_stack ~= 1.0.0',
            'wcpan.logger ~= 1.0.0',
        ],
        extras_require={
            'tests': [
                'wcpan.worker ~= 3.0.0',
            ],
        },
        classifiers=[
            'Programming Language :: Python :: 3 :: Only',
            'Programming Language :: Python :: 3.6',
        ])
