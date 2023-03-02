import pathlib

import setuptools

setuptools.setup(
    name='yape',
    version='0.3.0',
    url='https://github.com/guludo/yape',
    description='Yape - Yet another pipeline executor',
    long_description=(pathlib.Path(__file__).parent / 'README.rst').read_text(),
    long_description_content_type='text/x-rst',
    packages=['yape'],
    package_data={
        'yape': ['py.typed'],
    },
    entry_points={
        'console_scripts': [
            'yape = yape.__main__:run',
        ],
    },
    install_requires=[
        'argparse-subdec>=0.2.1,<1.0',
        'dill~=0.3.4',
    ],
    extras_require={
        'dev': [
            'mypy>=0.910,<1.0',
            'pytest~=7.1.3',
        ],
    },
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Mozilla Public License 2.0 (MPL 2.0)',
        'Programming Language :: Python :: 3',
    ],
    keywords='pipeline',
)
