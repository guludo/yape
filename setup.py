import pathlib

import setuptools

setuptools.setup(
    name='yape',
    version='0.1.0',
    url='https://github.com/guludo/yape',
    description='Yape - Yet another pipeline executor',
    long_description=(pathlib.Path(__file__).parent / 'README.rst').read_text(),
    long_description_content_type='text/x-rst',
    packages=['yape'],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Mozilla Public License 2.0 (MPL 2.0)',
        'Programming Language :: Python :: 3',
    ],
    keywords='pipeline',
)
