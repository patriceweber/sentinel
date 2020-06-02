#!/usr/bin/env python

import sys
import setuptools


if __name__ == '__main__':
    
    try:
        from setuptools import setup
    
    except:
        from ez_setup import use_setuptools
        use_setuptools()
        from setuptools import setup
    
    
    with open("docs/README.txt", "r") as fh:
        readme = fh.read()
    
    classifiers = [
        'Development Status :: 2 - Beta',
        'Intended Audience :: Darwin Centre for Bushfire Research',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: GNU License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Scientific/Remote Sensing :: GIS']
    
    # We'd like to let debian install the /etc/tilecache.cfg,
    # but put them in tilecache/tilecache.cfg using setuptools
    # otherwise.
    
    extra = { }
    extra['data_files']=[('sentdownloader', ['configs/params.conf']),('sentdownloader',['configs/kakadu.conf']),
                         ('sentdownloader', ['sentinelDownloader.py'])]
    
    setuptools.setup(
    	name='sentdownloader',
    	version='1.1',
    	description='Sentinel-2 Tile downloader',
    	author='SentDownloader original contributors, Patrice Weber [DCBR], 2019',
    	author_email='patrice.weber.fr@gmail.com',
    	url='https://bushfiresresearch.wordpress.com/',
    	long_description=readme,
    	packages=setuptools.find_packages(),
        scripts=[],
        install_requires=['tabulate>=0.8.6','sentinelsat>=0.13', 'urllib3>=1.25.6','six>=1.15','Pillow>=7.1.2','requests>=2.22.0'],
    	zip_safe=True,
    	python_requires='>=3.0',
    	license="GNU",
    	classifiers=classifiers,
        **extra)
