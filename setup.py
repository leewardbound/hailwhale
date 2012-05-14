from setuptools import setup, find_packages
import re

DESCRIPTION = "hailwhale: fast multidimensional counting, with django support"

LONG_DESCRIPTION = None
try:
    LONG_DESCRIPTION = open('README.rst').read()
except:
    pass

CLASSIFIERS = [
    'Development Status :: 4 - Beta',
    'Intended Audience :: Developers',
    'Operating System :: OS Independent',
    'Programming Language :: Python',
    'Topic :: Database',
    'Topic :: Software Development :: Libraries :: Python Modules',
    'License :: OSI Approved :: BSD License',
]

setup(name='hailwhale',
      packages=find_packages(exclude=('tests', 'tests.*')),
      author='Leeward Bound Corp',
      author_email='code@lwb.co',
      url='http://www.github.com/linked/hailwhale',
      description=DESCRIPTION,
      long_description=LONG_DESCRIPTION,
      platforms=['any'],
      classifiers=CLASSIFIERS,
      install_requires = ['bottle', 'redis', 'times', 'pycrypto'],
      version='1.3.3',
)
