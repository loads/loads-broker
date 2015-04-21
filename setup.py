import os
from setuptools import setup, find_packages
from loadsbroker import __version__


here = os.path.abspath(os.path.dirname(__file__))

with open(os.path.join(here, 'README.rst')) as f:
    README = f.read()

# FIXME: docker-py broke Python 3 compatibility in 0.5.1 and after,
#        See: https://github.com/docker/docker-py/issues/342
requires = ['cornice', 'docker-py==0.5.0', 'boto', 'paramiko', 'sqlalchemy',
            'tornado', 'requests', 'influxdb>=2.0.1']
tests_require = ['nose', 'nose-cov', 'flake8', 'moto', 'freezegun']


setup(name='loads-broker',
      version=__version__,
      packages=find_packages(),
      include_package_data=True,
      description='The Loads agent',
      long_description=README,
      zip_safe=False,
      license='APLv2.0',
      classifiers=[
        "Programming Language :: Python",
      ],
      install_requires=requires,
      author='Mozilla Services',
      author_email='services-dev@mozilla.org',
      url='https://github.com/loads/loads-broker',
      tests_require=tests_require,
      test_suite='nose.collector',
      entry_points="""
      [console_scripts]
      loads-broker = loadsbroker.main:main
      loads = loadsbroker.client:main
      """)
