from distutils.core import setup
setup(
  name = 'es_util',
  packages = ['es_util'],
  version = '0.08',
  license='MIT',
  description = 'Utility module containing helper methods for elasticsearch',
  author = 'Ashok M B',
  author_email = 'mukunduashok@gmail.com',
  url = 'https://github.com/mukunduashok/elasticsearch_util',
  download_url = 'https://github.com/mukunduashok/es_util/archive/v0.08.tar.gz',
  keywords = ['elasticsearch', 'ES', 'elastic search'],
  install_requires=[
        "elasticsearch"
      ],
  classifiers=[
    'Development Status :: 3 - Alpha',
    'Intended Audience :: Developers',
    'Topic :: Software Development :: Build Tools',
    'License :: OSI Approved :: MIT License',
    'Programming Language :: Python :: 3.7',
  ],
)