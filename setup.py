from distutils.core import setup
setup(
  name = 'elasticsearch_util',
  packages = ['elasticsearch_util'],
  version = '0.1',
  license='MIT',
  description = 'Utility module containing helper methods for elasticsearch',
  author = 'Ashok M B',
  author_email = 'mukunduashok@gmail.com',
  url = 'https://github.com/mukunduashok/elasticsearch_util',
  download_url = 'https://github.com/mukunduashok/elasticsearch_util/archive/v_01.tar.gz',
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