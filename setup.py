import os
import sys
import setuptools

# Some filesystems don't support hard links. Use the power of
# monkeypatching to overcome the problem.
import os, shutil
os.link = shutil.copy


if not os.path.exists("puka/spec.py"):
    print >> sys.stderr, "Run 'make' first."
    sys.exit(1)


setuptools.setup(name='puka',
      version=file('VERSION').read().strip(),
      description='Puka - the opinionated RabbitMQ client',
      author='Marek Majkowski',
      author_email='marek@popcount.org',
      url='http://github.com/majek/puka#readme',
      packages=['puka'],
      platforms=['any'],
      license='MIT',
      classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Topic :: Software Development :: Libraries :: Python Modules',
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 2.6",
        "Programming Language :: Python :: 2.7",
        ],
      zip_safe = True,
      )
