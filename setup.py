import os
import sys
import setuptools

if not os.path.exists("puka/spec.py"):
    print >> sys.stderr, "Run 'make' first."
    sys.exit(1)


setuptools.setup(name='puka',
      version='0.0.1',
      description='Puka - the opinionated RabbitMQ client',
      author='Marek Majkowski',
      author_email='marek@rabbitmq.com',
      url='http://github.com/majek/puka#readme',
      packages=['puka'],
      license='MIT',
      classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Topic :: Software Development :: Libraries :: Python Modules',
        ],
      )
