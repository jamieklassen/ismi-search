#! /usr/bin/env sh
hg clone http://bitbucket.org/mchaput/whoosh
cd whoosh
python setup.py install
cd ..
rm -rf whoosh
pip install tornado