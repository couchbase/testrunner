#!/bin/sh
easy_install zc.buildout
buildout bootstrap
./bin/buildout &> /dev/null
