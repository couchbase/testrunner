#!/bin/sh -ex
easy_install -U zc.buildout==1.7.0
buildout -t 180 -q
