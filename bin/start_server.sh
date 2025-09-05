#!/usr/bin/env bash
cat "$(dirname "$0")/SCRAPY_SERVER.txt"
scrapyd
