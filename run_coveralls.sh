#!/bin/bash

if [ "$TRAVIS" != "true" ]; then
    echo "Not running coveralls because we are not in travis-ci"
    exit 0
fi

if [ "$TRAVIS_BRANCH" != "master" ]; then
    echo "Not running coveralls because we are on branch '$TRAVIS_BRANCH'"
    exit 0
fi

if [ "$TRAVIS_PULL_REQUEST" != "false" ]; then
    echo "Not running coveralls because this build is for a pull request"
    exit 0
fi

if [ "$TOXENV" != "py27" ]; then
    echo "Not running coveralls because we are in the build environment '$TOX_ENV_NAME'"
    exit 0
fi

coveralls
