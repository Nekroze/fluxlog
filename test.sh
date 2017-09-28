#!/bin/sh
coveragedir=.coverage
coverageout=$coveragedir/coverage.out
coveragehtm=$coveragedir/coverage.html

if [ "$1" == "TEST" ]; then
    set -euf

    go test -v -short -bench . -cover -coverprofile $coverageout
    go tool cover -html=$coverageout -o $coveragehtm
elif [ "$1" == "DEMO" ]; then
    go test -v -bench . -benchtime 1m
else
    set -euf

    [ -f $coveragehtm ] && rm -f $coveragehtm
    [ -f $coverageout ] && rm -f $coverageout
    mkdir -p $coveragedir
    touch $coveragehtm $coverageout

    docker-compose build
    docker-compose up --abort-on-container-exit storage test
    [ "$(docker-compose ps -q | xargs docker inspect -f '{{ .State.ExitCode }}' | grep -v 0 | wc -l | tr -d ' ')" -eq 0 ]

    set +e
    [ -f $coveragehtm ] && xdg-open $coveragehtm
fi
