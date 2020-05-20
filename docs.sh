#!/bin/bash

# Get godoc from windows or from linux
win=true

hash godoc.exe 2>/dev/null || {
    win=false
}

if [ "$win" = true ]; then
    godoc=godoc.exe
    python=python.exe
fi

# Check if the godoc is available in linux
if [ "$win" = false ]; then
    linux=true
    hash godoc 2>/dev/null || {
        linux=false
    }
    if [ "$linux" = true ]; then
        godoc=godoc
        python=python
    fi
fi

if [ "$win" = true ] || [ "$linux" = true ]; then
    # Run the godoc and run the server
    export python="$python"
    bash -c 'sleep 5; "$python" -mwebbrowser http://localhost:8080/pkg/github.com/raralabs/canal/; exit' &

    echo "Generating Documentation. Please Wait."
    "$godoc" -http=:8080
else
    echo "godoc not found in system path"
fi
