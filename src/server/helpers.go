package main

import (
	"fmt"
	"log"
	"os"
	"strconv"
)

// Error logs the given error
func Error(err ...interface{}) {
	log.Println(ERROR + " " + fmt.Sprint(err...))
}

// Fail logs the given error and exits with status 1
func Fatal(err ...interface{}) {
	log.Fatalln(ERROR + " " + fmt.Sprint(err...))
}

// setArgsPositional parses the first three command line arguments into ID,
// NUM_PROCS, and PORT respectively. It should be called if no arguments were
// provided via flags.
func setArgsPositional() {
	getIntArg := func(i int) int {
		if len(os.Args) < 4 {
			fmt.Fprintf(os.Stderr, "%v: missing one or more "+
				"arguments (there are %d)\n"+
				"(e.g. \"%v 0 1 10000\" OR \"%v -id 0 -n 1 "+
				"-port 10000)\"\n\n",
				os.Args, len(REQUIRED_ARGUMENTS),
				os.Args[0], os.Args[0])
			os.Exit(1)
		}

		arg := os.Args[i+1]
		val, err := strconv.Atoi(arg)
		if err != nil {
			fmt.Fprintf(os.Stderr,
				"could not parse: '%v' into an integer\n", arg)
		}
		return val
	}

	for idx, val := range REQUIRED_ARGUMENTS {
		if *val == -1 {
			*val = getIntArg(idx)
		}
	}
}
