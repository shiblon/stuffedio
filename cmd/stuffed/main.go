// Command stuffed provides subcommands for working with stuffedio logs.
//
// Usage:
//
//	stuffed cat  <file> [file...]  - print records from stuffed log files
//	stuffed stuff <file>           - append stdin lines as records to a stuffed log file
package main

import (
	"fmt"
	"log"
	"os"
)

func usage() {
	fmt.Fprintf(os.Stderr, `Usage: stuffed <command> [args]

Commands:
  cat   <file> [file...]  Print records from stuffed log files
  stuff <file>            Append stdin lines as records to a stuffed log file
`)
	os.Exit(2)
}

func main() {
	if len(os.Args) < 2 {
		usage()
	}

	cmd, args := os.Args[1], os.Args[2:]

	switch cmd {
	case "cat":
		cmdCat(args)
	case "stuff":
		cmdStuff(args)
	default:
		log.Fatalf("Unknown command %q. Run stuffed with no arguments for usage.", cmd)
	}
}
