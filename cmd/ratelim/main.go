// ratelim is a single binary with three roles, selected by subcommand:
//
//	ratelim coordinator --config /etc/ratelim/config.yaml --addr :8081
//	ratelim worker --coordinator http://coordinator:8081 --addr :8080
//	ratelim loadgen --addr :8080
package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/anthropp/ratelimiter/internal/coordinator"
	"github.com/anthropp/ratelimiter/internal/loadgen"
	"github.com/anthropp/ratelimiter/internal/worker"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: ratelim coordinator|worker|loadgen [flags]")
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "coordinator":
		fs := flag.NewFlagSet("coordinator", flag.ExitOnError)
		cfg := fs.String("config", "/etc/ratelim/config.yaml", "path to config file")
		addr := fs.String("addr", ":8081", "listen address")
		fs.Parse(os.Args[2:])
		err = coordinator.Run(*cfg, *addr)
	case "worker":
		fs := flag.NewFlagSet("worker", flag.ExitOnError)
		coord := fs.String("coordinator", "http://coordinator:8081", "coordinator base URL")
		addr := fs.String("addr", ":8080", "listen address")
		fs.Parse(os.Args[2:])
		err = worker.Run(*coord, *addr)
	case "loadgen":
		fs := flag.NewFlagSet("loadgen", flag.ExitOnError)
		addr := fs.String("addr", ":8080", "listen address")
		workers := fs.String("workers", "http://ratelim-workers:8080", "worker service base URL")
		coord := fs.String("coordinator", "http://coordinator:8081", "coordinator base URL")
		fs.Parse(os.Args[2:])
		err = loadgen.Run(*addr, *workers, *coord)
	default:
		err = fmt.Errorf("unknown subcommand %q", os.Args[1])
	}
	if err != nil {
		log.Fatal(err)
	}
}
