// Command nvlink-rack-slurmd keeps NVLink-rack spares, same-block
// replacement and defragmentation around Slurm's topology/block plugin.
// Run it on a host where scontrol and squeue work with Slurm admin rights.
package main

import (
	"flag"
	"log"
	"time"

	"github.com/allensebastian97/nvlink-rack-scheduler/integrations/slurm/daemon"
	"github.com/allensebastian97/nvlink-rack-scheduler/integrations/slurm/slurm"
)

func main() {
	var cfg daemon.Config
	flag.IntVar(&cfg.Spares, "spares", 2, "idle nodes kept per block for replacements")
	flag.StringVar(&cfg.Prefix, "prefix", "nvlr", "name prefix for the daemon's reservations")
	flag.StringVar(&cfg.Preemptible, "preemptible", "nvlink-rack.io/preemptible",
		"comment marker for restart-safe jobs defrag may requeue (jobs must also be requeueable)")
	flag.StringVar(&cfg.Defrag, "defrag", "dry-run", "off | dry-run | on")
	flag.DurationVar(&cfg.HoldFor, "hold-for", 10*time.Minute, "how long a defrag hold waits for its job")
	flag.StringVar(&cfg.StatePath, "state", "/var/lib/nvlink-rack-slurmd/state.json", "job records")
	flag.StringVar(&cfg.Admin, "admin", "root", "user named on spare reservations")
	scontrol := flag.String("scontrol", "scontrol", "path to scontrol")
	squeue := flag.String("squeue", "squeue", "path to squeue")
	interval := flag.Duration("interval", 5*time.Second, "time between passes")
	once := flag.Bool("once", false, "run one pass and exit")
	flag.Parse()

	d, err := daemon.New(slurm.CLI{Scontrol: *scontrol, Squeue: *squeue}, cfg, time.Now)
	if err != nil {
		log.Fatal(err)
	}
	for {
		if err := d.Reconcile(); err != nil {
			log.Printf("pass failed: %v", err)
		}
		if *once {
			return
		}
		time.Sleep(*interval)
	}
}
