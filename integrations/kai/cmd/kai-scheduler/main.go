// Command kai-scheduler is NVIDIA's KAI Scheduler with the nvlinkrack plugin
// compiled in. Enable the plugin by name in the scheduler configuration.
package main

import (
	"fmt"
	"os"

	"github.com/kai-scheduler/KAI-scheduler/cmd/scheduler/app"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/framework"

	"github.com/allensebastian97/nvlink-rack-scheduler/integrations/kai/nvlinkrack"
)

func main() {
	// RunApp registers KAI's built-in plugins by name; ours survives that.
	framework.RegisterPluginBuilder(nvlinkrack.Name, nvlinkrack.New)
	if err := app.RunApp(); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
}
