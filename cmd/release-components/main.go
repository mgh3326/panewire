// Command release-components writes a release's components.json. See
// docs/release-components.md.
package main

import (
	"fmt"
	"os"

	"github.com/mgh3326/panewire/internal/releasecomponents"
)

func main() {
	if err := releasecomponents.Run(os.Args[1:], os.Stdin, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "release-components:", err)
		os.Exit(2)
	}
}
