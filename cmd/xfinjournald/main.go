// Command xfinjournald builds custom finjournald binaries with extra third-party
// Go modules compiled in (similar to xcaddy for Caddy; RFC Section 2 & 9).
//
// Usage:
//
//	xfinjournald build --with github.com/acme/ledger-fees@v1.2.0 [-o ./finjournald]
package main

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

type withMod struct {
	pkgPath string
	version string
}

func main() {
	if len(os.Args) < 2 || os.Args[1] != "build" {
		fmt.Fprintln(os.Stderr, "Usage: xfinjournald build [--with module[@version]]... [-o output]")
		os.Exit(1)
	}

	var mods []withMod
	output := "finjournald"
	args := os.Args[2:]
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--with":
			if i+1 >= len(args) {
				fmt.Fprintln(os.Stderr, "--with requires a module argument")
				os.Exit(1)
			}
			i++
			spec := args[i]
			var wm withMod
			if idx := strings.IndexByte(spec, '@'); idx != -1 {
				wm.pkgPath = spec[:idx]
				wm.version = spec[idx+1:]
			} else {
				wm.pkgPath = spec
			}
			mods = append(mods, wm)
		case "-o", "--output":
			if i+1 >= len(args) {
				fmt.Fprintln(os.Stderr, "-o requires an output path")
				os.Exit(1)
			}
			i++
			output = args[i]
		}
	}

	src := GenerateMainSource(mods)
	if os.Getenv("XFINJOURNALD_DRY_RUN") == "1" {
		fmt.Print(src)
		return
	}

	tmpDir, err := os.MkdirTemp("", "xfinjournald-*")
	if err != nil {
		fmt.Fprintf(os.Stderr, "mkdir temp: %v\n", err)
		os.Exit(1)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	mainFile := filepath.Join(tmpDir, "main.go")
	if err := os.WriteFile(mainFile, []byte(src), 0600); err != nil {
		fmt.Fprintf(os.Stderr, "write main.go: %v\n", err)
		os.Exit(1)
	}

	absOut, err := filepath.Abs(output)
	if err != nil {
		absOut = output
	}
	cmd := exec.Command("go", "build", "-trimpath", "-o", absOut, "./cmd/finjournald")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		os.Exit(1)
	}
	fmt.Printf("Built %s with %d extra module(s)\n", output, len(mods))
}

// GenerateMainSource generates the Go source importing the custom modules.
func GenerateMainSource(mods []withMod) string {
	var b bytes.Buffer
	b.WriteString("package main\n\nimport (\n")
	for _, m := range mods {
		fmt.Fprintf(&b, "\t_ %q\n", m.pkgPath)
	}
	b.WriteString(")\n")
	return b.String()
}
