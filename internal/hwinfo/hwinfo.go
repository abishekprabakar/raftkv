// Package hwinfo records the machine a benchmark ran on, so every number in
// results/*.json carries its hardware context.
package hwinfo

import (
	"runtime"
	"strings"
	"time"
)

// Info describes the host.
type Info struct {
	CollectedAt string `json:"collected_at"`
	OS          string `json:"os"`
	OSVersion   string `json:"os_version"`
	Arch        string `json:"arch"`
	CPU         string `json:"cpu"`
	Cores       int    `json:"cores"`
	MemoryBytes int64  `json:"memory_bytes"`
	Filesystem  string `json:"filesystem"`
	GoVersion   string `json:"go_version"`
	Note        string `json:"note,omitempty"`
}

// Collect gathers host information. dataDir is used to identify the
// filesystem the benchmark wrote to.
func Collect(dataDir string) Info {
	info := Info{
		CollectedAt: time.Now().UTC().Format(time.RFC3339),
		OS:          runtime.GOOS,
		Arch:        runtime.GOARCH,
		Cores:       runtime.NumCPU(),
		GoVersion:   runtime.Version(),
	}
	fillPlatform(&info, dataDir)
	info.CPU = strings.TrimSpace(info.CPU)
	return info
}
