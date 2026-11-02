//go:build darwin

package hwinfo

import (
	"os/exec"
	"strconv"
	"strings"
	"syscall"
)

func sysctl(name string) string {
	out, err := exec.Command("sysctl", "-n", name).Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}

func fillPlatform(info *Info, dataDir string) {
	info.CPU = sysctl("machdep.cpu.brand_string")
	if m, err := strconv.ParseInt(sysctl("hw.memsize"), 10, 64); err == nil {
		info.MemoryBytes = m
	}
	if out, err := exec.Command("sw_vers", "-productVersion").Output(); err == nil {
		info.OSVersion = "macOS " + strings.TrimSpace(string(out))
	}
	var st syscall.Statfs_t
	if err := syscall.Statfs(dataDir, &st); err == nil {
		b := make([]byte, 0, len(st.Fstypename))
		for _, c := range st.Fstypename {
			if c == 0 {
				break
			}
			b = append(b, byte(c))
		}
		info.Filesystem = string(b)
	}
	info.Note = "On macOS, Go's File.Sync issues fcntl(F_FULLFSYNC), which forces the drive to flush its cache; fsync costs here are higher than Linux ext4 fsync on the same hardware."
}
