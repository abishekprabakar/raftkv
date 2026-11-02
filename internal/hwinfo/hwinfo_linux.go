//go:build linux

package hwinfo

import (
	"bufio"
	"os"
	"strconv"
	"strings"
	"syscall"
)

func fillPlatform(info *Info, dataDir string) {
	if f, err := os.Open("/proc/cpuinfo"); err == nil {
		sc := bufio.NewScanner(f)
		for sc.Scan() {
			if strings.HasPrefix(sc.Text(), "model name") {
				if i := strings.Index(sc.Text(), ":"); i >= 0 {
					info.CPU = strings.TrimSpace(sc.Text()[i+1:])
				}
				break
			}
		}
		f.Close()
	}
	if f, err := os.Open("/proc/meminfo"); err == nil {
		sc := bufio.NewScanner(f)
		for sc.Scan() {
			if strings.HasPrefix(sc.Text(), "MemTotal:") {
				fields := strings.Fields(sc.Text())
				if len(fields) >= 2 {
					if kb, err := strconv.ParseInt(fields[1], 10, 64); err == nil {
						info.MemoryBytes = kb * 1024
					}
				}
				break
			}
		}
		f.Close()
	}
	if data, err := os.ReadFile("/etc/os-release"); err == nil {
		for _, line := range strings.Split(string(data), "\n") {
			if strings.HasPrefix(line, "PRETTY_NAME=") {
				info.OSVersion = strings.Trim(strings.TrimPrefix(line, "PRETTY_NAME="), `"`)
			}
		}
	}
	if data, err := os.ReadFile("/sys/devices/virtual/dmi/id/product_name"); err == nil {
		info.Note = "instance type: " + strings.TrimSpace(string(data))
	}
	var st syscall.Statfs_t
	if err := syscall.Statfs(dataDir, &st); err == nil {
		switch st.Type {
		case 0xEF53:
			info.Filesystem = "ext4"
		case 0x58465342:
			info.Filesystem = "xfs"
		case 0x01021994:
			info.Filesystem = "tmpfs"
		default:
			info.Filesystem = "0x" + strconv.FormatInt(int64(st.Type), 16)
		}
	}
}
