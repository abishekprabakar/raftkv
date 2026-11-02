//go:build !darwin && !linux

package hwinfo

func fillPlatform(info *Info, dataDir string) {}
