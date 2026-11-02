// Command storagebench measures the storage engine and writes
// results/storage.json. Every number in the README and RESULTS.md comes from
// that file; nothing is typed in by hand.
package main

import (
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/abishekprabakar/raftkv/internal/hwinfo"
	"github.com/abishekprabakar/raftkv/internal/storage/engine"
	"github.com/abishekprabakar/raftkv/internal/storage/wal"
)

var (
	out   = flag.String("out", "results/storage.json", "output JSON path")
	dir   = flag.String("dir", "", "scratch directory (default: temp dir)")
	quick = flag.Bool("quick", false, "smaller, faster run (results are not for publication)")
)

const (
	keyBytes   = 16
	valueBytes = 100
)

type sizes struct {
	WriteClients int
	WriteOps     int
	ReadOps      int
	Clients      int
	SyncSeconds  float64
	BloomKeys    int
	BloomLookups int
	AmpWrites    int
	AmpKeyspace  int
	RecoveryMB   int
}

func pickSizes() sizes {
	s := sizes{
		WriteOps: 200_000, ReadOps: 200_000, Clients: 8, SyncSeconds: 8,
		BloomKeys: 300_000, BloomLookups: 50_000,
		AmpWrites: 1_000_000, AmpKeyspace: 500_000, RecoveryMB: 64,
	}
	s.WriteClients = 64
	if *quick {
		s = sizes{
			WriteClients: 64,
			WriteOps:     50_000, ReadOps: 20_000, Clients: 8, SyncSeconds: 2,
			BloomKeys: 50_000, BloomLookups: 10_000,
			AmpWrites: 100_000, AmpKeyspace: 50_000, RecoveryMB: 8,
		}
	}
	return s
}

// ---- helpers ------------------------------------------------------------------

func keyOf(i uint64) []byte {
	return []byte(fmt.Sprintf("k%015d", i))
}

func valueOf(i uint64) []byte {
	v := make([]byte, valueBytes)
	r := rand.New(rand.NewPCG(i, 42))
	var w [8]byte
	for j := 0; j < valueBytes; j += 8 {
		binary.LittleEndian.PutUint64(w[:], r.Uint64())
		copy(v[j:], w[:])
	}
	return v
}

type latencies struct {
	mu   sync.Mutex
	nsec []int64
}

func (l *latencies) add(ns []int64) {
	l.mu.Lock()
	l.nsec = append(l.nsec, ns...)
	l.mu.Unlock()
}

type latencyStats struct {
	Ops       int     `json:"ops"`
	Seconds   float64 `json:"seconds"`
	OpsPerSec float64 `json:"ops_per_sec"`
	P50Us     float64 `json:"p50_us"`
	P99Us     float64 `json:"p99_us"`
	P999Us    float64 `json:"p999_us"`
	MaxUs     float64 `json:"max_us"`
}

func (l *latencies) stats(wall time.Duration) latencyStats {
	sort.Slice(l.nsec, func(i, j int) bool { return l.nsec[i] < l.nsec[j] })
	n := len(l.nsec)
	pct := func(p float64) float64 {
		if n == 0 {
			return 0
		}
		i := int(float64(n) * p)
		if i >= n {
			i = n - 1
		}
		return float64(l.nsec[i]) / 1e3
	}
	return latencyStats{
		Ops: n, Seconds: wall.Seconds(), OpsPerSec: float64(n) / wall.Seconds(),
		P50Us: pct(0.50), P99Us: pct(0.99), P999Us: pct(0.999), MaxUs: pct(1),
	}
}

// runClients runs fn(client, i) for i in [0,total) spread across clients and
// returns per-op latencies and the wall time.
func runClients(clients, total int, fn func(c, i int) error) (*latencies, time.Duration, error) {
	lat := &latencies{}
	var wg sync.WaitGroup
	var firstErr atomic.Value
	var next atomic.Int64
	start := time.Now()
	for c := 0; c < clients; c++ {
		wg.Add(1)
		go func(c int) {
			defer wg.Done()
			local := make([]int64, 0, total/clients+1)
			for {
				i := int(next.Add(1) - 1)
				if i >= total {
					break
				}
				t0 := time.Now()
				if err := fn(c, i); err != nil {
					firstErr.CompareAndSwap(nil, err)
					break
				}
				local = append(local, int64(time.Since(t0)))
			}
			lat.add(local)
		}(c)
	}
	wg.Wait()
	wall := time.Since(start)
	if e := firstErr.Load(); e != nil {
		return nil, wall, e.(error)
	}
	return lat, wall, nil
}

// runTimed runs fn concurrently for the given duration.
func runTimed(clients int, d time.Duration, fn func(c, i int) error) (*latencies, time.Duration, error) {
	lat := &latencies{}
	var wg sync.WaitGroup
	var firstErr atomic.Value
	var next atomic.Int64
	deadline := time.Now().Add(d)
	start := time.Now()
	for c := 0; c < clients; c++ {
		wg.Add(1)
		go func(c int) {
			defer wg.Done()
			var local []int64
			for time.Now().Before(deadline) {
				i := int(next.Add(1) - 1)
				t0 := time.Now()
				if err := fn(c, i); err != nil {
					firstErr.CompareAndSwap(nil, err)
					break
				}
				local = append(local, int64(time.Since(t0)))
			}
			lat.add(local)
		}(c)
	}
	wg.Wait()
	wall := time.Since(start)
	if e := firstErr.Load(); e != nil {
		return nil, wall, e.(error)
	}
	return lat, wall, nil
}

func dirSize(path string) int64 {
	var total int64
	filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			total += info.Size()
		}
		return nil
	})
	return total
}

// ---- result schema -----------------------------------------------------------

type results struct {
	GeneratedAt string             `json:"generated_at"`
	Quick       bool               `json:"quick_mode"`
	Hardware    hwinfo.Info        `json:"hardware"`
	Workload    map[string]any     `json:"workload"`
	Write       *writeResult       `json:"write"`
	Read        *readResult        `json:"read"`
	SyncModes   []syncModeResult   `json:"sync_modes"`
	Bloom       *bloomResult       `json:"bloom"`
	Amp         *ampResult         `json:"amplification"`
	Recovery    *recoveryResult    `json:"recovery"`
	Durations   map[string]float64 `json:"section_seconds"`
}

type writeResult struct {
	Clients  int          `json:"clients"`
	SyncMode string       `json:"sync_mode"`
	Latency  latencyStats `json:"latency"`
	Flushes  int64        `json:"flushes"`
	Compacts int64        `json:"compactions"`
	Stalls   int64        `json:"write_stalls"`
	WALSyncs int64        `json:"wal_fsyncs"`
	WALBatch float64      `json:"records_per_fsync"`
}

type readResult struct {
	Clients       int          `json:"clients"`
	Keys          int          `json:"keys_in_db"`
	Latency       latencyStats `json:"latency"`
	LevelFiles    []int        `json:"level_files"`
	BlockReads    int64        `json:"block_reads"`
	BlocksPerRead float64      `json:"block_reads_per_get"`
	Note          string       `json:"note"`
}

type syncModeResult struct {
	Mode         string       `json:"mode"`
	Clients      int          `json:"clients"`
	Latency      latencyStats `json:"latency"`
	Fsyncs       int64        `json:"fsyncs"`
	PerFsync     float64      `json:"records_per_fsync"`
	FsyncsPerSec float64      `json:"fsyncs_per_sec"`
}

type bloomSide struct {
	Enabled        bool         `json:"enabled"`
	Lookups        int          `json:"lookups"`
	BlockReads     int64        `json:"block_reads"`
	ReadsPerLookup float64      `json:"block_reads_per_lookup"`
	BloomChecks    int64        `json:"bloom_checks"`
	BloomNegatives int64        `json:"bloom_negatives"`
	FalsePositive  float64      `json:"false_positive_rate"`
	Latency        latencyStats `json:"latency"`
	LevelFiles     []int        `json:"level_files"`
}

type bloomResult struct {
	KeysInDB   int       `json:"keys_in_db"`
	BitsPerKey int       `json:"bits_per_key"`
	With       bloomSide `json:"with_bloom"`
	Without    bloomSide `json:"without_bloom"`
	Reduction  float64   `json:"disk_read_reduction_factor"`
}

type ampShape struct {
	LevelFiles         []int   `json:"level_files"`
	LevelBytes         []int64 `json:"level_bytes"`
	SSTBytes           int64   `json:"sst_bytes"`
	SpaceAmplification float64 `json:"space_amplification"`
}

type ampResult struct {
	Writes              int      `json:"writes"`
	UniqueKeys          int      `json:"unique_keys"`
	UserBytes           int64    `json:"user_bytes_written"`
	LiveBytes           int64    `json:"live_user_bytes"`
	WALBytes            int64    `json:"wal_bytes"`
	FlushBytes          int64    `json:"flush_bytes"`
	CompactionWrite     int64    `json:"compaction_bytes_written"`
	CompactionRead      int64    `json:"compaction_bytes_read"`
	Compactions         int64    `json:"compactions"`
	TrivialMoves        int64    `json:"trivial_moves"`
	WriteAmpTotal       float64  `json:"write_amplification_total"`
	WriteAmpSST         float64  `json:"write_amplification_sst_only"`
	SteadyState         ampShape `json:"steady_state"`
	AfterFullCompaction ampShape `json:"after_full_compaction"`
	SyncMode            string   `json:"sync_mode"`
	Note                string   `json:"note"`
}

type recoveryResult struct {
	Records        int64   `json:"wal_records"`
	WALBytes       int64   `json:"wal_bytes"`
	ReplayMs       float64 `json:"replay_ms"`
	OpenMs         float64 `json:"open_ms_including_flush"`
	ReplayMBPerSec float64 `json:"replay_mb_per_sec"`
	ReplayOpsPerS  float64 `json:"replay_records_per_sec"`
}

// ---- sections ------------------------------------------------------------------

func benchWrite(sz sizes, root string) (*writeResult, *engine.DB, []uint64, error) {
	db, err := engine.Open(filepath.Join(root, "write"), engine.Options{SyncMode: wal.GroupCommit})
	if err != nil {
		return nil, nil, nil, err
	}
	keys := make([]uint64, sz.WriteOps)
	r := rand.New(rand.NewPCG(1, 1))
	for i := range keys {
		keys[i] = r.Uint64N(uint64(sz.WriteOps) * 4)
	}
	lat, wall, err := runClients(sz.WriteClients, sz.WriteOps, func(_, i int) error {
		return db.Put(keyOf(keys[i]), valueOf(keys[i]))
	})
	if err != nil {
		return nil, nil, nil, err
	}
	s := db.Stats()
	res := &writeResult{
		Clients: sz.WriteClients, SyncMode: wal.GroupCommit.String(), Latency: lat.stats(wall),
		Flushes: s.Flushes, Compacts: s.Compactions, Stalls: s.WriteStalls, WALSyncs: s.WALSyncs,
	}
	if s.WALSyncs > 0 {
		res.WALBatch = float64(s.WALRecords) / float64(s.WALSyncs)
	}
	return res, db, keys, nil
}

func benchRead(sz sizes, db *engine.DB, keys []uint64) (*readResult, error) {
	if err := db.WaitIdle(); err != nil {
		return nil, err
	}
	before := db.Stats()
	r := rand.New(rand.NewPCG(2, 2))
	picks := make([]uint64, sz.ReadOps)
	for i := range picks {
		picks[i] = keys[r.IntN(len(keys))]
	}
	lat, wall, err := runClients(sz.Clients, sz.ReadOps, func(_, i int) error {
		_, err := db.Get(keyOf(picks[i]))
		return err
	})
	if err != nil {
		return nil, err
	}
	after := db.Stats()
	unique := map[uint64]bool{}
	for _, k := range keys {
		unique[k] = true
	}
	reads := after.BlockReads - before.BlockReads
	return &readResult{
		Clients: sz.Clients, Keys: len(unique), Latency: lat.stats(wall), LevelFiles: after.LevelFiles,
		BlockReads: reads, BlocksPerRead: float64(reads) / float64(sz.ReadOps),
		Note: "random point reads of existing keys over the fully compacted amplification data set; it fits in the OS page cache, so this measures engine overhead rather than disk latency",
	}, nil
}

func benchSyncModes(sz sizes, root string) ([]syncModeResult, error) {
	type cfg struct {
		mode    wal.SyncMode
		clients int
	}
	cfgs := []cfg{
		{wal.SyncEach, sz.Clients},
		{wal.GroupCommit, 1},
		{wal.GroupCommit, sz.Clients},
		{wal.GroupCommit, sz.WriteClients},
		{wal.NoSync, sz.Clients},
	}
	var out []syncModeResult
	for i, c := range cfgs {
		db, err := engine.Open(filepath.Join(root, fmt.Sprintf("sync-%d", i)), engine.Options{SyncMode: c.mode})
		if err != nil {
			return nil, err
		}
		lat, wall, err := runTimed(c.clients, time.Duration(sz.SyncSeconds*float64(time.Second)), func(_, i int) error {
			return db.Put(keyOf(uint64(i)), valueOf(uint64(i)))
		})
		if err != nil {
			return nil, err
		}
		s := db.Stats()
		r := syncModeResult{Mode: c.mode.String(), Clients: c.clients, Latency: lat.stats(wall), Fsyncs: s.WALSyncs}
		if s.WALSyncs > 0 {
			r.PerFsync = float64(s.WALRecords) / float64(s.WALSyncs)
			r.FsyncsPerSec = float64(s.WALSyncs) / wall.Seconds()
		}
		out = append(out, r)
		db.Close()
	}
	return out, nil
}

func benchBloom(sz sizes, root string) (*bloomResult, error) {
	res := &bloomResult{KeysInDB: sz.BloomKeys, BitsPerKey: 10}
	for _, enabled := range []bool{true, false} {
		db, err := engine.Open(filepath.Join(root, fmt.Sprintf("bloom-%v", enabled)), engine.Options{
			SyncMode: wal.NoSync, DisableBloomFilter: !enabled,
		})
		if err != nil {
			return nil, err
		}
		// Keys are even numbers; lookups probe odd numbers so every probe
		// misses but still lands inside a table's key range. Keys arrive
		// nearly in order, so tables barely overlap and a probe typically
		// reaches one table: the easy case for going without a filter.
		_, _, err = runClients(sz.Clients, sz.BloomKeys, func(_, i int) error {
			return db.Put(keyOf(uint64(i)*2), valueOf(uint64(i)))
		})
		if err != nil {
			return nil, err
		}
		if err := db.WaitIdle(); err != nil {
			return nil, err
		}
		before := db.Stats()
		r := rand.New(rand.NewPCG(3, 3))
		lat, wall, err := runClients(sz.Clients, sz.BloomLookups, func(_, i int) error {
			_, err := db.Get(keyOf(uint64(r.IntN(sz.BloomKeys))*2 + 1))
			if err == engine.ErrNotFound {
				return nil
			}
			return err
		})
		if err != nil {
			return nil, err
		}
		after := db.Stats()
		side := bloomSide{
			Enabled: enabled, Lookups: sz.BloomLookups, Latency: lat.stats(wall), LevelFiles: after.LevelFiles,
			BlockReads:     after.BlockReads - before.BlockReads,
			BloomChecks:    after.BloomChecks - before.BloomChecks,
			BloomNegatives: after.BloomNegatives - before.BloomNegatives,
		}
		side.ReadsPerLookup = float64(side.BlockReads) / float64(sz.BloomLookups)
		if side.BloomChecks > 0 {
			// Every probe is a miss, so every positive is a false positive.
			side.FalsePositive = float64(side.BloomChecks-side.BloomNegatives) / float64(side.BloomChecks)
		}
		if enabled {
			res.With = side
		} else {
			res.Without = side
		}
		db.Close()
	}
	if res.With.BlockReads > 0 {
		res.Reduction = float64(res.Without.BlockReads) / float64(res.With.BlockReads)
	}
	return res, nil
}

func benchAmplification(sz sizes, root string) (*ampResult, *engine.DB, []uint64, error) {
	path := filepath.Join(root, "amp")
	db, err := engine.Open(path, engine.Options{SyncMode: wal.NoSync})
	if err != nil {
		return nil, nil, nil, err
	}
	r := rand.New(rand.NewPCG(4, 4))
	keys := make([]uint64, sz.AmpWrites)
	for i := range keys {
		keys[i] = r.Uint64N(uint64(sz.AmpKeyspace))
	}
	_, _, err = runClients(sz.Clients, sz.AmpWrites, func(_, i int) error {
		return db.Put(keyOf(keys[i]), valueOf(keys[i]))
	})
	if err != nil {
		return nil, nil, nil, err
	}
	if err := db.WaitIdle(); err != nil {
		return nil, nil, nil, err
	}
	seen := make([]bool, sz.AmpKeyspace)
	unique := 0
	for _, k := range keys {
		if !seen[k] {
			seen[k] = true
			unique++
		}
	}
	s := db.Stats()
	res := &ampResult{
		Writes: sz.AmpWrites, UniqueKeys: unique,
		UserBytes: int64(sz.AmpWrites) * (keyBytes + valueBytes),
		LiveBytes: int64(unique) * (keyBytes + valueBytes),
		WALBytes:  s.WALBytes, FlushBytes: s.FlushBytes, CompactionWrite: s.CompactionBytesWritten,
		CompactionRead: s.CompactionBytesRead, Compactions: s.Compactions, TrivialMoves: s.TrivialMoves,
		SyncMode: wal.NoSync.String(),
		Note:     "write amplification = bytes physically written (WAL + flushes + compaction output) / user bytes; space amplification = SSTable bytes on disk / live user bytes, measured when the background worker is idle and again after a full compaction",
	}
	res.WriteAmpTotal = float64(res.WALBytes+res.FlushBytes+res.CompactionWrite) / float64(res.UserBytes)
	res.WriteAmpSST = float64(res.FlushBytes+res.CompactionWrite) / float64(res.UserBytes)
	res.SteadyState = ampShape{LevelFiles: s.LevelFiles, LevelBytes: s.LevelBytes, SSTBytes: s.TotalSSTBytes,
		SpaceAmplification: float64(s.TotalSSTBytes) / float64(res.LiveBytes)}
	if err := db.Compact(); err != nil {
		return nil, nil, nil, err
	}
	s = db.Stats()
	res.AfterFullCompaction = ampShape{LevelFiles: s.LevelFiles, LevelBytes: s.LevelBytes, SSTBytes: s.TotalSSTBytes,
		SpaceAmplification: float64(s.TotalSSTBytes) / float64(res.LiveBytes)}
	return res, db, keys, nil
}

func benchRecovery(sz sizes, root string) (*recoveryResult, error) {
	path := filepath.Join(root, "recovery")
	opts := engine.Options{SyncMode: wal.NoSync, MemtableBytes: 4 << 30, NoFlushOnClose: true}
	db, err := engine.Open(path, opts)
	if err != nil {
		return nil, err
	}
	target := int64(sz.RecoveryMB) << 20
	n := int(target / (keyBytes + valueBytes))
	_, _, err = runClients(sz.Clients, n, func(_, i int) error {
		return db.Put(keyOf(uint64(i)), valueOf(uint64(i)))
	})
	if err != nil {
		return nil, err
	}
	walBytes := db.Stats().WALBytes
	if err := db.Close(); err != nil {
		return nil, err
	}
	db, err = engine.Open(path, engine.Options{SyncMode: wal.NoSync})
	if err != nil {
		return nil, err
	}
	s := db.Stats()
	db.Close()
	res := &recoveryResult{
		Records: s.ReplayedRecords, WALBytes: walBytes,
		ReplayMs: float64(s.ReplayDuration) / 1e6, OpenMs: float64(s.RecoveryDuration) / 1e6,
	}
	res.ReplayMBPerSec = float64(walBytes) / (1 << 20) / s.ReplayDuration.Seconds()
	res.ReplayOpsPerS = float64(s.ReplayedRecords) / s.ReplayDuration.Seconds()
	return res, nil
}

// ---- main --------------------------------------------------------------------------

func main() {
	flag.Parse()
	sz := pickSizes()
	root := *dir
	if root == "" {
		d, err := os.MkdirTemp("", "raftkv-bench-")
		if err != nil {
			fatal(err)
		}
		root = d
		defer os.RemoveAll(d)
	}
	res := results{
		GeneratedAt: time.Now().UTC().Format(time.RFC3339),
		Quick:       *quick,
		Hardware:    hwinfo.Collect(root),
		Workload: map[string]any{
			"key_bytes": keyBytes, "value_bytes": valueBytes, "clients": sz.Clients, "write_clients": sz.WriteClients,
			"group_commit_window_us": 100,
			"write_ops":              sz.WriteOps, "read_ops": sz.ReadOps, "sync_mode_seconds_each": sz.SyncSeconds,
			"bloom_keys": sz.BloomKeys, "bloom_lookups": sz.BloomLookups,
			"amplification_writes": sz.AmpWrites, "amplification_keyspace": sz.AmpKeyspace,
			"recovery_wal_mb": sz.RecoveryMB,
			"engine_defaults": "memtable 4 MiB, block 4 KiB, bloom 10 bits/key, L0 trigger 4, L1 10 MiB x10 per level, 2 MiB files",
		},
		Durations: map[string]float64{},
	}
	section := func(name string, fn func() error) {
		fmt.Printf("== %s\n", name)
		t0 := time.Now()
		if err := fn(); err != nil {
			fatal(fmt.Errorf("%s: %w", name, err))
		}
		res.Durations[name] = time.Since(t0).Seconds()
	}

	var db *engine.DB
	var keys []uint64
	section("amplification", func() (err error) {
		res.Amp, db, keys, err = benchAmplification(sz, root)
		if err == nil {
			a := res.Amp
			fmt.Printf("   write amp %.2fx total (%.2fx SST only); space amp %.2fx steady, %.2fx after full compaction; levels %v\n",
				a.WriteAmpTotal, a.WriteAmpSST, a.SteadyState.SpaceAmplification, a.AfterFullCompaction.SpaceAmplification, a.SteadyState.LevelFiles)
		}
		return
	})
	section("read", func() (err error) {
		res.Read, err = benchRead(sz, db, keys)
		if err == nil {
			l := res.Read.Latency
			fmt.Printf("   %.0f ops/s  p50 %.0f us  p99 %.0f us  levels %v  %.2f block reads/get\n",
				l.OpsPerSec, l.P50Us, l.P99Us, res.Read.LevelFiles, res.Read.BlocksPerRead)
		}
		db.Close()
		return
	})
	section("write", func() (err error) {
		var wdb *engine.DB
		res.Write, wdb, _, err = benchWrite(sz, root)
		if err == nil {
			l := res.Write.Latency
			fmt.Printf("   %.0f ops/s  p50 %.0f us  p99 %.0f us  (%d clients, %d flushes, %d compactions, %.1f records/fsync)\n",
				l.OpsPerSec, l.P50Us, l.P99Us, res.Write.Clients, res.Write.Flushes, res.Write.Compacts, res.Write.WALBatch)
			wdb.Close()
		}
		return
	})
	section("sync_modes", func() (err error) {
		res.SyncModes, err = benchSyncModes(sz, root)
		for _, m := range res.SyncModes {
			fmt.Printf("   %-16s %3d clients %10.0f ops/s  p50 %8.0f us  p99 %8.0f us  %.1f records/fsync  %.0f fsync/s\n",
				m.Mode, m.Clients, m.Latency.OpsPerSec, m.Latency.P50Us, m.Latency.P99Us, m.PerFsync, m.FsyncsPerSec)
		}
		return
	})
	section("bloom", func() (err error) {
		res.Bloom, err = benchBloom(sz, root)
		if err == nil {
			b := res.Bloom
			fmt.Printf("   with bloom:    %.3f block reads/lookup, fp rate %.4f, p99 %.0f us, levels %v\n", b.With.ReadsPerLookup, b.With.FalsePositive, b.With.Latency.P99Us, b.With.LevelFiles)
			fmt.Printf("   without bloom: %.3f block reads/lookup, p99 %.0f us  (%.1fx fewer reads with filter)\n", b.Without.ReadsPerLookup, b.Without.Latency.P99Us, b.Reduction)
		}
		return
	})
	section("recovery", func() (err error) {
		res.Recovery, err = benchRecovery(sz, root)
		if err == nil {
			r := res.Recovery
			fmt.Printf("   %d records / %.1f MB replayed in %.0f ms (%.0f MB/s); Open incl. flush %.0f ms\n",
				r.Records, float64(r.WALBytes)/(1<<20), r.ReplayMs, r.ReplayMBPerSec, r.OpenMs)
		}
		return
	})

	os.MkdirAll(filepath.Dir(*out), 0o755)
	data, _ := json.MarshalIndent(res, "", "  ")
	if err := os.WriteFile(*out, data, 0o644); err != nil {
		fatal(err)
	}
	fmt.Println("wrote", *out)
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "error:", err)
	os.Exit(1)
}
