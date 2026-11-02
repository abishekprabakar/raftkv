package sstable

import (
	"bufio"
	"encoding/binary"
	"errors"
	"os"

	"github.com/abishekprabakar/raftkv/internal/storage/ikey"
)

// WriterOptions tune table construction.
type WriterOptions struct {
	BlockBytes      int // target uncompressed data block size
	BloomBitsPerKey int // 0 disables the bloom filter
}

// Writer builds an SSTable from internal keys added in increasing order.
type Writer struct {
	f    *os.File
	w    *bufio.Writer
	opts WriterOptions

	off     uint64
	block   []byte
	lastKey []byte
	index   []byte
	keys    [][]byte // user keys for the bloom filter
	props   Properties
	closed  bool
}

// NewWriter starts writing a table to f.
func NewWriter(f *os.File, opts WriterOptions) *Writer {
	if opts.BlockBytes <= 0 {
		opts.BlockBytes = 4096
	}
	return &Writer{f: f, w: bufio.NewWriterSize(f, 1<<20), opts: opts, props: Properties{MinSeq: ^uint64(0)}}
}

// Add appends an entry. Keys must be strictly increasing in ikey order.
func (w *Writer) Add(key, value []byte) error {
	if w.lastKey != nil && ikey.Compare(w.lastKey, key) >= 0 {
		return errors.New("sstable: keys out of order")
	}
	if w.props.NumEntries == 0 {
		w.props.Smallest = append([]byte(nil), key...)
	}
	w.block = binary.AppendUvarint(w.block, uint64(len(key)))
	w.block = binary.AppendUvarint(w.block, uint64(len(value)))
	w.block = append(w.block, key...)
	w.block = append(w.block, value...)
	w.lastKey = append(w.lastKey[:0], key...)
	_, seq, _ := ikey.Split(key)
	if seq < w.props.MinSeq {
		w.props.MinSeq = seq
	}
	if seq > w.props.MaxSeq {
		w.props.MaxSeq = seq
	}
	w.props.NumEntries++
	if w.opts.BloomBitsPerKey > 0 {
		w.keys = append(w.keys, append([]byte(nil), ikey.UserKey(key)...))
	}
	if len(w.block) >= w.opts.BlockBytes {
		return w.flushBlock()
	}
	return nil
}

func (w *Writer) flushBlock() error {
	if len(w.block) == 0 {
		return nil
	}
	data := blockWithCRC(w.block)
	w.index = appendBytes(w.index, w.lastKey)
	w.index = binary.AppendUvarint(w.index, w.off)
	w.index = binary.AppendUvarint(w.index, uint64(len(data)))
	if _, err := w.w.Write(data); err != nil {
		return err
	}
	w.off += uint64(len(data))
	w.props.NumBlocks++
	w.block = w.block[:0]
	return nil
}

// EstimatedSize returns the bytes written so far plus the open block.
func (w *Writer) EstimatedSize() uint64 { return w.off + uint64(len(w.block)) }

// Finish writes the index, bloom filter, properties and footer, fsyncs the
// file and returns the table properties. The caller closes f.
func (w *Writer) Finish() (Properties, error) {
	if w.closed {
		return w.props, errors.New("sstable: writer finished twice")
	}
	w.closed = true
	if w.props.NumEntries == 0 {
		return w.props, errors.New("sstable: empty table")
	}
	if err := w.flushBlock(); err != nil {
		return w.props, err
	}
	w.props.Largest = append([]byte(nil), w.lastKey...)
	w.props.DataBytes = w.off
	var ft footer

	write := func(block []byte) (off, n uint64, err error) {
		data := blockWithCRC(block)
		if _, err := w.w.Write(data); err != nil {
			return 0, 0, err
		}
		off = w.off
		w.off += uint64(len(data))
		return off, uint64(len(data)), nil
	}
	var err error
	if ft.indexOff, ft.indexLen, err = write(w.index); err != nil {
		return w.props, err
	}
	if w.opts.BloomBitsPerKey > 0 {
		if ft.bloomOff, ft.bloomLen, err = write(BuildBloom(w.keys, w.opts.BloomBitsPerKey)); err != nil {
			return w.props, err
		}
	}
	if ft.propsOff, ft.propsLen, err = write(w.props.encode()); err != nil {
		return w.props, err
	}
	if _, err := w.w.Write(ft.encode()); err != nil {
		return w.props, err
	}
	if err := w.w.Flush(); err != nil {
		return w.props, err
	}
	return w.props, w.f.Sync()
}
