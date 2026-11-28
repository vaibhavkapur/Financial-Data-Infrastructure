package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"

	"distributed-tick-data-database/src/domain"
)

var walMagic = [4]byte{'T', 'W', 'A', 'L'}

// WALFrame is one atomic batch entry written to the write-ahead log.
type WALFrame struct {
	WALSeq        uint64
	BatchID       string
	CommittedAtNS int64
	Ticks         []domain.CanonicalTick
}

// WALRecoveryStats captures diagnostics from WAL replay on startup.
type WALRecoveryStats struct {
	FramesRecovered       int   `json:"frames_recovered"`
	EventsRecovered       int   `json:"events_recovered"`
	CorruptTailDetected   bool  `json:"corrupt_tail_detected"`
	TruncatedCorruptBytes int64 `json:"truncated_corrupt_bytes"`
	ValidEndOffset        int64 `json:"valid_end_offset"`
}

// WAL is an append-only, CRC32-checksummed binary write-ahead log.
type WAL struct {
	mu       sync.Mutex
	path     string
	syncMode string
	file     *os.File
	size     int64
}

func OpenWAL(path string, syncMode string, truncateCorruptTail bool) (*WAL, []WALFrame, WALRecoveryStats, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, nil, WALRecoveryStats{}, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, nil, WALRecoveryStats{}, err
	}

	frames, stats, err := scanWAL(f, truncateCorruptTail)
	if err != nil {
		_ = f.Close()
		return nil, nil, stats, err
	}

	if _, err := f.Seek(stats.ValidEndOffset, io.SeekStart); err != nil {
		_ = f.Close()
		return nil, nil, stats, err
	}

	return &WAL{
		path:     path,
		syncMode: syncMode,
		file:     f,
		size:     stats.ValidEndOffset,
	}, frames, stats, nil
}

func scanWAL(f *os.File, truncateCorruptTail bool) ([]WALFrame, WALRecoveryStats, error) {
	info, err := f.Stat()
	if err != nil {
		return nil, WALRecoveryStats{}, err
	}
	fileSize := info.Size()
	if fileSize == 0 {
		return nil, WALRecoveryStats{}, nil
	}

	data, err := io.ReadAll(f)
	if err != nil {
		return nil, WALRecoveryStats{}, err
	}

	var frames []WALFrame
	var stats WALRecoveryStats
	offset := 0

	for offset < len(data) {
		remaining := len(data) - offset
		if remaining < 12 {
			// Truncated frame header at tail
			return handleCorruptTail(f, data, offset, frames, stats, truncateCorruptTail, "truncated WAL frame header")
		}
		if !bytes.Equal(data[offset:offset+4], walMagic[:]) {
			return handleCorruptTail(f, data, offset, frames, stats, truncateCorruptTail, "invalid WAL frame magic")
		}
		payloadLen := int(binary.LittleEndian.Uint32(data[offset+4 : offset+8]))
		expectedCRC := binary.LittleEndian.Uint32(data[offset+8 : offset+12])
		frameEnd := offset + 12 + payloadLen
		if payloadLen < 0 || frameEnd > len(data) {
			return handleCorruptTail(f, data, offset, frames, stats, truncateCorruptTail, "truncated WAL frame payload")
		}
		payload := data[offset+12 : frameEnd]
		if crc32.ChecksumIEEE(payload) != expectedCRC {
			return handleCorruptTail(f, data, offset, frames, stats, truncateCorruptTail, "WAL frame CRC32 mismatch")
		}
		frame, err := decodeWALPayload(payload)
		if err != nil {
			return handleCorruptTail(f, data, offset, frames, stats, truncateCorruptTail, fmt.Sprintf("WAL frame decode error: %v", err))
		}
		frames = append(frames, frame)
		stats.FramesRecovered++
		stats.EventsRecovered += len(frame.Ticks)
		offset = frameEnd
		stats.ValidEndOffset = int64(offset)
	}
	return frames, stats, nil
}

func handleCorruptTail(f *os.File, data []byte, validOffset int, frames []WALFrame, stats WALRecoveryStats, truncate bool, reason string) ([]WALFrame, WALRecoveryStats, error) {
	stats.CorruptTailDetected = true
	stats.TruncatedCorruptBytes = int64(len(data) - validOffset)
	stats.ValidEndOffset = int64(validOffset)
	if !truncate {
		return nil, stats, fmt.Errorf("ERR_WAL_CORRUPT_TAIL: %s at byte offset %d (%d trailing bytes)", reason, validOffset, stats.TruncatedCorruptBytes)
	}
	if err := f.Truncate(int64(validOffset)); err != nil {
		return nil, stats, fmt.Errorf("ERR_WAL_TRUNCATE_FAILED: %w", err)
	}
	if err := f.Sync(); err != nil {
		return nil, stats, fmt.Errorf("ERR_WAL_SYNC_FAILED: %w", err)
	}
	return frames, stats, nil
}

func (w *WAL) Append(frame WALFrame) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	payload := encodeWALPayload(frame)
	crc := crc32.ChecksumIEEE(payload)

	var hdr [12]byte
	copy(hdr[0:4], walMagic[:])
	binary.LittleEndian.PutUint32(hdr[4:8], uint32(len(payload)))
	binary.LittleEndian.PutUint32(hdr[8:12], crc)

	// Write header + payload in one buffer write to minimize partial-write window.
	wire := make([]byte, 12+len(payload))
	copy(wire[:12], hdr[:])
	copy(wire[12:], payload)

	n, err := w.file.Write(wire)
	if err != nil {
		return n, err
	}
	if w.syncMode == "fsync_per_batch" {
		if err := w.file.Sync(); err != nil {
			return n, err
		}
	}
	w.size += int64(n)
	return n, nil
}

func (w *WAL) Reset() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.file.Truncate(0); err != nil {
		return err
	}
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	if err := w.file.Sync(); err != nil {
		return err
	}
	w.size = 0
	return nil
}

func (w *WAL) Size() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.size
}

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		return nil
	}
	_ = w.file.Sync()
	err := w.file.Close()
	w.file = nil
	return err
}

func (w *WAL) Path() string {
	return w.path
}

// --- Fast binary payload codec for WAL frames ---

func encodeWALPayload(f WALFrame) []byte {
	var buf bytes.Buffer
	writeStr := func(s string) {
		var l [2]byte
		binary.LittleEndian.PutUint16(l[:], uint16(len(s)))
		buf.Write(l[:])
		buf.WriteString(s)
	}
	_ = binary.Write(&buf, binary.LittleEndian, f.WALSeq)
	writeStr(f.BatchID)
	_ = binary.Write(&buf, binary.LittleEndian, f.CommittedAtNS)
	_ = binary.Write(&buf, binary.LittleEndian, uint32(len(f.Ticks)))
	for _, t := range f.Ticks {
		writeStr(t.SchemaVersion)
		writeStr(t.SourceID)
		writeStr(t.FeedSessionID)
		writeStr(t.BatchID)
		writeStr(t.InstrumentID)
		writeStr(t.Venue)
		buf.WriteByte(t.EventType.Code())
		buf.WriteByte(t.Side.Code())
		_ = binary.Write(&buf, binary.LittleEndian, t.EventTimeNS)
		_ = binary.Write(&buf, binary.LittleEndian, t.IngestTimeNS)
		_ = binary.Write(&buf, binary.LittleEndian, t.SequenceNumber)
		_ = binary.Write(&buf, binary.LittleEndian, t.PriceNanos)
		_ = binary.Write(&buf, binary.LittleEndian, t.SizeNanos)
		_ = binary.Write(&buf, binary.LittleEndian, t.BidPriceNanos)
		_ = binary.Write(&buf, binary.LittleEndian, t.BidSizeNanos)
		_ = binary.Write(&buf, binary.LittleEndian, t.AskPriceNanos)
		_ = binary.Write(&buf, binary.LittleEndian, t.AskSizeNanos)
		if t.LateArrival {
			buf.WriteByte(1)
		} else {
			buf.WriteByte(0)
		}
		_ = binary.Write(&buf, binary.LittleEndian, t.ShardID)
		writeStr(t.PartitionKey)
	}
	return buf.Bytes()
}

func decodeWALPayload(payload []byte) (WALFrame, error) {
	r := bytes.NewReader(payload)
	readStr := func() (string, error) {
		var l uint16
		if err := binary.Read(r, binary.LittleEndian, &l); err != nil {
			return "", err
		}
		b := make([]byte, l)
		if _, err := io.ReadFull(r, b); err != nil {
			return "", err
		}
		return string(b), nil
	}

	var f WALFrame
	var err error
	if err = binary.Read(r, binary.LittleEndian, &f.WALSeq); err != nil {
		return f, err
	}
	if f.BatchID, err = readStr(); err != nil {
		return f, err
	}
	if err = binary.Read(r, binary.LittleEndian, &f.CommittedAtNS); err != nil {
		return f, err
	}
	var count uint32
	if err = binary.Read(r, binary.LittleEndian, &count); err != nil {
		return f, err
	}
	f.Ticks = make([]domain.CanonicalTick, count)
	for i := uint32(0); i < count; i++ {
		var t domain.CanonicalTick
		if t.SchemaVersion, err = readStr(); err != nil {
			return f, err
		}
		if t.SourceID, err = readStr(); err != nil {
			return f, err
		}
		if t.FeedSessionID, err = readStr(); err != nil {
			return f, err
		}
		if t.BatchID, err = readStr(); err != nil {
			return f, err
		}
		if t.InstrumentID, err = readStr(); err != nil {
			return f, err
		}
		if t.Venue, err = readStr(); err != nil {
			return f, err
		}
		et, err := r.ReadByte()
		if err != nil {
			return f, err
		}
		sd, err := r.ReadByte()
		if err != nil {
			return f, err
		}
		t.EventType = domain.EventTypeFromCode(et)
		t.Side = domain.SideFromCode(sd)
		if err = binary.Read(r, binary.LittleEndian, &t.EventTimeNS); err != nil {
			return f, err
		}
		if err = binary.Read(r, binary.LittleEndian, &t.IngestTimeNS); err != nil {
			return f, err
		}
		if err = binary.Read(r, binary.LittleEndian, &t.SequenceNumber); err != nil {
			return f, err
		}
		if err = binary.Read(r, binary.LittleEndian, &t.PriceNanos); err != nil {
			return f, err
		}
		if err = binary.Read(r, binary.LittleEndian, &t.SizeNanos); err != nil {
			return f, err
		}
		if err = binary.Read(r, binary.LittleEndian, &t.BidPriceNanos); err != nil {
			return f, err
		}
		if err = binary.Read(r, binary.LittleEndian, &t.BidSizeNanos); err != nil {
			return f, err
		}
		if err = binary.Read(r, binary.LittleEndian, &t.AskPriceNanos); err != nil {
			return f, err
		}
		if err = binary.Read(r, binary.LittleEndian, &t.AskSizeNanos); err != nil {
			return f, err
		}
		lateB, err := r.ReadByte()
		if err != nil {
			return f, err
		}
		t.LateArrival = lateB != 0
		if err = binary.Read(r, binary.LittleEndian, &t.ShardID); err != nil {
			return f, err
		}
		if t.PartitionKey, err = readStr(); err != nil {
			return f, err
		}
		f.Ticks[i] = t
	}
	return f, nil
}
