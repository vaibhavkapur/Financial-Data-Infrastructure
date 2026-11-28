package codec

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"sort"

	"distributed-tick-data-database/src/domain"
)

var SegmentMagic = [8]byte{'T', 'S', 'E', 'G', '0', '0', '0', '1'}

const (
	CodecDeltaDictRaw  uint8 = 1
	CodecDeltaDictGzip uint8 = 2
)

// ZoneMap is stored uncompressed at the head of every .tseg segment so queries
// can skip non-overlapping segments without reading or decompressing payloads.
type ZoneMap struct {
	CodecID            uint8    `json:"codec_id"`
	RowCount           uint32   `json:"row_count"`
	MinEventTimeNS     int64    `json:"min_event_time_ns"`
	MaxEventTimeNS     int64    `json:"max_event_time_ns"`
	MinSeq             uint64   `json:"min_seq"`
	MaxSeq             uint64   `json:"max_seq"`
	Instruments        []string `json:"instruments"`
	UncompressedBytes  uint32   `json:"uncompressed_bytes"`
	CompressedBytes    uint32   `json:"compressed_bytes"`
	PayloadCRC32       uint32   `json:"payload_crc32"`
	HeaderByteLength   int      `json:"-"`
}

func (z ZoneMap) OverlapsTime(startNS, endNS int64) bool {
	if z.RowCount == 0 {
		return false
	}
	return z.MaxEventTimeNS >= startNS && z.MinEventTimeNS <= endNS
}

func (z ZoneMap) ContainsInstrument(inst string) bool {
	if inst == "" {
		return true
	}
	idx := sort.SearchStrings(z.Instruments, inst)
	return idx < len(z.Instruments) && z.Instruments[idx] == inst
}

// EncodeSegment serializes ticks in canonical order into a checksummed columnar
// compressed binary segment with an uncompressed ZoneMap prefix.
func EncodeSegment(ticks []domain.CanonicalTick, compression string) ([]byte, ZoneMap, error) {
	sorted := make([]domain.CanonicalTick, len(ticks))
	copy(sorted, ticks)
	sort.Slice(sorted, func(i, j int) bool {
		return domain.CompareCanonicalOrder(sorted[i], sorted[j]) < 0
	})

	codecID := CodecDeltaDictGzip
	if compression == "delta_dict_raw" {
		codecID = CodecDeltaDictRaw
	}

	zm := ZoneMap{
		CodecID:  codecID,
		RowCount: uint32(len(sorted)),
	}
	instSet := make(map[string]struct{})
	for i, t := range sorted {
		if i == 0 || t.EventTimeNS < zm.MinEventTimeNS {
			zm.MinEventTimeNS = t.EventTimeNS
		}
		if i == 0 || t.EventTimeNS > zm.MaxEventTimeNS {
			zm.MaxEventTimeNS = t.EventTimeNS
		}
		if i == 0 || t.SequenceNumber < zm.MinSeq {
			zm.MinSeq = t.SequenceNumber
		}
		if i == 0 || t.SequenceNumber > zm.MaxSeq {
			zm.MaxSeq = t.SequenceNumber
		}
		instSet[t.InstrumentID] = struct{}{}
	}
	zm.Instruments = make([]string, 0, len(instSet))
	for inst := range instSet {
		zm.Instruments = append(zm.Instruments, inst)
	}
	sort.Strings(zm.Instruments)

	// Encode columns
	var colBuf bytes.Buffer
	n := len(sorted)

	eventTimes := make([]int64, n)
	ingestTimes := make([]int64, n)
	seqs := make([]int64, n)
	prices := make([]int64, n)
	sizes := make([]int64, n)
	bidPrices := make([]int64, n)
	bidSizes := make([]int64, n)
	askPrices := make([]int64, n)
	askSizes := make([]int64, n)
	schemaVers := make([]string, n)
	sourceIDs := make([]string, n)
	feedSess := make([]string, n)
	batchIDs := make([]string, n)
	instIDs := make([]string, n)
	venues := make([]string, n)
	partKeys := make([]string, n)
	eventTypes := make([]byte, n)
	sides := make([]byte, n)
	lateFlags := make([]byte, n)
	shardIDs := make([]int64, n)

	for i, t := range sorted {
		eventTimes[i] = t.EventTimeNS
		ingestTimes[i] = t.IngestTimeNS
		seqs[i] = int64(t.SequenceNumber)
		prices[i] = t.PriceNanos
		sizes[i] = t.SizeNanos
		bidPrices[i] = t.BidPriceNanos
		bidSizes[i] = t.BidSizeNanos
		askPrices[i] = t.AskPriceNanos
		askSizes[i] = t.AskSizeNanos
		schemaVers[i] = t.SchemaVersion
		sourceIDs[i] = t.SourceID
		feedSess[i] = t.FeedSessionID
		batchIDs[i] = t.BatchID
		instIDs[i] = t.InstrumentID
		venues[i] = t.Venue
		partKeys[i] = t.PartitionKey
		eventTypes[i] = t.EventType.Code()
		sides[i] = t.Side.Code()
		if t.LateArrival {
			lateFlags[i] = 1
		}
		shardIDs[i] = int64(t.ShardID)
	}

	encodeDeltaOfDeltaI64(&colBuf, eventTimes)
	encodeDeltaOfDeltaI64(&colBuf, ingestTimes)
	encodeDeltaOfDeltaI64(&colBuf, seqs)
	encodeDeltaI64(&colBuf, prices)
	encodeVarintI64(&colBuf, sizes)
	encodeDeltaI64(&colBuf, bidPrices)
	encodeVarintI64(&colBuf, bidSizes)
	encodeDeltaI64(&colBuf, askPrices)
	encodeVarintI64(&colBuf, askSizes)
	encodeDictRLE(&colBuf, schemaVers)
	encodeDictRLE(&colBuf, sourceIDs)
	encodeDictRLE(&colBuf, feedSess)
	encodeDictRLE(&colBuf, batchIDs)
	encodeDictRLE(&colBuf, instIDs)
	encodeDictRLE(&colBuf, venues)
	encodeDictRLE(&colBuf, partKeys)
	encodeByteRLE(&colBuf, eventTypes)
	encodeByteRLE(&colBuf, sides)
	encodeByteRLE(&colBuf, lateFlags)
	encodeDeltaI64(&colBuf, shardIDs)

	rawPayload := colBuf.Bytes()
	zm.UncompressedBytes = uint32(len(rawPayload))

	var payload []byte
	if codecID == CodecDeltaDictGzip {
		var gzBuf bytes.Buffer
		gw, err := gzip.NewWriterLevel(&gzBuf, gzip.BestSpeed)
		if err != nil {
			return nil, ZoneMap{}, err
		}
		if _, err := gw.Write(rawPayload); err != nil {
			_ = gw.Close()
			return nil, ZoneMap{}, err
		}
		if err := gw.Close(); err != nil {
			return nil, ZoneMap{}, err
		}
		payload = gzBuf.Bytes()
	} else {
		payload = rawPayload
	}

	zm.CompressedBytes = uint32(len(payload))
	zm.PayloadCRC32 = crc32.ChecksumIEEE(payload)

	var out bytes.Buffer
	out.Write(SegmentMagic[:])
	var headerBody bytes.Buffer
	headerBody.WriteByte(zm.CodecID)
	_ = binary.Write(&headerBody, binary.LittleEndian, zm.RowCount)
	_ = binary.Write(&headerBody, binary.LittleEndian, zm.MinEventTimeNS)
	_ = binary.Write(&headerBody, binary.LittleEndian, zm.MaxEventTimeNS)
	_ = binary.Write(&headerBody, binary.LittleEndian, zm.MinSeq)
	_ = binary.Write(&headerBody, binary.LittleEndian, zm.MaxSeq)
	_ = binary.Write(&headerBody, binary.LittleEndian, zm.UncompressedBytes)
	_ = binary.Write(&headerBody, binary.LittleEndian, zm.CompressedBytes)
	_ = binary.Write(&headerBody, binary.LittleEndian, zm.PayloadCRC32)
	_ = binary.Write(&headerBody, binary.LittleEndian, uint32(len(zm.Instruments)))
	for _, inst := range zm.Instruments {
		_ = binary.Write(&headerBody, binary.LittleEndian, uint16(len(inst)))
		headerBody.WriteString(inst)
	}
	hb := headerBody.Bytes()
	headerCRC := crc32.ChecksumIEEE(hb)
	_ = binary.Write(&out, binary.LittleEndian, uint32(len(hb)))
	_ = binary.Write(&out, binary.LittleEndian, headerCRC)
	out.Write(hb)
	zm.HeaderByteLength = out.Len()
	out.Write(payload)
	return out.Bytes(), zm, nil
}

// ReadZoneMap decodes only the uncompressed ZoneMap header and verifies its CRC32.
func ReadZoneMap(data []byte) (ZoneMap, error) {
	if len(data) < 16 {
		return ZoneMap{}, fmt.Errorf("ERR_SEGMENT_CORRUPT: segment too short (%d bytes)", len(data))
	}
	if !bytes.Equal(data[:8], SegmentMagic[:]) {
		return ZoneMap{}, fmt.Errorf("ERR_SEGMENT_CORRUPT: invalid segment magic")
	}
	headerLen := int(binary.LittleEndian.Uint32(data[8:12]))
	headerCRC := binary.LittleEndian.Uint32(data[12:16])
	if len(data) < 16+headerLen {
		return ZoneMap{}, fmt.Errorf("ERR_SEGMENT_CORRUPT: truncated header")
	}
	hb := data[16 : 16+headerLen]
	if crc32.ChecksumIEEE(hb) != headerCRC {
		return ZoneMap{}, fmt.Errorf("ERR_SEGMENT_CORRUPT: header CRC32 mismatch")
	}

	r := bytes.NewReader(hb)
	var zm ZoneMap
	var err error
	if zm.CodecID, err = r.ReadByte(); err != nil {
		return ZoneMap{}, err
	}
	if err = binary.Read(r, binary.LittleEndian, &zm.RowCount); err != nil {
		return ZoneMap{}, err
	}
	if err = binary.Read(r, binary.LittleEndian, &zm.MinEventTimeNS); err != nil {
		return ZoneMap{}, err
	}
	if err = binary.Read(r, binary.LittleEndian, &zm.MaxEventTimeNS); err != nil {
		return ZoneMap{}, err
	}
	if err = binary.Read(r, binary.LittleEndian, &zm.MinSeq); err != nil {
		return ZoneMap{}, err
	}
	if err = binary.Read(r, binary.LittleEndian, &zm.MaxSeq); err != nil {
		return ZoneMap{}, err
	}
	if err = binary.Read(r, binary.LittleEndian, &zm.UncompressedBytes); err != nil {
		return ZoneMap{}, err
	}
	if err = binary.Read(r, binary.LittleEndian, &zm.CompressedBytes); err != nil {
		return ZoneMap{}, err
	}
	if err = binary.Read(r, binary.LittleEndian, &zm.PayloadCRC32); err != nil {
		return ZoneMap{}, err
	}
	var instCount uint32
	if err = binary.Read(r, binary.LittleEndian, &instCount); err != nil {
		return ZoneMap{}, err
	}
	zm.Instruments = make([]string, instCount)
	for i := uint32(0); i < instCount; i++ {
		var l uint16
		if err = binary.Read(r, binary.LittleEndian, &l); err != nil {
			return ZoneMap{}, err
		}
		buf := make([]byte, l)
		if _, err = io.ReadFull(r, buf); err != nil {
			return ZoneMap{}, err
		}
		zm.Instruments[i] = string(buf)
	}
	zm.HeaderByteLength = 16 + headerLen
	return zm, nil
}

// DecodeSegment verifies checksums, decompresses, and reconstructs all ticks.
func DecodeSegment(data []byte, verifyChecksum bool) ([]domain.CanonicalTick, ZoneMap, error) {
	zm, err := ReadZoneMap(data)
	if err != nil {
		return nil, ZoneMap{}, err
	}
	payloadStart := zm.HeaderByteLength
	if len(data) < payloadStart+int(zm.CompressedBytes) {
		return nil, ZoneMap{}, fmt.Errorf("ERR_SEGMENT_CORRUPT: truncated payload")
	}
	payload := data[payloadStart : payloadStart+int(zm.CompressedBytes)]
	if verifyChecksum && crc32.ChecksumIEEE(payload) != zm.PayloadCRC32 {
		return nil, ZoneMap{}, fmt.Errorf("ERR_SEGMENT_CORRUPT: payload CRC32 mismatch")
	}

	var rawPayload []byte
	switch zm.CodecID {
	case CodecDeltaDictGzip:
		gr, err := gzip.NewReader(bytes.NewReader(payload))
		if err != nil {
			return nil, ZoneMap{}, fmt.Errorf("ERR_SEGMENT_CORRUPT: gzip init failed: %w", err)
		}
		rawPayload, err = io.ReadAll(gr)
		_ = gr.Close()
		if err != nil {
			return nil, ZoneMap{}, fmt.Errorf("ERR_SEGMENT_CORRUPT: gzip read failed: %w", err)
		}
	case CodecDeltaDictRaw:
		rawPayload = payload
	default:
		return nil, ZoneMap{}, fmt.Errorf("ERR_SEGMENT_CORRUPT: unsupported codec %d", zm.CodecID)
	}

	n := int(zm.RowCount)
	if n == 0 {
		return nil, zm, nil
	}

	r := bytes.NewReader(rawPayload)
	eventTimes, err := decodeDeltaOfDeltaI64(r, n)
	if err != nil {
		return nil, ZoneMap{}, err
	}
	ingestTimes, err := decodeDeltaOfDeltaI64(r, n)
	if err != nil {
		return nil, ZoneMap{}, err
	}
	seqs, err := decodeDeltaOfDeltaI64(r, n)
	if err != nil {
		return nil, ZoneMap{}, err
	}
	prices, err := decodeDeltaI64(r, n)
	if err != nil {
		return nil, ZoneMap{}, err
	}
	sizes, err := decodeVarintI64(r, n)
	if err != nil {
		return nil, ZoneMap{}, err
	}
	bidPrices, err := decodeDeltaI64(r, n)
	if err != nil {
		return nil, ZoneMap{}, err
	}
	bidSizes, err := decodeVarintI64(r, n)
	if err != nil {
		return nil, ZoneMap{}, err
	}
	askPrices, err := decodeDeltaI64(r, n)
	if err != nil {
		return nil, ZoneMap{}, err
	}
	askSizes, err := decodeVarintI64(r, n)
	if err != nil {
		return nil, ZoneMap{}, err
	}
	schemaVers, err := decodeDictRLE(r, n)
	if err != nil {
		return nil, ZoneMap{}, err
	}
	sourceIDs, err := decodeDictRLE(r, n)
	if err != nil {
		return nil, ZoneMap{}, err
	}
	feedSess, err := decodeDictRLE(r, n)
	if err != nil {
		return nil, ZoneMap{}, err
	}
	batchIDs, err := decodeDictRLE(r, n)
	if err != nil {
		return nil, ZoneMap{}, err
	}
	instIDs, err := decodeDictRLE(r, n)
	if err != nil {
		return nil, ZoneMap{}, err
	}
	venues, err := decodeDictRLE(r, n)
	if err != nil {
		return nil, ZoneMap{}, err
	}
	partKeys, err := decodeDictRLE(r, n)
	if err != nil {
		return nil, ZoneMap{}, err
	}
	eventTypes, err := decodeByteRLE(r, n)
	if err != nil {
		return nil, ZoneMap{}, err
	}
	sides, err := decodeByteRLE(r, n)
	if err != nil {
		return nil, ZoneMap{}, err
	}
	lateFlags, err := decodeByteRLE(r, n)
	if err != nil {
		return nil, ZoneMap{}, err
	}
	shardIDs, err := decodeDeltaI64(r, n)
	if err != nil {
		return nil, ZoneMap{}, err
	}

	out := make([]domain.CanonicalTick, n)
	for i := 0; i < n; i++ {
		out[i] = domain.CanonicalTick{
			SchemaVersion:  schemaVers[i],
			SourceID:       sourceIDs[i],
			FeedSessionID:  feedSess[i],
			BatchID:        batchIDs[i],
			InstrumentID:   instIDs[i],
			Venue:          venues[i],
			EventType:      domain.EventTypeFromCode(eventTypes[i]),
			Side:           domain.SideFromCode(sides[i]),
			EventTimeNS:    eventTimes[i],
			IngestTimeNS:   ingestTimes[i],
			SequenceNumber: uint64(seqs[i]),
			PriceNanos:     prices[i],
			SizeNanos:      sizes[i],
			BidPriceNanos:  bidPrices[i],
			BidSizeNanos:   bidSizes[i],
			AskPriceNanos:  askPrices[i],
			AskSizeNanos:   askSizes[i],
			LateArrival:    lateFlags[i] != 0,
			ShardID:        uint16(shardIDs[i]),
			PartitionKey:   partKeys[i],
		}
	}
	return out, zm, nil
}

// --- Column encoding helpers ---

func writeVarint(buf *bytes.Buffer, v int64) {
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutVarint(tmp[:], v)
	buf.Write(tmp[:n])
}

func writeUvarint(buf *bytes.Buffer, v uint64) {
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], v)
	buf.Write(tmp[:n])
}

func encodeDeltaOfDeltaI64(buf *bytes.Buffer, vals []int64) {
	if len(vals) == 0 {
		return
	}
	writeVarint(buf, vals[0])
	if len(vals) == 1 {
		return
	}
	prevDelta := vals[1] - vals[0]
	writeVarint(buf, prevDelta)
	for i := 2; i < len(vals); i++ {
		d := vals[i] - vals[i-1]
		dod := d - prevDelta
		writeVarint(buf, dod)
		prevDelta = d
	}
}

func decodeDeltaOfDeltaI64(r *bytes.Reader, n int) ([]int64, error) {
	out := make([]int64, n)
	if n == 0 {
		return out, nil
	}
	v0, err := binary.ReadVarint(r)
	if err != nil {
		return nil, err
	}
	out[0] = v0
	if n == 1 {
		return out, nil
	}
	prevDelta, err := binary.ReadVarint(r)
	if err != nil {
		return nil, err
	}
	out[1] = v0 + prevDelta
	for i := 2; i < n; i++ {
		dod, err := binary.ReadVarint(r)
		if err != nil {
			return nil, err
		}
		d := prevDelta + dod
		out[i] = out[i-1] + d
		prevDelta = d
	}
	return out, nil
}

func encodeDeltaI64(buf *bytes.Buffer, vals []int64) {
	if len(vals) == 0 {
		return
	}
	writeVarint(buf, vals[0])
	for i := 1; i < len(vals); i++ {
		writeVarint(buf, vals[i]-vals[i-1])
	}
}

func decodeDeltaI64(r *bytes.Reader, n int) ([]int64, error) {
	out := make([]int64, n)
	if n == 0 {
		return out, nil
	}
	v0, err := binary.ReadVarint(r)
	if err != nil {
		return nil, err
	}
	out[0] = v0
	for i := 1; i < n; i++ {
		d, err := binary.ReadVarint(r)
		if err != nil {
			return nil, err
		}
		out[i] = out[i-1] + d
	}
	return out, nil
}

func encodeVarintI64(buf *bytes.Buffer, vals []int64) {
	for _, v := range vals {
		writeVarint(buf, v)
	}
}

func decodeVarintI64(r *bytes.Reader, n int) ([]int64, error) {
	out := make([]int64, n)
	for i := 0; i < n; i++ {
		v, err := binary.ReadVarint(r)
		if err != nil {
			return nil, err
		}
		out[i] = v
	}
	return out, nil
}

func encodeDictRLE(buf *bytes.Buffer, vals []string) {
	dictIdx := make(map[string]uint64)
	dict := make([]string, 0)
	ids := make([]uint64, len(vals))
	for i, s := range vals {
		id, ok := dictIdx[s]
		if !ok {
			id = uint64(len(dict))
			dictIdx[s] = id
			dict = append(dict, s)
		}
		ids[i] = id
	}
	writeUvarint(buf, uint64(len(dict)))
	for _, s := range dict {
		writeUvarint(buf, uint64(len(s)))
		buf.WriteString(s)
	}
	// RLE over dictionary IDs
	i := 0
	for i < len(ids) {
		val := ids[i]
		run := uint64(1)
		for i+int(run) < len(ids) && ids[i+int(run)] == val {
			run++
		}
		writeUvarint(buf, val)
		writeUvarint(buf, run)
		i += int(run)
	}
}

func decodeDictRLE(r *bytes.Reader, n int) ([]string, error) {
	dictLen, err := binary.ReadUvarint(r)
	if err != nil {
		return nil, err
	}
	dict := make([]string, dictLen)
	for i := uint64(0); i < dictLen; i++ {
		l, err := binary.ReadUvarint(r)
		if err != nil {
			return nil, err
		}
		b := make([]byte, l)
		if _, err := io.ReadFull(r, b); err != nil {
			return nil, err
		}
		dict[i] = string(b)
	}
	out := make([]string, 0, n)
	for len(out) < n {
		id, err := binary.ReadUvarint(r)
		if err != nil {
			return nil, err
		}
		run, err := binary.ReadUvarint(r)
		if err != nil {
			return nil, err
		}
		if id >= uint64(len(dict)) {
			return nil, fmt.Errorf("ERR_SEGMENT_CORRUPT: dict index out of range")
		}
		for k := uint64(0); k < run && len(out) < n; k++ {
			out = append(out, dict[id])
		}
	}
	return out, nil
}

func encodeByteRLE(buf *bytes.Buffer, vals []byte) {
	i := 0
	for i < len(vals) {
		v := vals[i]
		run := uint64(1)
		for i+int(run) < len(vals) && vals[i+int(run)] == v {
			run++
		}
		buf.WriteByte(v)
		writeUvarint(buf, run)
		i += int(run)
	}
}

func decodeByteRLE(r *bytes.Reader, n int) ([]byte, error) {
	out := make([]byte, 0, n)
	for len(out) < n {
		v, err := r.ReadByte()
		if err != nil {
			return nil, err
		}
		run, err := binary.ReadUvarint(r)
		if err != nil {
			return nil, err
		}
		for k := uint64(0); k < run && len(out) < n; k++ {
			out = append(out, v)
		}
	}
	return out, nil
}
