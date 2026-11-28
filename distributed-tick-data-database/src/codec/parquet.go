package codec

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"

	"distributed-tick-data-database/src/domain"
)

// Apache Parquet v1 spec constants (parquet.thrift)
const (
	parquetMagic = "PAR1"

	pqTypeBoolean   int32 = 0
	pqTypeInt32     int32 = 1
	pqTypeInt64     int32 = 2
	pqTypeByteArray int32 = 6

	pqRepRequired int32 = 0

	pqEncodingPlain int32 = 0

	pqCodecUncompressed int32 = 0
	pqCodecGzip         int32 = 2

	pqPageTypeDataPage int32 = 0
)

// Thrift Compact Protocol wire types
const (
	ctStop   byte = 0
	ctBoolT  byte = 1
	ctBoolF  byte = 2
	ctI32    byte = 5
	ctI64    byte = 6
	ctBinary byte = 8
	ctList   byte = 9
	ctStruct byte = 12
)

type pqColumnSpec struct {
	name string
	typ  int32
}

var tickParquetSchema = []pqColumnSpec{
	{name: "schema_version", typ: pqTypeByteArray},
	{name: "source_id", typ: pqTypeByteArray},
	{name: "feed_session_id", typ: pqTypeByteArray},
	{name: "batch_id", typ: pqTypeByteArray},
	{name: "instrument_id", typ: pqTypeByteArray},
	{name: "venue", typ: pqTypeByteArray},
	{name: "event_type", typ: pqTypeByteArray},
	{name: "side", typ: pqTypeByteArray},
	{name: "event_time_ns", typ: pqTypeInt64},
	{name: "ingest_time_ns", typ: pqTypeInt64},
	{name: "sequence_number", typ: pqTypeInt64},
	{name: "price_nanos", typ: pqTypeInt64},
	{name: "size_nanos", typ: pqTypeInt64},
	{name: "bid_price_nanos", typ: pqTypeInt64},
	{name: "bid_size_nanos", typ: pqTypeInt64},
	{name: "ask_price_nanos", typ: pqTypeInt64},
	{name: "ask_size_nanos", typ: pqTypeInt64},
	{name: "late_arrival", typ: pqTypeBoolean},
	{name: "shard_id", typ: pqTypeInt32},
	{name: "partition_key", typ: pqTypeByteArray},
}

type pqChunkMeta struct {
	colSpec          pqColumnSpec
	codec            int32
	numValues        int64
	uncompressedSize int64
	compressedSize   int64
	dataPageOffset   int64
}

// WriteParquet writes ticks into a spec-compliant Apache Parquet 1.0 binary
// stream (PAR1 magic, PLAIN+GZIP DataPage v1 chunks, Thrift Compact Protocol
// FileMetaData footer, footer length, and trailing PAR1 magic).
func WriteParquet(w io.Writer, ticks []domain.CanonicalTick) (int64, error) {
	var out bytes.Buffer
	out.WriteString(parquetMagic)

	numRows := int64(len(ticks))
	chunks := make([]pqChunkMeta, 0, len(tickParquetSchema))
	var totalRowGroupBytes int64

	if numRows > 0 {
		for colIdx, spec := range tickParquetSchema {
			plain := encodeParquetPlainColumn(colIdx, ticks)

			var gz bytes.Buffer
			gw, err := gzip.NewWriterLevel(&gz, gzip.BestSpeed)
			if err != nil {
				return 0, err
			}
			if _, err := gw.Write(plain); err != nil {
				_ = gw.Close()
				return 0, err
			}
			if err := gw.Close(); err != nil {
				return 0, err
			}
			compressed := gz.Bytes()
			pageCRC := int32(crc32.ChecksumIEEE(compressed))

			pageHeader := encodePageHeader(int32(numRows), int32(len(plain)), int32(len(compressed)), pageCRC)

			offset := int64(out.Len())
			out.Write(pageHeader)
			out.Write(compressed)

			uncompTotal := int64(len(pageHeader) + len(plain))
			compTotal := int64(len(pageHeader) + len(compressed))
			totalRowGroupBytes += uncompTotal

			chunks = append(chunks, pqChunkMeta{
				colSpec:          spec,
				codec:            pqCodecGzip,
				numValues:        numRows,
				uncompressedSize: uncompTotal,
				compressedSize:   compTotal,
				dataPageOffset:   offset,
			})
		}
	}

	footer := encodeFileMetaData(numRows, totalRowGroupBytes, chunks)
	out.Write(footer)
	_ = binary.Write(&out, binary.LittleEndian, uint32(len(footer)))
	out.WriteString(parquetMagic)

	n, err := w.Write(out.Bytes())
	return int64(n), err
}

// ReadParquet parses a Parquet 1.0 file produced by WriteParquet, validating
// magic bytes, Thrift Compact FileMetaData, page CRC32, and decompressing all
// columns back into CanonicalTick records.
func ReadParquet(data []byte) ([]domain.CanonicalTick, error) {
	if len(data) < 12 {
		return nil, fmt.Errorf("ERR_PARQUET_CORRUPT: file too small (%d bytes)", len(data))
	}
	if string(data[:4]) != parquetMagic || string(data[len(data)-4:]) != parquetMagic {
		return nil, fmt.Errorf("ERR_PARQUET_CORRUPT: invalid PAR1 magic bytes")
	}
	footerLen := int(binary.LittleEndian.Uint32(data[len(data)-8 : len(data)-4]))
	if footerLen <= 0 || 4+footerLen+8 > len(data) {
		return nil, fmt.Errorf("ERR_PARQUET_CORRUPT: invalid footer length %d", footerLen)
	}
	footerStart := len(data) - 8 - footerLen
	meta, err := decodeFileMetaData(data[footerStart : len(data)-8])
	if err != nil {
		return nil, fmt.Errorf("ERR_PARQUET_CORRUPT: footer decode failed: %w", err)
	}
	if meta.numRows == 0 {
		return []domain.CanonicalTick{}, nil
	}
	if len(meta.chunks) != len(tickParquetSchema) {
		return nil, fmt.Errorf("ERR_PARQUET_CORRUPT: expected %d columns, got %d", len(tickParquetSchema), len(meta.chunks))
	}

	ticks := make([]domain.CanonicalTick, meta.numRows)
	for colIdx, ch := range meta.chunks {
		if ch.dataPageOffset < 4 || int(ch.dataPageOffset) >= footerStart {
			return nil, fmt.Errorf("ERR_PARQUET_CORRUPT: invalid page offset for column %d", colIdx)
		}
		pr := newCompactReader(data[ch.dataPageOffset:footerStart])
		ph, err := decodePageHeader(pr)
		if err != nil {
			return nil, fmt.Errorf("ERR_PARQUET_CORRUPT: page header col %d: %w", colIdx, err)
		}
		pageStart := int(ch.dataPageOffset) + pr.pos
		pageEnd := pageStart + int(ph.compressedSize)
		if pageEnd > footerStart {
			return nil, fmt.Errorf("ERR_PARQUET_CORRUPT: truncated page col %d", colIdx)
		}
		compBytes := data[pageStart:pageEnd]
		if ph.hasCRC && int32(crc32.ChecksumIEEE(compBytes)) != ph.crc {
			return nil, fmt.Errorf("ERR_PARQUET_CORRUPT: page CRC mismatch col %d", colIdx)
		}
		var plain []byte
		switch ch.codec {
		case pqCodecGzip:
			gr, err := gzip.NewReader(bytes.NewReader(compBytes))
			if err != nil {
				return nil, err
			}
			plain, err = io.ReadAll(gr)
			_ = gr.Close()
			if err != nil {
				return nil, err
			}
		case pqCodecUncompressed:
			plain = compBytes
		default:
			return nil, fmt.Errorf("ERR_PARQUET_CORRUPT: unsupported codec %d", ch.codec)
		}
		if err := decodeParquetPlainColumn(colIdx, plain, ticks); err != nil {
			return nil, err
		}
	}
	return ticks, nil
}

func encodeParquetPlainColumn(colIdx int, ticks []domain.CanonicalTick) []byte {
	var buf bytes.Buffer
	writeStr := func(s string) {
		_ = binary.Write(&buf, binary.LittleEndian, uint32(len(s)))
		buf.WriteString(s)
	}
	writeI64 := func(v int64) {
		_ = binary.Write(&buf, binary.LittleEndian, v)
	}
	writeI32 := func(v int32) {
		_ = binary.Write(&buf, binary.LittleEndian, v)
	}
	switch colIdx {
	case 0:
		for _, t := range ticks {
			writeStr(t.SchemaVersion)
		}
	case 1:
		for _, t := range ticks {
			writeStr(t.SourceID)
		}
	case 2:
		for _, t := range ticks {
			writeStr(t.FeedSessionID)
		}
	case 3:
		for _, t := range ticks {
			writeStr(t.BatchID)
		}
	case 4:
		for _, t := range ticks {
			writeStr(t.InstrumentID)
		}
	case 5:
		for _, t := range ticks {
			writeStr(t.Venue)
		}
	case 6:
		for _, t := range ticks {
			writeStr(string(t.EventType))
		}
	case 7:
		for _, t := range ticks {
			writeStr(string(t.Side))
		}
	case 8:
		for _, t := range ticks {
			writeI64(t.EventTimeNS)
		}
	case 9:
		for _, t := range ticks {
			writeI64(t.IngestTimeNS)
		}
	case 10:
		for _, t := range ticks {
			writeI64(int64(t.SequenceNumber))
		}
	case 11:
		for _, t := range ticks {
			writeI64(t.PriceNanos)
		}
	case 12:
		for _, t := range ticks {
			writeI64(t.SizeNanos)
		}
	case 13:
		for _, t := range ticks {
			writeI64(t.BidPriceNanos)
		}
	case 14:
		for _, t := range ticks {
			writeI64(t.BidSizeNanos)
		}
	case 15:
		for _, t := range ticks {
			writeI64(t.AskPriceNanos)
		}
	case 16:
		for _, t := range ticks {
			writeI64(t.AskSizeNanos)
		}
	case 17:
		// Parquet PLAIN BOOLEAN: bit-packed LSB-first
		numBytes := (len(ticks) + 7) / 8
		packed := make([]byte, numBytes)
		for i, t := range ticks {
			if t.LateArrival {
				packed[i/8] |= 1 << (uint(i) % 8)
			}
		}
		buf.Write(packed)
	case 18:
		for _, t := range ticks {
			writeI32(int32(t.ShardID))
		}
	case 19:
		for _, t := range ticks {
			writeStr(t.PartitionKey)
		}
	}
	return buf.Bytes()
}

func decodeParquetPlainColumn(colIdx int, plain []byte, ticks []domain.CanonicalTick) error {
	r := bytes.NewReader(plain)
	readStr := func() (string, error) {
		var l uint32
		if err := binary.Read(r, binary.LittleEndian, &l); err != nil {
			return "", err
		}
		b := make([]byte, l)
		if _, err := io.ReadFull(r, b); err != nil {
			return "", err
		}
		return string(b), nil
	}
	readI64 := func() (int64, error) {
		var v int64
		err := binary.Read(r, binary.LittleEndian, &v)
		return v, err
	}
	readI32 := func() (int32, error) {
		var v int32
		err := binary.Read(r, binary.LittleEndian, &v)
		return v, err
	}
	for i := range ticks {
		var err error
		switch colIdx {
		case 0:
			ticks[i].SchemaVersion, err = readStr()
		case 1:
			ticks[i].SourceID, err = readStr()
		case 2:
			ticks[i].FeedSessionID, err = readStr()
		case 3:
			ticks[i].BatchID, err = readStr()
		case 4:
			ticks[i].InstrumentID, err = readStr()
		case 5:
			ticks[i].Venue, err = readStr()
		case 6:
			var s string
			s, err = readStr()
			ticks[i].EventType = domain.EventType(s)
		case 7:
			var s string
			s, err = readStr()
			ticks[i].Side = domain.Side(s)
		case 8:
			ticks[i].EventTimeNS, err = readI64()
		case 9:
			ticks[i].IngestTimeNS, err = readI64()
		case 10:
			var v int64
			v, err = readI64()
			ticks[i].SequenceNumber = uint64(v)
		case 11:
			ticks[i].PriceNanos, err = readI64()
		case 12:
			ticks[i].SizeNanos, err = readI64()
		case 13:
			ticks[i].BidPriceNanos, err = readI64()
		case 14:
			ticks[i].BidSizeNanos, err = readI64()
		case 15:
			ticks[i].AskPriceNanos, err = readI64()
		case 16:
			ticks[i].AskSizeNanos, err = readI64()
		case 17:
			if i/8 >= len(plain) {
				return fmt.Errorf("ERR_PARQUET_CORRUPT: boolean column truncated")
			}
			ticks[i].LateArrival = (plain[i/8] & (1 << (uint(i) % 8))) != 0
		case 18:
			var v int32
			v, err = readI32()
			ticks[i].ShardID = uint16(v)
		case 19:
			ticks[i].PartitionKey, err = readStr()
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// --- Minimal Thrift Compact Protocol Encoder/Decoder for Parquet ---

type compactWriter struct {
	buf       bytes.Buffer
	lastField int16
	stack     []int16
}

func (w *compactWriter) beginStruct() {
	w.stack = append(w.stack, w.lastField)
	w.lastField = 0
}

func (w *compactWriter) endStruct() {
	w.buf.WriteByte(ctStop)
	w.lastField = w.stack[len(w.stack)-1]
	w.stack = w.stack[:len(w.stack)-1]
}

func (w *compactWriter) writeFieldHeader(fieldID int16, ftype byte) {
	delta := fieldID - w.lastField
	if delta > 0 && delta <= 15 {
		w.buf.WriteByte(byte(delta<<4) | ftype)
	} else {
		w.buf.WriteByte(ftype)
		w.writeZigZag(int64(fieldID))
	}
	w.lastField = fieldID
}

func (w *compactWriter) writeUvarint(v uint64) {
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], v)
	w.buf.Write(tmp[:n])
}

func (w *compactWriter) writeZigZag(v int64) {
	zz := uint64((v << 1) ^ (v >> 63))
	w.writeUvarint(zz)
}

func (w *compactWriter) writeI32Field(fieldID int16, v int32) {
	w.writeFieldHeader(fieldID, ctI32)
	w.writeZigZag(int64(v))
}

func (w *compactWriter) writeI64Field(fieldID int16, v int64) {
	w.writeFieldHeader(fieldID, ctI64)
	w.writeZigZag(v)
}

func (w *compactWriter) writeStringField(fieldID int16, s string) {
	w.writeFieldHeader(fieldID, ctBinary)
	w.writeUvarint(uint64(len(s)))
	w.buf.WriteString(s)
}

func (w *compactWriter) writeListHeader(fieldID int16, elemType byte, size int) {
	w.writeFieldHeader(fieldID, ctList)
	if size < 15 {
		w.buf.WriteByte(byte(size<<4) | elemType)
	} else {
		w.buf.WriteByte(0xF0 | elemType)
		w.writeUvarint(uint64(size))
	}
}

func encodePageHeader(numValues, uncompSize, compSize, crcVal int32) []byte {
	var w compactWriter
	w.beginStruct()
	w.writeI32Field(1, pqPageTypeDataPage)
	w.writeI32Field(2, uncompSize)
	w.writeI32Field(3, compSize)
	w.writeI32Field(4, crcVal)
	// Field 5: DataPageHeader struct
	w.writeFieldHeader(5, ctStruct)
	w.beginStruct()
	w.writeI32Field(1, numValues)
	w.writeI32Field(2, pqEncodingPlain)
	w.writeI32Field(3, pqEncodingPlain)
	w.writeI32Field(4, pqEncodingPlain)
	w.endStruct()
	w.endStruct()
	return w.buf.Bytes()
}

func encodeFileMetaData(numRows, totalRowGroupBytes int64, chunks []pqChunkMeta) []byte {
	var w compactWriter
	w.beginStruct()
	w.writeI32Field(1, 1) // version = 1

	// Field 2: schema list<SchemaElement> (root + leaf columns)
	w.writeListHeader(2, ctStruct, 1+len(tickParquetSchema))
	// Root element
	w.beginStruct()
	w.writeStringField(4, "market_ticks")
	w.writeI32Field(5, int32(len(tickParquetSchema)))
	w.endStruct()
	// Leaf columns
	for _, col := range tickParquetSchema {
		w.beginStruct()
		w.writeI32Field(1, col.typ)
		w.writeI32Field(3, pqRepRequired)
		w.writeStringField(4, col.name)
		w.endStruct()
	}

	// Field 3: num_rows
	w.writeI64Field(3, numRows)

	// Field 4: row_groups
	if numRows > 0 {
		w.writeListHeader(4, ctStruct, 1)
		w.beginStruct()
		// RowGroup field 1: columns list<ColumnChunk>
		w.writeListHeader(1, ctStruct, len(chunks))
		for _, ch := range chunks {
			w.beginStruct()
			// ColumnChunk field 2: file_offset
			w.writeI64Field(2, ch.dataPageOffset)
			// ColumnChunk field 3: meta_data (ColumnMetaData)
			w.writeFieldHeader(3, ctStruct)
			w.beginStruct()
			w.writeI32Field(1, ch.colSpec.typ)
			// encodings list<i32>
			w.writeListHeader(2, ctI32, 1)
			w.writeZigZag(int64(pqEncodingPlain))
			// path_in_schema list<string>
			w.writeListHeader(3, ctBinary, 1)
			w.writeUvarint(uint64(len(ch.colSpec.name)))
			w.buf.WriteString(ch.colSpec.name)
			w.writeI32Field(4, ch.codec)
			w.writeI64Field(5, ch.numValues)
			w.writeI64Field(6, ch.uncompressedSize)
			w.writeI64Field(7, ch.compressedSize)
			w.writeI64Field(9, ch.dataPageOffset)
			w.endStruct()
			w.endStruct()
		}
		w.writeI64Field(2, totalRowGroupBytes)
		w.writeI64Field(3, numRows)
		w.endStruct()
	} else {
		w.writeListHeader(4, ctStruct, 0)
	}

	// Field 6: created_by
	w.writeStringField(6, "distributed-tick-data-database/1.0.0")
	w.endStruct()
	return w.buf.Bytes()
}

// --- Thrift Compact Protocol Reader ---

type compactReader struct {
	data []byte
	pos  int
}

func newCompactReader(data []byte) *compactReader {
	return &compactReader{data: data}
}

func (r *compactReader) readByte() (byte, error) {
	if r.pos >= len(r.data) {
		return 0, io.ErrUnexpectedEOF
	}
	b := r.data[r.pos]
	r.pos++
	return b, nil
}

func (r *compactReader) readUvarint() (uint64, error) {
	v, n := binary.Uvarint(r.data[r.pos:])
	if n <= 0 {
		return 0, fmt.Errorf("invalid uvarint")
	}
	r.pos += n
	return v, nil
}

func (r *compactReader) readZigZag() (int64, error) {
	u, err := r.readUvarint()
	if err != nil {
		return 0, err
	}
	return int64((u >> 1) ^ uint64(-(int64(u & 1)))), nil
}

func (r *compactReader) readString() (string, error) {
	l, err := r.readUvarint()
	if err != nil {
		return "", err
	}
	if r.pos+int(l) > len(r.data) {
		return "", io.ErrUnexpectedEOF
	}
	s := string(r.data[r.pos : r.pos+int(l)])
	r.pos += int(l)
	return s, nil
}

func (r *compactReader) readFieldHeader(lastField int16) (int16, byte, bool, error) {
	b, err := r.readByte()
	if err != nil {
		return 0, 0, false, err
	}
	if b == ctStop {
		return 0, ctStop, true, nil
	}
	ftype := b & 0x0F
	delta := int16((b >> 4) & 0x0F)
	var fieldID int16
	if delta == 0 {
		id, err := r.readZigZag()
		if err != nil {
			return 0, 0, false, err
		}
		fieldID = int16(id)
	} else {
		fieldID = lastField + delta
	}
	return fieldID, ftype, false, nil
}

func (r *compactReader) readListHeader() (byte, int, error) {
	b, err := r.readByte()
	if err != nil {
		return 0, 0, err
	}
	size := int((b >> 4) & 0x0F)
	elemType := b & 0x0F
	if size == 15 {
		u, err := r.readUvarint()
		if err != nil {
			return 0, 0, err
		}
		size = int(u)
	}
	return elemType, size, nil
}

func (r *compactReader) skipValue(ftype byte) error {
	switch ftype {
	case ctBoolT, ctBoolF:
		return nil
	case ctI32, ctI64:
		_, err := r.readZigZag()
		return err
	case ctBinary:
		_, err := r.readString()
		return err
	case ctList:
		et, sz, err := r.readListHeader()
		if err != nil {
			return err
		}
		for i := 0; i < sz; i++ {
			if err := r.skipValue(et); err != nil {
				return err
			}
		}
		return nil
	case ctStruct:
		var last int16
		for {
			fid, ft, stop, err := r.readFieldHeader(last)
			if err != nil {
				return err
			}
			if stop {
				return nil
			}
			last = fid
			if err := r.skipValue(ft); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("unsupported thrift type %d", ftype)
	}
}

type decodedPageHeader struct {
	uncompressedSize int32
	compressedSize   int32
	crc              int32
	hasCRC           bool
}

func decodePageHeader(r *compactReader) (decodedPageHeader, error) {
	var ph decodedPageHeader
	var last int16
	for {
		fid, ft, stop, err := r.readFieldHeader(last)
		if err != nil {
			return ph, err
		}
		if stop {
			return ph, nil
		}
		last = fid
		switch fid {
		case 2:
			v, err := r.readZigZag()
			if err != nil {
				return ph, err
			}
			ph.uncompressedSize = int32(v)
		case 3:
			v, err := r.readZigZag()
			if err != nil {
				return ph, err
			}
			ph.compressedSize = int32(v)
		case 4:
			v, err := r.readZigZag()
			if err != nil {
				return ph, err
			}
			ph.crc = int32(v)
			ph.hasCRC = true
		default:
			if err := r.skipValue(ft); err != nil {
				return ph, err
			}
		}
	}
}

type decodedFileMeta struct {
	numRows int64
	chunks  []pqChunkMeta
}

func decodeFileMetaData(data []byte) (decodedFileMeta, error) {
	r := newCompactReader(data)
	var meta decodedFileMeta
	var last int16
	for {
		fid, ft, stop, err := r.readFieldHeader(last)
		if err != nil {
			return meta, err
		}
		if stop {
			return meta, nil
		}
		last = fid
		switch fid {
		case 3:
			v, err := r.readZigZag()
			if err != nil {
				return meta, err
			}
			meta.numRows = v
		case 4:
			_, rgCount, err := r.readListHeader()
			if err != nil {
				return meta, err
			}
			for g := 0; g < rgCount; g++ {
				chunks, err := decodeRowGroup(r)
				if err != nil {
					return meta, err
				}
				meta.chunks = append(meta.chunks, chunks...)
			}
		default:
			if err := r.skipValue(ft); err != nil {
				return meta, err
			}
		}
	}
}

func decodeRowGroup(r *compactReader) ([]pqChunkMeta, error) {
	var chunks []pqChunkMeta
	var last int16
	for {
		fid, ft, stop, err := r.readFieldHeader(last)
		if err != nil {
			return nil, err
		}
		if stop {
			return chunks, nil
		}
		last = fid
		if fid == 1 {
			_, colCount, err := r.readListHeader()
			if err != nil {
				return nil, err
			}
			for c := 0; c < colCount; c++ {
				ch, err := decodeColumnChunk(r)
				if err != nil {
					return nil, err
				}
				chunks = append(chunks, ch)
			}
		} else {
			if err := r.skipValue(ft); err != nil {
				return nil, err
			}
		}
	}
}

func decodeColumnChunk(r *compactReader) (pqChunkMeta, error) {
	var ch pqChunkMeta
	var last int16
	for {
		fid, ft, stop, err := r.readFieldHeader(last)
		if err != nil {
			return ch, err
		}
		if stop {
			return ch, nil
		}
		last = fid
		switch fid {
		case 2:
			v, err := r.readZigZag()
			if err != nil {
				return ch, err
			}
			ch.dataPageOffset = v
		case 3:
			if err := decodeColumnMetaData(r, &ch); err != nil {
				return ch, err
			}
		default:
			if err := r.skipValue(ft); err != nil {
				return ch, err
			}
		}
	}
}

func decodeColumnMetaData(r *compactReader, ch *pqChunkMeta) error {
	var last int16
	for {
		fid, ft, stop, err := r.readFieldHeader(last)
		if err != nil {
			return err
		}
		if stop {
			return nil
		}
		last = fid
		switch fid {
		case 1:
			v, err := r.readZigZag()
			if err != nil {
				return err
			}
			ch.colSpec.typ = int32(v)
		case 4:
			v, err := r.readZigZag()
			if err != nil {
				return err
			}
			ch.codec = int32(v)
		case 5:
			v, err := r.readZigZag()
			if err != nil {
				return err
			}
			ch.numValues = v
		case 6:
			v, err := r.readZigZag()
			if err != nil {
				return err
			}
			ch.uncompressedSize = v
		case 7:
			v, err := r.readZigZag()
			if err != nil {
				return err
			}
			ch.compressedSize = v
		case 9:
			v, err := r.readZigZag()
			if err != nil {
				return err
			}
			ch.dataPageOffset = v
		default:
			if err := r.skipValue(ft); err != nil {
				return err
			}
		}
	}
}
