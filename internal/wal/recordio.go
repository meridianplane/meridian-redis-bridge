// Package wal: recordio is the on-disk record codec used by the WAL. A record
// is a length-prefixed, CRC-tailed byte blob:
//
//	<varint payload_size><body><crc32>
//
// The codec is deliberately body-agnostic: callers marshal/unmarshal their own
// protobuf (a WAL entry, an ownership claim) into and out of the body. The one
// property both callers rely on is crash safety: a process that dies mid-append
// leaves a truncated final record, and Scan treats that partial tail as a clean
// end of file, so recovery stops exactly at the last fully-durable record. A
// CRC mismatch on an otherwise complete record is instead reported as an error,
// since it signals corruption rather than a torn tail.
package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
)

// errTruncated marks a partial record at end of file (a torn tail from a crash
// mid-append). Scan converts it to a clean stop.
var errTruncated = errors.New(" truncated record")

// AppendRecord writes one length-prefixed, CRC-tailed record carrying body to
// w. Callers typically pass an *os.File opened for append and Sync it after.
func AppendRecord(w io.Writer, body []byte) error {
	var hdr [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(hdr[:], uint64(len(body)))
	if _, err := w.Write(hdr[:n]); err != nil {
		return err
	}
	if _, err := w.Write(body); err != nil {
		return err
	}
	var crc [4]byte
	binary.BigEndian.PutUint32(crc[:], crc32.ChecksumIEEE(body))
	_, err := w.Write(crc[:])
	return err
}

// Scan iterates the records in r from its current position, calling fn with
// each record's body. Iteration stops early (without error) if fn returns
// false. A truncated tail or end of file ends the scan cleanly; a CRC mismatch
// or malformed length returns an error. The body passed to fn is owned by the
// callee for the duration of the call only.
func Scan(r io.Reader, fn func(body []byte) (bool, error)) error {
	rr := newReader(r)
	for {
		body, err := rr.readRecord()
		if errors.Is(err, io.EOF) || errors.Is(err, errTruncated) {
			return nil
		}
		if err != nil {
			return err
		}
		cont, err := fn(body)
		if err != nil {
			return err
		}
		if !cont {
			return nil
		}
	}
}

// reader is a buffered varint+payload+crc record reader. Its buffer grows to
// fit any single record larger than the initial size.
type reader struct {
	r       io.Reader
	buf     []byte
	bufPos  int
	bufFill int
}

func newReader(r io.Reader) *reader { return &reader{r: r, buf: make([]byte, 64*1024)} }

func (r *reader) ensure(n int) error {
	for r.bufFill-r.bufPos < n {
		if r.bufPos > 0 {
			copy(r.buf, r.buf[r.bufPos:r.bufFill])
			r.bufFill -= r.bufPos
			r.bufPos = 0
		}
		if r.bufFill == len(r.buf) {
			grown := make([]byte, len(r.buf)*2)
			copy(grown, r.buf[:r.bufFill])
			r.buf = grown
		}
		nr, err := r.r.Read(r.buf[r.bufFill:])
		if nr > 0 {
			r.bufFill += nr
			continue
		}
		if errors.Is(err, io.EOF) {
			if r.bufFill-r.bufPos == 0 {
				return io.EOF
			}
			return errTruncated
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *reader) readRecord() ([]byte, error) {
	if err := r.ensure(1); err != nil {
		return nil, err
	}
	for {
		l, n := binary.Uvarint(r.buf[r.bufPos:r.bufFill])
		if n > 0 {
			r.bufPos += n
			if err := r.ensure(int(l) + 4); err != nil {
				return nil, errTruncated
			}
			body := make([]byte, l)
			copy(body, r.buf[r.bufPos:r.bufPos+int(l)])
			r.bufPos += int(l)
			gotCRC := binary.BigEndian.Uint32(r.buf[r.bufPos : r.bufPos+4])
			r.bufPos += 4
			if gotCRC != crc32.ChecksumIEEE(body) {
				return nil, fmt.Errorf(" CRC mismatch")
			}
			return body, nil
		}
		if n == 0 {
			if err := r.ensure(r.bufFill - r.bufPos + 1); err != nil {
				return nil, errTruncated
			}
			continue
		}
		return nil, fmt.Errorf(" varint overflow")
	}
}
