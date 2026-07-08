// Package resp implements a minimal, spec-correct RESP codec for proxy use.
// On the read side it accepts both multi-bulk arrays (`*N\r\n$L\r\n...`) and
// inline commands (`PING\r\n`). On the write side it emits RESP2 replies,
// which every client understands.
package resp

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// ErrProtocol is returned for any malformed RESP framing.
var ErrProtocol = errors.New("resp: protocol error")

// Command is a parsed client command. Args[0] is the command name and the
// rest are its payload. The byte slices are freshly allocated per read, so
// they are safe to retain.
type Command struct {
	Args [][]byte
}

// Name returns the upper-cased command name (Args[0]).
func (c *Command) Name() string {
	if len(c.Args) == 0 {
		return ""
	}
	return strings.ToUpper(string(c.Args[0]))
}

// Arg returns Args[i] as a string, or "" if i is out of range.
func (c *Command) Arg(i int) string {
	if i < 0 || i >= len(c.Args) {
		return ""
	}
	return string(c.Args[i])
}

// ArgBytes returns the raw bytes of Args[i], or nil if i is out of range.
func (c *Command) ArgBytes(i int) []byte {
	if i < 0 || i >= len(c.Args) {
		return nil
	}
	return c.Args[i]
}

// Reader decodes client commands from an underlying stream.
type Reader struct {
	br *bufio.Reader
}

// NewReader wraps r with a buffered RESP reader.
func NewReader(r io.Reader) *Reader {
	return &Reader{br: bufio.NewReaderSize(r, 16<<10)}
}

// ReadCommand reads exactly one client command.
func (r *Reader) ReadCommand() (*Command, error) {
	line, err := r.readLine()
	if err != nil {
		return nil, err
	}
	if len(line) == 0 {
		return nil, ErrProtocol
	}
	if line[0] != '*' {
		// Inline command, e.g. "PING\r\n" or "SET k v\r\n".
		return parseInline(line), nil
	}

	n, err := strconv.Atoi(string(line[1:]))
	if err != nil || n < 1 {
		return nil, fmt.Errorf("%w: bad multi-bulk count", ErrProtocol)
	}
	args := make([][]byte, n)
	for i := 0; i < n; i++ {
		head, err := r.readLine()
		if err != nil {
			return nil, err
		}
		if len(head) == 0 || head[0] != '$' {
			return nil, fmt.Errorf("%w: expected bulk string", ErrProtocol)
		}
		l, err := strconv.Atoi(string(head[1:]))
		if err != nil || l < 0 {
			return nil, fmt.Errorf("%w: bad bulk length", ErrProtocol)
		}
		buf := make([]byte, l)
		if _, err := io.ReadFull(r.br, buf); err != nil {
			return nil, err
		}
		if _, err := r.br.Discard(2); err != nil { // trailing CRLF
			return nil, err
		}
		args[i] = buf
	}
	return &Command{Args: args}, nil
}

// readLine reads through the next CRLF and returns the line without it.
func (r *Reader) readLine() ([]byte, error) {
	line, err := r.br.ReadBytes('\n')
	if err != nil {
		return nil, err
	}
	if len(line) < 2 || line[len(line)-2] != '\r' {
		return nil, fmt.Errorf("%w: missing CRLF", ErrProtocol)
	}
	return line[:len(line)-2], nil
}

func parseInline(line []byte) *Command {
	parts := strings.Fields(string(line))
	args := make([][]byte, len(parts))
	for i, p := range parts {
		args[i] = []byte(p)
	}
	return &Command{Args: args}
}

// Writer encodes RESP2 replies. The methods are intentionally small and
// independent so callers can mix structured replies with raw passthrough.
type Writer struct {
	bw *bufio.Writer
}

// NewWriter wraps w with a buffered RESP writer.
func NewWriter(w io.Writer) *Writer {
	return &Writer{bw: bufio.NewWriterSize(w, 16<<10)}
}

// Flush pushes buffered bytes to the underlying writer.
func (w *Writer) Flush() error { return w.bw.Flush() }

// WriteSimpleString writes a `+OK`-style reply.
func (w *Writer) WriteSimpleString(s string) error {
	_, err := fmt.Fprintf(w.bw, "+%s\r\n", s)
	return err
}

// WriteError writes a `-ERR ...`-style reply.
func (w *Writer) WriteError(s string) error {
	_, err := fmt.Fprintf(w.bw, "-%s\r\n", s)
	return err
}

// WriteInt writes an integer reply.
func (w *Writer) WriteInt(n int64) error {
	_, err := fmt.Fprintf(w.bw, ":%d\r\n", n)
	return err
}

// WriteBulkString writes a bulk string, or a null bulk if b is nil.
func (w *Writer) WriteBulkString(b []byte) error {
	if b == nil {
		return w.WriteNullBulk()
	}
	if _, err := fmt.Fprintf(w.bw, "$%d\r\n", len(b)); err != nil {
		return err
	}
	if _, err := w.bw.Write(b); err != nil {
		return err
	}
	_, err := w.bw.WriteString("\r\n")
	return err
}

// WriteNullBulk writes the RESP2 null bulk string.
func (w *Writer) WriteNullBulk() error {
	_, err := w.bw.WriteString("$-1\r\n")
	return err
}

// WriteArrayHeader writes an array length prefix; n elements must follow.
func (w *Writer) WriteArrayHeader(n int) error {
	_, err := fmt.Fprintf(w.bw, "*%d\r\n", n)
	return err
}

// WriteNullArray writes the RESP2 null array.
func (w *Writer) WriteNullArray() error {
	_, err := w.bw.WriteString("*-1\r\n")
	return err
}

// WriteRaw writes a pre-encoded RESP frame verbatim (backend passthrough).
func (w *Writer) WriteRaw(b []byte) error {
	_, err := w.bw.Write(b)
	return err
}

// EncodeCommand serialises args as a RESP multi-bulk array, used to re-emit a
// command to a backend connection.
func EncodeCommand(args ...[]byte) []byte {
	var buf strings.Builder
	fmt.Fprintf(&buf, "*%d\r\n", len(args))
	for _, a := range args {
		fmt.Fprintf(&buf, "$%d\r\n", len(a))
		buf.Write(a)
		buf.WriteString("\r\n")
	}
	return []byte(buf.String())
}

// EncodeStrings is a string-args convenience wrapper over EncodeCommand.
func EncodeStrings(args ...string) []byte {
	bs := make([][]byte, len(args))
	for i, a := range args {
		bs[i] = []byte(a)
	}
	return EncodeCommand(bs...)
}
