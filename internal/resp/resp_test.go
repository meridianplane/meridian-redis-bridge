package resp_test

import (
	"bytes"
	"strings"
	"testing"

	"github.com/meridianplane/meridian-redis-bridge/internal/resp"
)

func readOne(t *testing.T, wire string) *resp.Command {
	t.Helper()
	c, err := resp.NewReader(strings.NewReader(wire)).ReadCommand()
	if err != nil {
		t.Fatalf("ReadCommand(%q) error: %v", wire, err)
	}
	return c
}

func TestReadCommand_MultiBulk(t *testing.T) {
	c := readOne(t, "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n")
	if c.Name() != "SET" {
		t.Fatalf("Name = %q, want SET", c.Name())
	}
	if c.Arg(1) != "foo" || c.Arg(2) != "bar" {
		t.Fatalf("args = %q,%q want foo,bar", c.Arg(1), c.Arg(2))
	}
}

func TestReadCommand_BinarySafe(t *testing.T) {
	// A value containing CRLF and NUL must survive intact.
	val := "a\r\nb\x00c"
	wire := "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$" + itoa(len(val)) + "\r\n" + val + "\r\n"
	c := readOne(t, wire)
	if !bytes.Equal(c.ArgBytes(2), []byte(val)) {
		t.Fatalf("binary value corrupted: %q", c.ArgBytes(2))
	}
}

func TestReadCommand_Inline(t *testing.T) {
	c := readOne(t, "PING\r\n")
	if c.Name() != "PING" || len(c.Args) != 1 {
		t.Fatalf("inline PING parsed wrong: %+v", c.Args)
	}
	c = readOne(t, "set k v\r\n")
	if c.Name() != "SET" || c.Arg(1) != "k" || c.Arg(2) != "v" {
		t.Fatalf("inline SET parsed wrong: %+v", c.Args)
	}
}

func TestReadCommand_NameUpperCased(t *testing.T) {
	if got := readOne(t, "*1\r\n$4\r\npInG\r\n").Name(); got != "PING" {
		t.Fatalf("Name = %q, want PING", got)
	}
}

func TestReadCommand_Errors(t *testing.T) {
	cases := []string{
		"*0\r\n",             // zero-length multi-bulk
		"*x\r\n",             // bad count
		"*1\r\n+notbulk\r\n", // element is not a bulk string
		"*1\r\n$x\r\n",       // bad bulk length
		"PING\n",             // missing CR
	}
	for _, wire := range cases {
		if _, err := resp.NewReader(strings.NewReader(wire)).ReadCommand(); err == nil {
			t.Fatalf("ReadCommand(%q) = nil error, want failure", wire)
		}
	}
}

func TestCommand_ArgOutOfRange(t *testing.T) {
	c := &resp.Command{Args: [][]byte{[]byte("GET")}}
	if c.Arg(5) != "" {
		t.Fatal("Arg out of range should be empty string")
	}
	if c.ArgBytes(5) != nil {
		t.Fatal("ArgBytes out of range should be nil")
	}
	empty := &resp.Command{}
	if empty.Name() != "" {
		t.Fatal("Name of empty command should be empty")
	}
}

func TestWriter_Replies(t *testing.T) {
	cases := []struct {
		name string
		fn   func(*resp.Writer) error
		want string
	}{
		{"simple", func(w *resp.Writer) error { return w.WriteSimpleString("OK") }, "+OK\r\n"},
		{"error", func(w *resp.Writer) error { return w.WriteError("ERR boom") }, "-ERR boom\r\n"},
		{"int", func(w *resp.Writer) error { return w.WriteInt(42) }, ":42\r\n"},
		{"bulk", func(w *resp.Writer) error { return w.WriteBulkString([]byte("hi")) }, "$2\r\nhi\r\n"},
		{"nilbulk", func(w *resp.Writer) error { return w.WriteBulkString(nil) }, "$-1\r\n"},
		{"nullbulk", func(w *resp.Writer) error { return w.WriteNullBulk() }, "$-1\r\n"},
		{"nullarray", func(w *resp.Writer) error { return w.WriteNullArray() }, "*-1\r\n"},
		{"arrhdr", func(w *resp.Writer) error { return w.WriteArrayHeader(2) }, "*2\r\n"},
	}
	for _, tc := range cases {
		var buf bytes.Buffer
		w := resp.NewWriter(&buf)
		if err := tc.fn(w); err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if err := w.Flush(); err != nil {
			t.Fatalf("%s flush: %v", tc.name, err)
		}
		if buf.String() != tc.want {
			t.Fatalf("%s = %q, want %q", tc.name, buf.String(), tc.want)
		}
	}
}

func TestEncodeCommand_RoundTrip(t *testing.T) {
	wire := resp.EncodeStrings("SET", "k", "v")
	c := readOne(t, string(wire))
	if c.Name() != "SET" || c.Arg(1) != "k" || c.Arg(2) != "v" {
		t.Fatalf("round-trip mismatch: %+v", c.Args)
	}
}

// itoa avoids pulling strconv into the test for one small use.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}
