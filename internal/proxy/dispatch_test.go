package proxy

import (
	"strconv"
	"strings"
	"testing"
)

func TestNormalizeExpiry_SET_EX(t *testing.T) {
	now := int64(1000000)
	entries := normalizeExpiry(bargs("SET", "k", "v", "EX", "60"), now)
	if len(entries) != 2 {
		t.Fatalf("SET EX should produce 2 entries, got %d", len(entries))
	}
	// First entry: SET k v
	if string(entries[0][0]) != "SET" || string(entries[0][1]) != "k" || string(entries[0][2]) != "v" {
		t.Fatalf("SET entry: %v", entries[0])
	}
	// Second: PEXPIREAT k abs
	if string(entries[1][0]) != "PEXPIREAT" || string(entries[1][1]) != "k" {
		t.Fatalf("PEXPIREAT entry: %v", entries[1])
	}
	expAbs, _ := strconv.ParseInt(string(entries[1][2]), 10, 64)
	if expAbs != now+60*1000 {
		t.Fatalf("PEXPIREAT abs = %d, want %d", expAbs, now+60*1000)
	}
}

func TestNormalizeExpiry_SET_PX(t *testing.T) {
	now := int64(2000000)
	entries := normalizeExpiry(bargs("SET", "k", "v", "PX", "5000"), now)
	if len(entries) != 2 {
		t.Fatalf("got %d entries, want 2", len(entries))
	}
	expAbs, _ := strconv.ParseInt(string(entries[1][2]), 10, 64)
	if expAbs != now+5000 {
		t.Fatalf("PX 5000 should be abs %d, got %d", now+5000, expAbs)
	}
}

func TestNormalizeExpiry_SET_no_expiry(t *testing.T) {
	entries := normalizeExpiry(bargs("SET", "k", "v"), 0)
	if len(entries) != 1 {
		t.Fatalf("SET without expiry: got %d entries, want 1", len(entries))
	}
}

func TestNormalizeExpiry_SET_with_NX(t *testing.T) {
	now := int64(3000000)
	entries := normalizeExpiry(bargs("SET", "k", "v", "NX", "EX", "10"), now)
	if len(entries) != 2 {
		t.Fatalf("SET NX EX: got %d entries, want 2", len(entries))
	}
	// SET k v NX
	if strings.ToUpper(string(entries[0][3])) != "NX" {
		t.Fatalf("expected NX preserved in SET entry: %v", entries[0])
	}
}

func TestNormalizeExpiry_SETEX(t *testing.T) {
	now := int64(4000000)
	entries := normalizeExpiry(bargs("SETEX", "k", "30", "v"), now)
	if len(entries) != 2 {
		t.Fatalf("got %d entries, want 2", len(entries))
	}
	if string(entries[0][0]) != "SET" {
		t.Fatalf("first entry should be SET, got %s", entries[0][0])
	}
	expAbs, _ := strconv.ParseInt(string(entries[1][2]), 10, 64)
	if expAbs != now+30*1000 {
		t.Fatalf("SETEX 30s: abs = %d, want %d", expAbs, now+30*1000)
	}
}

func TestNormalizeExpiry_PSETEX(t *testing.T) {
	now := int64(5000000)
	entries := normalizeExpiry(bargs("PSETEX", "k", "5000", "v"), now)
	expAbs, _ := strconv.ParseInt(string(entries[1][2]), 10, 64)
	if expAbs != now+5000 {
		t.Fatalf("PSETEX 5000ms: abs = %d, want %d", expAbs, now+5000)
	}
}

func TestNormalizeExpiry_EXPIRE(t *testing.T) {
	now := int64(6000000)
	entries := normalizeExpiry(bargs("EXPIRE", "k", "30"), now)
	if string(entries[0][0]) != "PEXPIREAT" {
		t.Fatalf("EXPIRE should become PEXPIREAT, got %s", entries[0][0])
	}
	expAbs, _ := strconv.ParseInt(string(entries[0][2]), 10, 64)
	if expAbs != now+30*1000 {
		t.Fatalf("EXPIRE 30s: abs = %d, want %d", expAbs, now+30*1000)
	}
}

func TestNormalizeExpiry_EXPIREAT(t *testing.T) {
	entries := normalizeExpiry(bargs("EXPIREAT", "k", "1234567890"), 0)
	if string(entries[0][0]) != "PEXPIREAT" {
		t.Fatalf("EXPIREAT should become PEXPIREAT")
	}
	expAbs, _ := strconv.ParseInt(string(entries[0][2]), 10, 64)
	if expAbs != 1234567890000 {
		t.Fatalf("EXPIREAT abs = %d, want 1234567890000", expAbs)
	}
}

func TestNormalizeExpiry_PERSIST(t *testing.T) {
	entries := normalizeExpiry(bargs("PERSIST", "k"), 0)
	if len(entries) != 1 || string(entries[0][0]) != "PERSIST" {
		t.Fatalf("PERSIST should pass through: %v", entries)
	}
}

func TestNormalizeExpiry_default(t *testing.T) {
	entries := normalizeExpiry(bargs("INCR", "k"), 0)
	if len(entries) != 1 {
		t.Fatalf("INCR should pass through unchanged, got %d entries", len(entries))
	}
}

