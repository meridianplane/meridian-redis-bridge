package proxy

import "testing"

func TestDecide_WriteCommands(t *testing.T) {
	r := NewRouter()
	for _, name := range defaultWriteCommands {
		if got := r.Decide(name); got != RouteWrite {
			t.Fatalf("Decide(%q) = %v, want RouteWrite", name, got)
		}
		// Case-insensitive.
		if got := r.Decide(lower(name)); got != RouteWrite {
			t.Fatalf("Decide(%q lowercased) = %v, want RouteWrite", name, got)
		}
	}
}

func TestDecide_ReadCommands(t *testing.T) {
	r := NewRouter()
	for _, name := range defaultReadCommands {
		if got := r.Decide(name); got != RouteRead {
			t.Fatalf("Decide(%q) = %v, want RouteRead", name, got)
		}
	}
}

func TestDecide_DenyCommands(t *testing.T) {
	r := NewRouter()
	for _, name := range defaultDenyCommands {
		if got := r.Decide(name); got != RouteDeny {
			t.Fatalf("Decide(%q) = %v, want RouteDeny", name, got)
		}
	}
}

func TestDecide_UnknownDefaultsToDeny(t *testing.T) {
	r := NewRouter()
	for _, name := range []string{"NOSUCHCMD", "NOSUCHCMD2", "SUBSCRIBE", ""} {
		if got := r.Decide(name); got != RouteDeny {
			t.Fatalf("Decide(%q) = %v, want RouteDeny (default-deny)", name, got)
		}
	}
}

// TestRouterTables_NoOverlap guards the invariant that the three command tables
// are disjoint: a name in more than one table would make routing depend on the
// lookup order in Decide rather than on intent.
func TestRouterTables_NoOverlap(t *testing.T) {
	in := map[string][]string{}
	for _, n := range defaultWriteCommands {
		in[n] = append(in[n], "write")
	}
	for _, n := range defaultReadCommands {
		in[n] = append(in[n], "read")
	}
	for _, n := range defaultDenyCommands {
		in[n] = append(in[n], "deny")
	}
	for name, tables := range in {
		if len(tables) > 1 {
			t.Fatalf("command %q appears in multiple tables: %v", name, tables)
		}
	}
}

func lower(s string) string {
	b := []byte(s)
	for i := range b {
		if b[i] >= 'A' && b[i] <= 'Z' {
			b[i] += 'a' - 'A'
		}
	}
	return string(b)
}
