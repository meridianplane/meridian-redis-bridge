package auth

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/crypto/bcrypt"
)

func writePasswd(t *testing.T, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "passwd")
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestNotRequiredWhenEmptyPath(t *testing.T) {
	a, err := NewFromFile("")
	if err != nil {
		t.Fatalf("NewFromFile(empty): %v", err)
	}
	if a.Required() {
		t.Fatal("empty path should mean Required()==false")
	}
}

func TestDefaultPassword(t *testing.T) {
	p := writePasswd(t, "default:s3cr3t\n")
	a, err := NewFromFile(p)
	if err != nil {
		t.Fatalf("NewFromFile: %v", err)
	}
	if !a.Required() {
		t.Fatal("credentials loaded should make Required()==true")
	}
	if err := a.Verify("", "s3cr3t"); err != nil {
		t.Fatalf("correct password rejected: %v", err)
	}
	if err := a.Verify(DefaultUser, "s3cr3t"); err != nil {
		t.Fatalf("explicit default user rejected: %v", err)
	}
	if err := a.Verify("", "wrong"); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("wrong password err = %v, want ErrUnauthorized", err)
	}
}

func TestNamedUsers(t *testing.T) {
	p := writePasswd(t, "alice:pw1\nbob:pw2\n")
	a, _ := NewFromFile(p)
	if err := a.Verify("alice", "pw1"); err != nil {
		t.Fatalf("alice rejected: %v", err)
	}
	if err := a.Verify("bob", "pw1"); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("bob with alice's password should fail")
	}
	if err := a.Verify("carol", "pw1"); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("unknown user should fail")
	}
}

func TestBcryptPassword(t *testing.T) {
	hash, err := bcrypt.GenerateFromPassword([]byte("s3cr3t"), bcrypt.DefaultCost)
	if err != nil {
		t.Fatal(err)
	}
	p := writePasswd(t, "default:"+string(hash)+"\n")
	a, _ := NewFromFile(p)
	if err := a.Verify("", "s3cr3t"); err != nil {
		t.Fatalf("bcrypt correct rejected: %v", err)
	}
	if err := a.Verify("", "wrong"); !errors.Is(err, ErrUnauthorized) {
		t.Fatal("bcrypt wrong password should be rejected")
	}
}

func TestCommentsIgnored(t *testing.T) {
	p := writePasswd(t, "# this is a comment\ndefault:pw\n# another comment\n")
	a, _ := NewFromFile(p)
	if len(a.users) != 1 {
		t.Fatalf("expected 1 user, got %d", len(a.users))
	}
	if err := a.Verify("", "pw"); err != nil {
		t.Fatalf("password rejected: %v", err)
	}
}
