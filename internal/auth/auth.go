// Package auth verifies client credentials at the proxy front-end.
// Credentials are loaded from a passwd file (nginx htpasswd-compatible:
// one line per user, "username:password"), read once at startup.
package auth

import (
	"crypto/subtle"
	"errors"
	"os"
	"strings"

	"golang.org/x/crypto/bcrypt"
)

const DefaultUser = "default"

var ErrUnauthorized = errors.New("WRONGPASS invalid username-password pair or user is disabled")

type Authenticator interface {
	Required() bool
	Verify(username, password string) error
}

// Static is a credential set loaded from a passwd file. Each line is
// "username:password". When loaded with an empty file path, Required()
// returns false.
type Static struct {
	users map[string]string
}

// NewFromFile reads a passwd file at path. If path is empty, the
// authenticator reports Required()==false. Lines starting with '#' are
// comments. Password comparison is constant-time.
func NewFromFile(path string) (*Static, error) {
	if path == "" {
		return &Static{}, nil
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	m := map[string]string{}
	for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || line[0] == '#' {
			continue
		}
		u, p, ok := strings.Cut(line, ":")
		if !ok || u == "" {
			continue
		}
		m[u] = p
	}
	return &Static{users: m}, nil
}

func (s *Static) Required() bool { return len(s.users) > 0 }

func (s *Static) Verify(username, password string) error {
	if username == "" {
		username = DefaultUser
	}
	want, ok := s.users[username]
	if !ok {
		subtle.ConstantTimeCompare([]byte(password), []byte(password))
		return ErrUnauthorized
	}
	if strings.HasPrefix(want, "$2") {
		if bcrypt.CompareHashAndPassword([]byte(want), []byte(password)) != nil {
			return ErrUnauthorized
		}
		return nil
	}
	if subtle.ConstantTimeCompare([]byte(password), []byte(want)) != 1 {
		return ErrUnauthorized
	}
	return nil
}
