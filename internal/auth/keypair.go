package auth

import (
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"fmt"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// TokenConfig holds info needed to generate a Snowflake JWT.
type TokenConfig struct {
	Account     string // e.g., CXEEZLW-JQB53549
	User        string // e.g., VJAIN27
	PrivateKey  []byte // PEM-encoded private key (PKCS8)
	PublicKey   []byte // PEM-encoded public key (used for fingerprint)
	ExpireAfter time.Duration
}

// GenerateJWT returns a Snowflake-compatible JWT token.
func GenerateJWT(cfg TokenConfig) (string, error) {
	privKey, err := parsePrivateKey(cfg.PrivateKey)
	if err != nil {
		return "", err
	}

	fp, err := fingerprint(cfg.PublicKey)
	if err != nil {
		return "", fmt.Errorf("fingerprint generation failed: %w", err)
	}

	account := normalizeAccount(cfg.Account)
	user := strings.ToUpper(cfg.User)
	subject := fmt.Sprintf("%s.%s", account, user)
	issuer := fmt.Sprintf("%s.%s.%s", account, user, fp)

	now := time.Now().UTC()
	claims := jwt.RegisteredClaims{
		Issuer:    issuer,
		Subject:   subject,
		Audience:  jwt.ClaimStrings{"snowflake"},
		IssuedAt:  jwt.NewNumericDate(now),
		ExpiresAt: jwt.NewNumericDate(now.Add(cfg.ExpireAfter)),
	}

	token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	signed, err := token.SignedString(privKey)
	if err != nil {
		return "", fmt.Errorf("JWT signing failed: %w", err)
	}
	return signed, nil
}

// parsePrivateKey parses a PEM-encoded PKCS#8 RSA key.
func parsePrivateKey(pemBytes []byte) (*rsa.PrivateKey, error) {
	block, _ := pem.Decode(pemBytes)
	if block == nil {
		return nil, fmt.Errorf("invalid PEM format for private key")
	}
	key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, err
	}
	rsaKey, ok := key.(*rsa.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("not an RSA private key")
	}
	return rsaKey, nil
}

// fingerprint computes the SHA256 fingerprint of a PEM-encoded public key.
func fingerprint(pubPEM []byte) (string, error) {
	block, _ := pem.Decode(pubPEM)
	if block == nil {
		return "", fmt.Errorf("invalid PEM for public key")
	}
	hash := sha256.Sum256(block.Bytes)
	return "SHA256:" + base64.StdEncoding.EncodeToString(hash[:]), nil
}

// normalizeAccount ensures uppercase and replaces periods with hyphens.
func normalizeAccount(account string) string {
	return strings.ToUpper(strings.ReplaceAll(account, ".", "-"))
}
