package context

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"os"
	"path/filepath"
	"testing"

	"github.com/golang-jwt/jwt/v5"

	"github.com/free5gc/openapi/models"
)

func TestAuthorizationCheckOAuthDisabled(t *testing.T) {
	t.Parallel()

	ctx := &NWDAFContext{}
	if err := ctx.AuthorizationCheck("not-a-token", models.ServiceName_NNWDAF_EVENTSSUBSCRIPTION); err != nil {
		t.Fatalf("AuthorizationCheck() error = %v", err)
	}
}

func TestAuthorizationCheckUsesFree5GCOAuthVerification(t *testing.T) {
	t.Parallel()

	certPath, signingKey := writeOAuthPublicKey(t)
	_, otherSigningKey := writeOAuthPublicKey(t)
	serviceName := models.ServiceName_NNWDAF_EVENTSSUBSCRIPTION

	tests := []struct {
		name    string
		scope   string
		key     *rsa.PrivateKey
		wantErr bool
	}{
		{name: "required scope", scope: string(serviceName), key: signingKey},
		{name: "required scope among multiple scopes", scope: "nnrf-disc " + string(serviceName), key: signingKey},
		{name: "missing scope", scope: "", key: signingKey, wantErr: true},
		{name: "wrong scope", scope: "nnrf-nfm", key: signingKey, wantErr: true},
		{name: "incorrect signature", scope: string(serviceName), key: otherSigningKey, wantErr: true},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := &NWDAFContext{oauth2Required: true, nrfCertPem: certPath}
			token := signOAuthToken(t, tt.key, tt.scope)
			err := ctx.AuthorizationCheck("Bearer "+token, serviceName)
			if (err != nil) != tt.wantErr {
				t.Fatalf("AuthorizationCheck() error = %v, wantErr %t", err, tt.wantErr)
			}
		})
	}
}

func TestAuthorizationCheckRejectsMissingVerificationMaterial(t *testing.T) {
	t.Parallel()

	for _, certPath := range []string{"", filepath.Join(t.TempDir(), "missing.pem")} {
		ctx := &NWDAFContext{oauth2Required: true, nrfCertPem: certPath}
		if err := ctx.AuthorizationCheck(
			"Bearer unusable",
			models.ServiceName_NNWDAF_EVENTSSUBSCRIPTION,
		); err == nil {
			t.Fatalf("AuthorizationCheck() error = nil for cert path %q", certPath)
		}
	}
}

func writeOAuthPublicKey(t *testing.T) (string, *rsa.PrivateKey) {
	t.Helper()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate RSA key: %v", err)
	}
	publicKey, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	if err != nil {
		t.Fatalf("marshal RSA public key: %v", err)
	}
	certPath := filepath.Join(t.TempDir(), "nrf.pem")
	if err = os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{
		Type:  "PUBLIC KEY",
		Bytes: publicKey,
	}), 0o600); err != nil {
		t.Fatalf("write RSA public key: %v", err)
	}
	return certPath, key
}

func signOAuthToken(t *testing.T, key *rsa.PrivateKey, scope string) string {
	t.Helper()

	token := jwt.NewWithClaims(jwt.SigningMethodRS512, &models.NrfAccessTokenAccessTokenClaims{
		Scope: scope,
	})
	signed, err := token.SignedString(key)
	if err != nil {
		t.Fatalf("sign OAuth token: %v", err)
	}
	return signed
}
