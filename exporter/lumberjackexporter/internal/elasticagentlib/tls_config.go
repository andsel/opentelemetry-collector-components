package elasticagentlib

import (
	"github.com/elastic/elastic-agent-libs/config"
	"github.com/elastic/elastic-agent-libs/transport/tlscommon"
)

type TLSConfig struct {
	Enabled              *bool    `mapstructure:"enabled" config:"enabled"`
	VerificationMode     string   `mapstructure:"verification_mode" config:"verification_mode"` // one of 'none', 'full'
	Versions             []string `mapstructure:"supported_protocols" config:"supported_protocols"`
	CipherSuites         []string `mapstructure:"cipher_suites" config:"cipher_suites"`
	CAs                  []string `mapstructure:"certificate_authorities" config:"certificate_authorities"`
	CertificateConfig    `mapstructure:",squash" config:",inline"`
	CurveTypes           []string `mapstructure:"curve_types" config:"curve_types"`
	Renegotiation        string   `mapstructure:"renegotiation" config:"renegotiation"`
	CASha256             []string `mapstructure:"ca_sha256" config:"ca_sha256"`
	CATrustedFingerprint string   `mapstructure:"ca_trusted_fingerprint" config:"ca_trusted_fingerprint"`
}

type CertificateConfig struct {
	Certificate    string `mapstructure:"certificate" config:"certificate"`
	Key            string `mapstructure:"key" config:"key"`
	Passphrase     string `mapstructure:"key_passphrase" config:"key_passphrase"`
	PassphrasePath string `mapstructure:"key_passphrase_path" config:"key_passphrase_path"`
}

func (c *TLSConfig) IsEnabled() bool {
	return c != nil && (c.Enabled == nil || *c.Enabled)
}

func (c *TLSConfig) ToTLSCommonConfig() (*tlscommon.Config, error) {
	if !c.IsEnabled() {
		return nil, nil
	}

	cfg, err := config.NewConfigFrom(c)
	if err != nil {
		return nil, err
	}

	tcConfig := tlscommon.Config{}
	if err := cfg.Unpack(&tcConfig); err != nil {
		return nil, err
	}

	return &tcConfig, nil
}
