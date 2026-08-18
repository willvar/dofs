package config

import (
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/willvar/dofs"
	"go.yaml.in/yaml/v3"
)

const maxConfigSize = 1 << 20
const maxSecretSize = 64 << 10

type Config struct {
	Version    int              `yaml:"version"`
	StateRoot  string           `yaml:"state_root"`
	Metadata   MetadataConfig   `yaml:"metadata"`
	Object     ObjectConfig     `yaml:"object"`
	Encryption EncryptionConfig `yaml:"encryption"`
	Mount      MountConfig      `yaml:"mount"`
}

type MetadataConfig struct {
	Driver   string         `yaml:"driver"`
	SQLite   SQLiteConfig   `yaml:"sqlite"`
	Postgres PostgresConfig `yaml:"postgres"`
}

type SQLiteConfig struct {
	Path               string `yaml:"path"`
	BusyTimeoutSeconds int    `yaml:"busy_timeout_seconds"`
	MaxOpenConnections int    `yaml:"max_open_connections"`
}

type PostgresConfig struct {
	DSN SecretSource `yaml:"dsn"`
}

type ObjectConfig struct {
	Driver string   `yaml:"driver"`
	S3     S3Config `yaml:"s3"`
}

type S3Config struct {
	Endpoint     string       `yaml:"endpoint"`
	Bucket       string       `yaml:"bucket"`
	Region       string       `yaml:"region"`
	Prefix       string       `yaml:"prefix"`
	Secure       bool         `yaml:"secure"`
	AccessKey    SecretSource `yaml:"access_key"`
	SecretKey    SecretSource `yaml:"secret_key"`
	SessionToken SecretSource `yaml:"session_token"`
}

type EncryptionConfig struct {
	MasterKey SecretSource `yaml:"master_key"`
}

type MountConfig struct {
	Root       string `yaml:"root"`
	Writable   bool   `yaml:"writable"`
	AllowOther bool   `yaml:"allow_other"`
	UID        uint32 `yaml:"uid"`
	GID        uint32 `yaml:"gid"`
	Debug      bool   `yaml:"debug"`
}

// SecretSource deliberately has no inline value. Production credentials must
// arrive through the process environment or a mounted secret file.
type SecretSource struct {
	Env  string `yaml:"env"`
	File string `yaml:"file"`
}

func Default() Config {
	return Config{
		Version: 1, StateRoot: "/var/lib/dofs",
		Metadata: MetadataConfig{
			Driver: "sqlite",
			SQLite: SQLiteConfig{BusyTimeoutSeconds: 5, MaxOpenConnections: 8},
		},
		Object: ObjectConfig{Driver: "s3", S3: S3Config{Secure: true}},
		Mount:  MountConfig{Root: "/mnt/dofs", Writable: true},
	}
}

func Load(path string) (Config, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return Config{}, errors.New("dofs config: path is required")
	}
	file, err := os.Open(path)
	if err != nil {
		return Config{}, fmt.Errorf("open DOFS config: %w", err)
	}
	defer file.Close()
	config := Default()
	decoder := yaml.NewDecoder(io.LimitReader(file, maxConfigSize+1))
	decoder.KnownFields(true)
	if err := decoder.Decode(&config); err != nil {
		return Config{}, fmt.Errorf("decode DOFS config: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return Config{}, errors.New("dofs config: multiple YAML documents are not allowed")
		}
		return Config{}, fmt.Errorf("decode trailing DOFS config: %w", err)
	}
	info, err := file.Stat()
	if err != nil {
		return Config{}, fmt.Errorf("inspect DOFS config: %w", err)
	}
	if info.Size() > maxConfigSize {
		return Config{}, errors.New("dofs config: file exceeds 1 MiB")
	}
	absolutePath, err := filepath.Abs(path)
	if err != nil {
		return Config{}, fmt.Errorf("resolve DOFS config path: %w", err)
	}
	if err := config.normalize(filepath.Dir(absolutePath)); err != nil {
		return Config{}, err
	}
	return config, nil
}

func (c *Config) normalize(configDirectory string) error {
	if c.Version != 1 {
		return fmt.Errorf("dofs config: unsupported version %d", c.Version)
	}
	var err error
	c.StateRoot, err = cleanAbsolute(c.StateRoot, "state_root")
	if err != nil {
		return err
	}
	c.Metadata.Driver = strings.ToLower(strings.TrimSpace(c.Metadata.Driver))
	switch c.Metadata.Driver {
	case "sqlite":
		if strings.TrimSpace(c.Metadata.SQLite.Path) == "" {
			c.Metadata.SQLite.Path = filepath.Join(c.StateRoot, "metadata.sqlite")
		} else if !filepath.IsAbs(c.Metadata.SQLite.Path) {
			c.Metadata.SQLite.Path = filepath.Join(configDirectory, c.Metadata.SQLite.Path)
		}
		c.Metadata.SQLite.Path = filepath.Clean(c.Metadata.SQLite.Path)
		if c.Metadata.SQLite.BusyTimeoutSeconds <= 0 {
			return errors.New("dofs config: metadata.sqlite.busy_timeout_seconds must be positive")
		}
		if c.Metadata.SQLite.MaxOpenConnections <= 0 {
			return errors.New("dofs config: metadata.sqlite.max_open_connections must be positive")
		}
	case "postgres":
		if err := c.Metadata.Postgres.DSN.validate("metadata.postgres.dsn", true, configDirectory); err != nil {
			return err
		}
	default:
		return fmt.Errorf("dofs config: unsupported metadata driver %q", c.Metadata.Driver)
	}
	c.Object.Driver = strings.ToLower(strings.TrimSpace(c.Object.Driver))
	if c.Object.Driver != "s3" {
		return fmt.Errorf("dofs config: unsupported object driver %q", c.Object.Driver)
	}
	c.Object.S3.Endpoint = strings.TrimSpace(c.Object.S3.Endpoint)
	c.Object.S3.Bucket = strings.TrimSpace(c.Object.S3.Bucket)
	if c.Object.S3.Endpoint == "" || strings.Contains(c.Object.S3.Endpoint, "://") || strings.ContainsAny(c.Object.S3.Endpoint, "/?#") {
		return errors.New("dofs config: object.s3.endpoint must be a host[:port] without a URL scheme or path")
	}
	if c.Object.S3.Bucket == "" {
		return errors.New("dofs config: object.s3.bucket is required")
	}
	if err := c.Object.S3.AccessKey.validate("object.s3.access_key", true, configDirectory); err != nil {
		return err
	}
	if err := c.Object.S3.SecretKey.validate("object.s3.secret_key", true, configDirectory); err != nil {
		return err
	}
	if err := c.Object.S3.SessionToken.validate("object.s3.session_token", false, configDirectory); err != nil {
		return err
	}
	if err := c.Encryption.MasterKey.validate("encryption.master_key", true, configDirectory); err != nil {
		return err
	}
	c.Mount.Root, err = cleanAbsolute(c.Mount.Root, "mount.root")
	if err != nil {
		return err
	}
	if sameOrInside(c.StateRoot, c.Mount.Root) || sameOrInside(c.Mount.Root, c.StateRoot) {
		return errors.New("dofs config: state_root and mount.root must not overlap")
	}
	return nil
}

func cleanAbsolute(value, name string) (string, error) {
	value = filepath.Clean(strings.TrimSpace(value))
	if value == "." || !filepath.IsAbs(value) || value == string(filepath.Separator) {
		return "", fmt.Errorf("dofs config: %s must be an absolute non-root path", name)
	}
	return value, nil
}

func sameOrInside(parent, candidate string) bool {
	relative, err := filepath.Rel(parent, candidate)
	return err == nil && relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator))
}

func (s *SecretSource) validate(name string, required bool, configDirectory string) error {
	s.Env = strings.TrimSpace(s.Env)
	s.File = strings.TrimSpace(s.File)
	if s.Env != "" && s.File != "" {
		return fmt.Errorf("dofs config: %s must use either env or file, not both", name)
	}
	if required && s.Env == "" && s.File == "" {
		return fmt.Errorf("dofs config: %s requires env or file", name)
	}
	if s.File != "" && !filepath.IsAbs(s.File) {
		s.File = filepath.Join(configDirectory, s.File)
	}
	return nil
}

func (s SecretSource) Read(required bool) (string, error) {
	var value string
	switch {
	case s.Env != "":
		resolved, exists := os.LookupEnv(s.Env)
		if !exists {
			return "", fmt.Errorf("secret environment variable %s is not set", s.Env)
		}
		value = resolved
	case s.File != "":
		file, err := os.Open(s.File)
		if err != nil {
			return "", fmt.Errorf("open secret file %s: %w", s.File, err)
		}
		defer file.Close()
		info, err := file.Stat()
		if err != nil {
			return "", fmt.Errorf("inspect secret file %s: %w", s.File, err)
		}
		if !info.Mode().IsRegular() || info.Size() > maxSecretSize {
			return "", fmt.Errorf("secret file %s must be a regular file no larger than 64 KiB", s.File)
		}
		contents, err := io.ReadAll(io.LimitReader(file, maxSecretSize+1))
		if err != nil {
			return "", fmt.Errorf("read secret file %s: %w", s.File, err)
		}
		value = string(contents)
	default:
		if required {
			return "", errors.New("required secret source is empty")
		}
		return "", nil
	}
	value = strings.TrimSpace(value)
	if required && value == "" {
		return "", errors.New("required secret is empty")
	}
	return value, nil
}

func (c Config) LoadMasterKey() ([]byte, error) {
	encoded, err := c.Encryption.MasterKey.Read(true)
	if err != nil {
		return nil, fmt.Errorf("load DOFS master key: %w", err)
	}
	var key []byte
	switch {
	case strings.HasPrefix(encoded, "hex:"):
		key, err = hex.DecodeString(strings.TrimPrefix(encoded, "hex:"))
	case strings.HasPrefix(encoded, "base64:"):
		key, err = base64.StdEncoding.DecodeString(strings.TrimPrefix(encoded, "base64:"))
	default:
		key, err = hex.DecodeString(encoded)
		if err != nil {
			key, err = base64.StdEncoding.DecodeString(encoded)
		}
	}
	if err != nil || len(key) != dofs.KeySize {
		dofs.Clear(key)
		return nil, fmt.Errorf("DOFS master key must decode to %d bytes", dofs.KeySize)
	}
	return key, nil
}
