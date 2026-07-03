package config

import "testing"

type fakeCfg struct {
	Connection struct {
		URI string `yaml:"uri"`
	} `yaml:"connection"`
}

func TestLoadDriverYAML(t *testing.T) {
	var c fakeCfg
	if err := LoadDriverYAML("testdata/amqp.yaml", &c); err != nil {
		t.Fatal(err)
	}
	if c.Connection.URI != "amqp://guest:guest@localhost:5672/" {
		t.Errorf("uri = %q", c.Connection.URI)
	}
}

func TestLoadDriverYAMLMissingPath(t *testing.T) {
	var c fakeCfg
	if err := LoadDriverYAML("", &c); err == nil {
		t.Error("empty path should error")
	}
}
