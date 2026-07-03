package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// LoadDriverYAML 把 path 指向的 YAML 解析进驱动私有配置 dst。
func LoadDriverYAML(path string, dst any) error {
	if path == "" {
		return fmt.Errorf("--config is required for this command")
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read config %q: %w", path, err)
	}
	if err := yaml.Unmarshal(b, dst); err != nil {
		return fmt.Errorf("parse config %q: %w", path, err)
	}
	return nil
}
