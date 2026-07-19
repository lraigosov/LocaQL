package capabilities

import (
	"errors"
	"fmt"
	"os"
	"sort"

	"gopkg.in/yaml.v3"
)

type Entry struct {
	Status   string   `yaml:"status" json:"status"`
	Fidelity string   `yaml:"fidelity" json:"fidelity"`
	Tests    []string `yaml:"tests,omitempty" json:"tests,omitempty"`
	Reason   string   `yaml:"reason,omitempty" json:"reason,omitempty"`
}

type Registry struct {
	Capabilities map[string]Entry `yaml:"capabilities" json:"capabilities"`
}

func Load(path string) (Registry, error) {
	if path == "" {
		return Registry{}, errors.New("capabilities path is required")
	}

	content, err := os.ReadFile(path)
	if err != nil {
		return Registry{}, fmt.Errorf("read capabilities: %w", err)
	}

	var reg Registry
	if err := yaml.Unmarshal(content, &reg); err != nil {
		return Registry{}, fmt.Errorf("parse capabilities yaml: %w", err)
	}

	if len(reg.Capabilities) == 0 {
		return Registry{}, errors.New("capabilities registry is empty")
	}

	return reg, nil
}

func (r Registry) SortedKeys() []string {
	keys := make([]string, 0, len(r.Capabilities))
	for k := range r.Capabilities {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
