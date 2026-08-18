//go:build linux

package dofs

import (
	"strings"
	"testing"
)

func TestFuseConfigAllowsOther(t *testing.T) {
	tests := []struct {
		name    string
		config  string
		allowed bool
	}{
		{name: "enabled", config: "# comment\n user_allow_other # explanation\n", allowed: true},
		{name: "commented", config: "# user_allow_other\n", allowed: false},
		{name: "similar", config: "user_allow_other_value\n", allowed: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			allowed, err := fuseConfigAllowsOther(strings.NewReader(test.config))
			if err != nil || allowed != test.allowed {
				t.Fatalf("allowed = %v, error = %v", allowed, err)
			}
		})
	}
}
