package statsd

import (
	"strings"
)

func HostKey(h string) string {
	return strings.ReplaceAll(strings.ReplaceAll(h, ".", "_"), ":", "_")
}
