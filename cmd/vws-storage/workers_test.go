package main

import (
	"strings"
	"testing"
)

func TestWorkersFlagBounds(t *testing.T) {
	for _, n := range []int{1, defaultWorkers, maxWorkers} {
		if err := checkWorkers(n); err != nil {
			t.Fatalf("-workers=%d: %v", n, err)
		}
	}
	for _, v := range []string{"0", "-1", "257"} {
		err := run([]string{"-workers=" + v})
		if err == nil || !strings.Contains(err.Error(), "-workers "+v+": must be 1 to 256") {
			t.Fatalf("-workers=%s: got %v", v, err)
		}
	}
	if err := run([]string{"-workers=many"}); err == nil {
		t.Fatal("-workers=many: run succeeded")
	}
}
