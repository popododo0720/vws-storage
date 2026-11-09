package main

import (
	"context"
	"strings"
	"testing"
)

func TestWorkersFlagBounds(t *testing.T) {
	for _, n := range []int{1, defaultWorkers, maxWorkers} {
		if err := checkWorkers(n); err != nil {
			t.Fatalf("-workers=%d: %v", n, err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for _, v := range []string{"0", "-1", "257"} {
		err := runContext(ctx, []string{"-workers=" + v})
		if err == nil || !strings.Contains(err.Error(), "-workers "+v+": must be 1 to 256") {
			t.Fatalf("-workers=%s: got %v", v, err)
		}
	}
	if err := runContext(ctx, []string{"-workers=many"}); err == nil {
		t.Fatal("-workers=many: run succeeded")
	}
}
