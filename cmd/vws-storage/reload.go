package main

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/popododo0720/vws-lib/obs"

	"github.com/popododo0720/vws-storage/internal/backend"
	"github.com/popododo0720/vws-storage/internal/backend/kube"
)

func startVolumeTypes(parent context.Context, block backend.Block, namespace, name string) (func(), error) {
	if name == "" {
		return func() {}, nil
	}
	b, ok := block.(*kube.Block)
	if !ok {
		return nil, fmt.Errorf("-kube-volume-types-configmap requires -block-backend=kube")
	}
	reloads, err := obs.M().NewCounter("vws.storage.volume_types.reloads", "Applied volume type mapping changes")
	if err != nil {
		return nil, fmt.Errorf("volume type reload counter: %w", err)
	}
	failures, err := obs.M().NewCounter("vws.storage.volume_types.reload_errors",
		"Volume type reload failures retaining the last good mapping")
	if err != nil {
		return nil, fmt.Errorf("volume type reload error counter: %w", err)
	}
	ctx, cancel := context.WithCancel(parent)
	reload := func() {
		changed, err := b.ReloadVolumeTypes(ctx, namespace, name)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			failures.Add(ctx, 1)
			slog.WarnContext(ctx, "volume type reload refused; retaining last good mapping",
				slog.String("namespace", namespace), slog.String("configmap", name), slog.String(obs.FieldErr, err.Error()))
		} else if changed {
			reloads.Add(ctx, 1)
			slog.InfoContext(ctx, "volume type mapping reloaded",
				slog.String("namespace", namespace), slog.String("configmap", name))
		}
	}
	reload()
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				reload()
			}
		}
	}()
	return func() { cancel(); <-done }, nil
}
