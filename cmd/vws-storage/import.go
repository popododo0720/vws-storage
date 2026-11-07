package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/popododo0720/vws-storage/internal/storage"
)

// runImport is `vws-storage import-sqlite`: it copies a SQLite file's records
// into the Kubernetes store (#141). Every line goes to out.
func runImport(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("vws-storage import-sqlite", flag.ContinueOnError)
	from := fs.String("from", "", "SQLite file to import; it is read from a copy and left unchanged")
	kubeconfig := fs.String("store-kube-config", "", "kubeconfig path; empty uses in-cluster credentials")
	namespace := fs.String("store-namespace", "", "namespace of the Kubernetes store")
	dryRun := fs.Bool("dry-run", false, "print the records the import would create and write nothing")
	verify := fs.Bool("verify", false, "compare every record of the file with the store and write nothing")
	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("parse flags: %w", err)
	}
	switch {
	case *from == "" || *namespace == "":
		return errors.New("import-sqlite: -from and -store-namespace are required")
	case *dryRun && *verify:
		return errors.New("import-sqlite: -dry-run and -verify are exclusive")
	case fs.NArg() > 0:
		return fmt.Errorf("import-sqlite: unexpected argument %q", fs.Arg(0))
	}
	mode, verb := storage.ImportWrite, "created"
	switch {
	case *dryRun:
		mode, verb = storage.ImportDryRun, "would create"
	case *verify:
		mode, verb = storage.ImportVerify, "missing or different"
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	target := storage.ImportTarget{Kubeconfig: *kubeconfig, Namespace: *namespace}
	rep, err := storage.ImportSQLite(ctx, *from, target, mode)
	var b strings.Builder
	if mode != storage.ImportVerify {
		for _, line := range rep.Create {
			b.WriteString("create " + line + "\n")
		}
	}
	for _, line := range rep.Differences {
		b.WriteString(line + "\n")
	}
	n := len(rep.Create)
	if mode == storage.ImportVerify {
		n = len(rep.Differences)
	}
	fmt.Fprintf(&b, "records %d, %s %d\n", rep.Records, verb, n)
	if _, werr := io.WriteString(out, b.String()); werr != nil {
		return errors.Join(err, fmt.Errorf("import-sqlite: write report: %w", werr))
	}
	return err
}
