package storage_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/popododo0720/vws-storage/internal/backend"
	"github.com/popododo0720/vws-storage/internal/backend/fake"
	"github.com/popododo0720/vws-storage/internal/model"
	"github.com/popododo0720/vws-storage/internal/storage"
)

const leak = `Post "https://10.0.0.1:6443/api": token=s3cr3t /var/lib/vws/storage.db`

func TestBackendErrorTextStaysOutOfTheStatus(t *testing.T) {
	cases := []struct {
		err  error
		code model.Code
	}{
		{fmt.Errorf("kube: get claim: %w: %s", backend.ErrNotFound, leak), model.CodeNotFound},
		{backend.Permanent(fmt.Errorf("%w: %s", backend.ErrConflict, leak)), model.CodeFailedPrecondition},
		{fmt.Errorf("%w: %s", backend.ErrBusy, leak), model.CodeUnavailable},
		{errors.New("s3: create bucket: " + leak), model.CodeUnavailable},
	}
	for _, c := range cases {
		st := storage.StatusOf(context.Background(), c.err)
		if st.Code != c.code || st.Message == "" {
			t.Errorf("%v: status %+v", c.err, st)
		}
		for _, s := range []string{"10.0.0.1", "s3cr3t", "storage.db", "kube:", "s3:"} {
			if strings.Contains(st.Message, s) {
				t.Errorf("%v: message %q carries %q", c.err, st.Message, s)
			}
		}
	}
}

func TestInvalidArgumentKeepsItsText(t *testing.T) {
	st := storage.StatusOf(context.Background(), backend.Permanent(fmt.Errorf("%w: volume cannot shrink", backend.ErrInvalid)))
	if st.Code != model.CodeInvalidArgument || !strings.Contains(st.Message, "volume cannot shrink") {
		t.Fatalf("status %+v", st)
	}
}

// A backend that gave up says why: retry limit or timeout.
func TestGaveUpKeepsItsText(t *testing.T) {
	st := storage.StatusOf(context.Background(), backend.Permanent(fmt.Errorf("%w: the CDI DataVolume failed 4 times", backend.ErrGaveUp)))
	if st.Code != model.CodeDeadlineExceeded || !strings.Contains(st.Message, "failed 4 times") {
		t.Fatalf("status %+v", st)
	}
}

// The text that reaches the client through a failed operation is the fixed one.
func TestFailedOperationDoesNotCarryBackendText(t *testing.T) {
	blk := fake.NewBlock()
	blk.FailNext("CreateVolume", backend.Permanent(fmt.Errorf("%w: %s", backend.ErrConflict, leak)))
	svc, cancel := openRunning(t, storage.Memory(), blk, fake.NewObjects(fake.ObjectOptions{}))
	defer cancel()
	op, err := svc.Submit(context.Background(), token(t, "leak-1"), volumeReq(t))
	if err != nil {
		t.Fatal(err)
	}
	got := waitDone(t, svc, op.ID)
	if got.Phase() != model.PhaseFailed || got.Err().Code != model.CodeFailedPrecondition {
		t.Fatalf("phase %s %+v", got.Phase(), got.Err())
	}
	if strings.Contains(got.Err().Message, "10.0.0.1") || strings.Contains(got.Err().Message, "s3cr3t") {
		t.Fatalf("message %q", got.Err().Message)
	}
}
