package server

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/popododo0720/vws-storage/internal/model"
)

func TestUnclassifiedErrorIsInternalWithoutItsMessage(t *testing.T) {
	err := toStatus(fmt.Errorf("sqlite: open /var/lib/vws/secret.db: %w", errors.New("disk on fire")))
	st, _ := status.FromError(err)
	if st.Code() != codes.Internal || strings.Contains(st.Message(), "secret") || strings.Contains(st.Message(), "fire") {
		t.Fatalf("status = %v", st)
	}
}

func TestDependencyMissingIsAFailedPreconditionWithItsOwnReason(t *testing.T) {
	reason := func(err error) string {
		st, _ := status.FromError(toStatus(err))
		if st.Code() != codes.FailedPrecondition {
			t.Fatalf("code = %v", st.Code())
		}
		for _, d := range st.Details() {
			if info, ok := d.(*errdetails.ErrorInfo); ok {
				return info.GetReason()
			}
		}
		return ""
	}
	if got := reason(fmt.Errorf("%w: bucket is not empty", model.ErrDependencyMissing)); got != "DEPENDENCY_MISSING" {
		t.Fatalf("reason = %q", got)
	}
	if got := reason(fmt.Errorf("%w: volume is not available", model.ErrFailedPrecondition)); got != "DEPENDENCY_NOT_READY" {
		t.Fatalf("reason = %q", got)
	}
	if !errors.Is(model.ErrDependencyMissing, model.ErrFailedPrecondition) {
		t.Fatal("ErrDependencyMissing must match ErrFailedPrecondition")
	}
}
