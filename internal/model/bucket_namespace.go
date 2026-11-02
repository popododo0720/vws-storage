package model

import (
	"fmt"
	"strings"
)

// ErrInvalidBucketNamespace is an ErrInvalidArgument for a bucket name that
// does not fit the requested S3 bucket namespace (S3 InvalidBucketNamespace).
// It prints as ErrInvalidArgument.
var ErrInvalidBucketNamespace error = refinedError{name: "bucket namespace", parent: ErrInvalidArgument}

// BucketNamespace is the S3 general purpose bucket namespace, the
// x-amz-bucket-namespace header of CreateBucket. The zero value is global.
type BucketNamespace uint8

const (
	// BucketNamespaceGlobal is the shared namespace across every project.
	BucketNamespaceGlobal BucketNamespace = iota
	// BucketNamespaceAccountRegional is the reserved subdivision of the global
	// namespace that only one account creates in. A name ends with
	// "-{account}-{region}-an".
	BucketNamespaceAccountRegional
)

// accountRegionalSuffix ends every account-regional bucket name.
const accountRegionalSuffix = "-an"

// String returns the x-amz-bucket-namespace value.
func (n BucketNamespace) String() string {
	if n == BucketNamespaceAccountRegional {
		return "account-regional"
	}
	return "global"
}

// ParseBucketNamespace reads an x-amz-bucket-namespace value. Empty is global.
func ParseBucketNamespace(s string) (BucketNamespace, error) {
	switch s {
	case "", "global":
		return BucketNamespaceGlobal, nil
	case "account-regional":
		return BucketNamespaceAccountRegional, nil
	}
	return 0, fmt.Errorf("%w: bucket namespace %q", ErrInvalidArgument, s)
}

// Account returns the 12-digit account id of a project named "a" plus the
// account id, the shape vws-console-api gives every account's project.
// ok is false for any other project.
func (p Project) Account() (string, bool) {
	id, found := strings.CutPrefix(p.s, "a")
	if !found || len(id) != 12 || !allDigits(id) {
		return "", false
	}
	return id, true
}

// AccountRegionalSuffix returns "-{account}-{region}-an" for the project and
// region. ok is false when the project carries no account id or the region is
// unset.
func AccountRegionalSuffix(p Project, r Region) (string, bool) {
	account, ok := p.Account()
	if !ok || r.IsZero() {
		return "", false
	}
	return "-" + account + "-" + r.String() + accountRegionalSuffix, true
}

// CheckNamespace applies the S3 namespace naming rules to a create. An
// account-regional name must end with the caller's own suffix and keep a
// prefix before it. A global name must not end with "-an", which S3 keeps for
// the account regional namespace.
func (r CreateBucketRequest) CheckNamespace() error {
	name := r.Bucket.Name()
	if r.Namespace != BucketNamespaceAccountRegional {
		if strings.HasSuffix(name, accountRegionalSuffix) {
			return fmt.Errorf("%w: bucket name %q ends with %q, which is reserved for the account-regional namespace",
				ErrInvalidBucketNamespace, name, accountRegionalSuffix)
		}
		return nil
	}
	want, ok := AccountRegionalSuffix(r.Bucket.Project(), r.Region)
	switch {
	case !ok && r.Region.IsZero():
		return fmt.Errorf("%w: an account-regional bucket needs a region", ErrInvalidBucketNamespace)
	case !ok:
		return fmt.Errorf("%w: project %q has no 12-digit account id for the account-regional namespace",
			ErrInvalidBucketNamespace, r.Bucket.Project())
	}
	if prefix, found := strings.CutSuffix(name, want); found && prefix != "" {
		return nil
	}
	account, _ := r.Bucket.Project().Account()
	if other, found := suffixAccount(name); found && other != account {
		return fmt.Errorf("%w: the requested account id %q does not match the caller's account id %q; "+
			"the name must end with %q", ErrInvalidBucketNamespace, other, account, want)
	}
	return fmt.Errorf("%w: an account-regional bucket name must be a prefix followed by %q",
		ErrInvalidBucketNamespace, want)
}

// suffixAccount returns the 12-digit account id in an account-regional
// shaped name "{prefix}-{account}-{region}-an", if there is one.
func suffixAccount(name string) (string, bool) {
	rest, ok := strings.CutSuffix(name, accountRegionalSuffix)
	if !ok {
		return "", false
	}
	parts := strings.Split(rest, "-")
	// The account id is the last all-digit 12-character segment that has a
	// prefix before it and at least one region segment after it.
	for i := len(parts) - 2; i >= 1; i-- {
		if len(parts[i]) == 12 && allDigits(parts[i]) {
			return parts[i], true
		}
	}
	return "", false
}

func allDigits(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return s != ""
}
