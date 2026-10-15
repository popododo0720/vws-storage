package kube

import (
	"testing"

	"github.com/popododo0720/vws-storage/internal/model"
)

func TestCloneAllowed(t *testing.T) {
	img, err := model.ParseImageName("projects/a123456789012/regions/vws-1/images/ami-0123456789abcdef0")
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		name   string
		labels map[string]string
		want   bool
	}{
		{"owner", map[string]string{LabelProject: "a123456789012", LabelRegion: "vws-1"}, true},
		{"public of another project", map[string]string{LabelProject: "b999999999999", LabelRegion: "vws-1", LabelPublic: "true"}, true},
		{"public without an owner", map[string]string{LabelRegion: "vws-1", LabelPublic: "true"}, true},
		{"private of another project", map[string]string{LabelProject: "b999999999999", LabelRegion: "vws-1"}, false},
		{"public is not true", map[string]string{LabelProject: "b999999999999", LabelRegion: "vws-1", LabelPublic: "yes"}, false},
		{"public in another region", map[string]string{LabelProject: "b999999999999", LabelRegion: "vws-2", LabelPublic: "true"}, false},
		{"owner in another region", map[string]string{LabelProject: "a123456789012", LabelRegion: "vws-2"}, false},
		{"public without a region", map[string]string{LabelPublic: "true"}, false},
		{"no labels", nil, false},
	} {
		if got := cloneAllowed(c.labels, img); got != c.want {
			t.Errorf("%s: %v, want %v", c.name, got, c.want)
		}
	}
}
