package main

import "testing"

func TestDefaultZoneIsVWS1A(t *testing.T) {
	zones, err := parseZones(defaultZones)
	if err != nil {
		t.Fatal(err)
	}
	if len(zones) != 1 || zones[0].String() != "vws-1a" {
		t.Fatalf("zones %v", zones)
	}
}
