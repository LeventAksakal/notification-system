package main

import (
	"reflect"
	"testing"
)

func TestPackagesOf(t *testing.T) {
	got := PackagesOf([]string{"internal/api/h.go", "internal/api/h_test.go", "main.go", "README.md", "cmd/app/main.go"})
	want := []string{".", "./cmd/app", "./internal/api"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}
