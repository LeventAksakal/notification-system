package main

import "testing"

func TestMajorReadsInfoVersionOnly(t *testing.T) {
	doc := []byte("openapi: 3.0.3\ncomponents:\n  schemas:\n    X:\n      version: 9.9.9\ninfo:\n  title: t\n  version: 2.1.0\npaths: {}\n")
	got, err := major(doc)
	if err != nil || got != 2 {
		t.Fatalf("major = %d, %v; want 2", got, err)
	}
	if _, err := major([]byte("info:\n  version: next\n")); err == nil {
		t.Fatal("a non-semver version was accepted")
	}
	if _, err := major([]byte("openapi: 3.0.3\n")); err == nil {
		t.Fatal("a document without info.version was accepted")
	}
}

func TestOnlyAMajorBumpMayBreakClients(t *testing.T) {
	for _, c := range []struct {
		name       string
		base, head int
		want       checkMode
		wantErr    bool
	}{
		{"same major is checked strictly", 1, 1, strict, false},
		{"a major bump lists the breaks and passes", 1, 2, report, false},
		{"a major going down is refused", 2, 1, strict, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			got, err := decide(c.base, c.head)
			if (err != nil) != c.wantErr || got != c.want {
				t.Fatalf("decide(%d, %d) = %v, %v", c.base, c.head, got, err)
			}
		})
	}
}
