package main

import (
	"slices"
	"testing"
)

func TestTriggerBodiesReachGooseAsOneStatement(t *testing.T) {
	in := "ALTER TABLE `t` ADD COLUMN `s` int NULL;\n" +
		"CREATE TRIGGER `t_bi` before insert ON `t` FOR EACH ROW begin\n" +
		"set NEW.s = 1;\n" +
		"end;\n" +
		"DROP TRIGGER IF EXISTS `old`;\n"
	want := "ALTER TABLE `t` ADD COLUMN `s` int NULL;\n" +
		"-- +goose StatementBegin\n" +
		"CREATE TRIGGER `t_bi` before insert ON `t` FOR EACH ROW begin\n" +
		"set NEW.s = 1;\n" +
		"end;\n" +
		"-- +goose StatementEnd\n" +
		"DROP TRIGGER IF EXISTS `old`;\n"
	if got := wrapTriggers(in); got != want {
		t.Fatalf("wrapTriggers:\n%s\nwant:\n%s", got, want)
	}
}

func TestATriggerWithANestedBlockStaysOneStatement(t *testing.T) {
	in := "CREATE TRIGGER `t_bu` before update ON `t` FOR EACH ROW begin\n" +
		"if NEW.s = 1 then begin\n" +
		"set NEW.s = 2;\n" +
		"end;\n" +
		"end if;\n" +
		"end;\n" +
		"ALTER TABLE `t` DROP COLUMN `x`;\n"
	want := "-- +goose StatementBegin\n" +
		"CREATE TRIGGER `t_bu` before update ON `t` FOR EACH ROW begin\n" +
		"if NEW.s = 1 then begin\n" +
		"set NEW.s = 2;\n" +
		"end;\n" +
		"end if;\n" +
		"end;\n" +
		"-- +goose StatementEnd\n" +
		"ALTER TABLE `t` DROP COLUMN `x`;\n"
	if got := wrapTriggers(in); got != want {
		t.Fatalf("wrapTriggers:\n%s\nwant:\n%s", got, want)
	}
}

func TestTriggersSchemaNoLongerDeclaresAreDropped(t *testing.T) {
	current := "CREATE TABLE t (id int);\nCREATE TRIGGER `Keep` BEFORE INSERT ON t FOR EACH ROW begin\nset NEW.id = 1;\nend;\n" +
		"CREATE TRIGGER gone BEFORE UPDATE ON t FOR EACH ROW begin\nset NEW.id = 1;\nend;\n"
	desired := "CREATE TABLE t (id int);\ncreate trigger keep before insert on t for each row begin\nset NEW.id = 1;\nend;\n"
	stale := missing(triggerNames(current), triggerNames(desired))
	if !slices.Equal(stale, []string{"gone"}) {
		t.Fatalf("stale triggers = %v, want [gone]", stale)
	}
	if got, want := dropTriggers(stale), "DROP TRIGGER IF EXISTS `gone`;\n"; got != want {
		t.Fatalf("dropTriggers = %q, want %q", got, want)
	}
}

func TestSQLCSchemaHasNoTriggers(t *testing.T) {
	export := "CREATE TABLE t (id int);\n\nCREATE TRIGGER a BEFORE INSERT ON t FOR EACH ROW begin\nset NEW.id = 1;\nend;\n\n" +
		"CREATE TRIGGER b BEFORE UPDATE ON t FOR EACH ROW SET NEW.id = 2;\n\nCREATE TABLE u (id int);\n"
	if got, want := stripTriggers(export), "CREATE TABLE t (id int);\n\nCREATE TABLE u (id int);\n"; got != want {
		t.Fatalf("stripTriggers = %q, want %q", got, want)
	}
}
