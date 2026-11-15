# Decisions in force

Accepted decisions only; this list is what agents load. Superseded records move to `superseded/` with a banner pointing at their replacement. `go test ./tools/adrcheck` enforces both.

Records 0002 to 0019 in this folder are still `proposed`, so they are not listed here. Each was written before its slice was built; the code follows the recommended option, and where it does not the record says so under "As built". Accepting them is the owner's step. `docs/REQUIREMENTS.md` names the record behind each requirement.

- [Record decisions as short, checkable ADRs (0001)](0001-record-decisions-as-adrs.md)
- [One settings schema in code; secret values come from the platform, never from the repository (0020)](0020-one-settings-schema-secrets-from-the-platform.md)
- [main moves by fast-forward; releases are cut by hand from a green commit (0021)](0021-fast-forward-main-releases-cut-by-hand.md)
- [Packages are named for their role; contracts and settings sit at the root (0022)](0022-packages-by-role-contracts-at-the-root.md)
- [schema/ is the desired state, diffed into goose migrations by sqldef (0023)](0023-schema-as-code-with-sqldef.md)
