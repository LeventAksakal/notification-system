package support

// Images match docker-compose.yml exactly (tag and digest), so tests run
// against what the stack runs; TestImagesMatchCompose fails if they drift.
const (
	MySQLImage  = "mysql:8.4@sha256:a81d043930e51745522bd5c430961cf8286f0a23f0a9a2a6e0108f7ea71b0902"
	ValkeyImage = "valkey/valkey:9.0.7-alpine3.24@sha256:2d04268dfa26ab31cfa3fdc538866e76438b2d00e41db41b8cb86d780ac5a416"
)
