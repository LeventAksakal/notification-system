# Limits from standards: acceptance tests the guard blocked (16 Nov 2026)

Written by the limits-from-standards session, branch `claude/feat-limits-from-standards-5989f4`.

## Status
`tools/agentguard` refused test-writer's Write to `test/acceptance/validation_test.go` in this worktree (owner item 0002). The hook matches paths relative to `$CLAUDE_PROJECT_DIR`, so a worktree path never matches `test/acceptance/**`. Per the brief, the guard was not routed around.

Consequence: **`test/acceptance` does not compile on this branch.** `validation_test.go` still refers to `notification.MaxContentLenFor`, `MaxRecipientLenFor`, `MaxEmailRecipientLen`, `MaxTemplateBodyLen` and `MaxTemplateVariableLen`. Those were replaced by `ContentLimit(ch)` and `RecipientLimit(ch)`, which return `Limit{Max, Unit}`, and by `MaxEmailRecipientBytes`, `MaxTemplateBodyBytes` and `MaxTemplateVariableBytes`. Run test-writer from the main checkout with the brief below before merging. The same SQL-level rows already pass in `internal/store/limit_checks_integration_test.go`.

## The changes to make in `test/acceptance/validation_test.go` (designed by test-writer, never compiled)
1. Imports: add `fmt` and `unicode`. Add these helpers:
   - `atAndPast(l notification.Limit)`. For bytes: `"é"×(Max/2) + "a"×(Max%2)`, and one past appends `"a"`. For characters: `"é"×Max`, and one past appends `"é"`.
   - `whiteSpaceRunes()`, which iterates `unicode.White_Space.R16` and `.R32`.
   - `notWhiteSpace = U+200B, U+FEFF, U+180E`.
2. `TestR10ContentLimitsPerChannel`: use `limit := notification.ContentLimit(ch)` and `at, past := atAndPast(limit)`. Content at the limit is stored intact; one unit past is refused with 400 naming `content`.
3. `TestR10RequiredAndMalformedFields`:
   - The 254 subtest uses `emailOfLen(notification.MaxEmailRecipientBytes)`.
   - Add a subtest that accepts the sms `to` "+12".
   - Delete "sms to too short" (`+123456` is now valid E.164).
   - Add these refusals, naming `content`: whitespace-only content `" \t 　"`.
   - Add these refusals, naming `to`:
     - whitespace-only push `to`;
     - "+0123456" (country code starting with 0);
     - "+1234567890123456" (16 digits);
     - "+";
     - `emailOfLen(MaxEmailRecipientBytes+1)`;
     - `strings.Repeat("é",249)+"@b.cd"` (254 characters, 503 bytes);
     - a push `to` of `MaxPushRecipientLen+1`.
   - After the table:
     - PUT a template "blank-render" with body "{{x}}", then create sms with variables {"x": " 　"}: refused naming `content`.
     - PUT "blank-body" with body `" \t\n　"`: refused naming `body`.
     - PUT "welcome_1": refused naming `name` (errors.go strips `path.`).
4. `TestR10LimitsAgreeSpecAndCore`:
   - TemplateWrite.body maxLength equals `MaxTemplateBodyBytes`; the variables value maxLength equals `MaxTemplateVariableBytes`.
   - The patterns of `NotificationCreate.to`, `NotificationCreate.content` and `TemplateWrite.body` equal `notification.NotBlankPattern`.
5. `TestR10LimitsAgreeDatabaseAndCore`:
   - Per channel, unit-aware rows: at the limit accepted; one past, empty and whitespace-only refused.
   - Every `unicode.White_Space` rune alone as content is refused, and `IsBlank` agrees; the three notWhiteSpace runes are accepted.
   - Recipients:
     - email: 254 ASCII bytes accepted, 255 refused, 254 characters over 254 bytes refused, whitespace-only refused;
     - push: 255 accepted, 256 refused, empty and whitespace-only refused;
     - sms: "+1234567" and "+12" accepted, "1234567", "+0123456" and 16 digits refused.
   - Templates:
     - accepted: a 63-character name with a body at the 32,768-byte limit;
     - refused: a 64-character name, a body one byte past, a whitespace-only body, "welcome_1", "Welcome", "welcome-", "Bad Name".
6. The command once written: `go vet -tags integration ./test/acceptance && go test -tags integration ./test/acceptance -run TestR10 -count=1 -p 1`.
