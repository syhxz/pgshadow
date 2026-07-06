package replayguard

import (
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"pgshadow/pkg/core"
)

// Config configures production safeguards.
type Config struct {
	// DDL protection (#4)
	ExcludeDDL bool `yaml:"exclude_ddl"` // drop all DDL (CREATE/ALTER/DROP)

	// Sequence handling (#6): no-op, sequences replay naturally on shadow DB.
	RewriteSequences bool `yaml:"rewrite_sequences"` // reserved for future use

	// Time function rewriting (#7)
	RewriteTimeFunctions bool `yaml:"rewrite_time_functions"` // replace now()/current_timestamp with captured timestamp

	// Non-deterministic function handling (#8): always replay as-is.
	// This field is kept for config compatibility but has no effect.
	SkipNonDeterministic bool `yaml:"skip_non_deterministic"` // deprecated: no-op, non-deterministic SQL always replays

	// External dependency filtering (#9)
	SkipExternalDeps bool `yaml:"skip_external_deps"` // skip dblink/FDW/pg_notify calls

	// Backpressure (#10)
	MaxReplayLagSeconds float64 `yaml:"max_replay_lag_seconds"` // pause capture when lag exceeds this
	MaxQueueDepth       int     `yaml:"max_queue_depth"`        // pause when queue depth exceeds this

	// Large transaction protection (#11)
	MaxTransactionDuration time.Duration `yaml:"max_transaction_duration"` // timeout single transactions
	MaxStatementsPerTx     int           `yaml:"max_statements_per_tx"`    // split tx after N statements

	// Data masking (#14)
	MaskPatterns []MaskRule `yaml:"mask_patterns"` // regex-based data masking rules

	// Selective replay (#18)
	ReplayPercentage float64  `yaml:"replay_percentage"` // 0-100, percentage of traffic to replay
	IncludeTables    []string `yaml:"include_tables"`    // only replay SQL touching these tables
	ExcludeTables    []string `yaml:"exclude_tables"`    // skip SQL touching these tables
	IncludeUsers     []string `yaml:"include_users"`     // only replay from these source users
	ReadOnly         bool     `yaml:"read_only"`         // only replay SELECT (for query plan validation)
}

// MaskRule defines a regex-based data masking rule.
type MaskRule struct {
	Pattern     string `yaml:"pattern"`     // regex to match in SQL
	Replacement string `yaml:"replacement"` // replacement string
}

// Guard applies production safeguards to SQL events before replay.
type Guard struct {
	cfg Config

	// Compiled regexes
	ddlPattern       *regexp.Regexp
	timePatterns     []*regexp.Regexp
	externalPatterns []*regexp.Regexp
	maskPatterns     []compiledMask
	tablePatterns    []*regexp.Regexp
	excludeTablePat  []*regexp.Regexp

	// User filtering (lowercased for case-insensitive comparison)
	includeUsers map[string]struct{}

	// Backpressure state
	mu       sync.RWMutex
	paused   bool
	pausedAt time.Time

	// Selective replay counter (for percentage-based filtering)
	// Uses atomic operations for concurrent safety.
	counter uint64
}

type compiledMask struct {
	re          *regexp.Regexp
	replacement string
}

// New creates a Guard from config.
func New(cfg Config) *Guard {
	s := &Guard{cfg: cfg}

	// DDL pattern: matches DDL keywords at statement start, allowing leading
	// whitespace and SQL comments (-- line comments and /* block comments */).
	s.ddlPattern = regexp.MustCompile(`(?i)^\s*(?:--[^\n]*\n\s*|/\*[\s\S]*?\*/\s*)*(CREATE|ALTER|DROP|TRUNCATE|GRANT|REVOKE)\s`)

	// Time function patterns (#7)
	// Order matters: current_timestamp must be listed before current_time.
	// After current_timestamp is replaced, current_time won't false-match
	// on the remaining text.
	s.timePatterns = []*regexp.Regexp{
		regexp.MustCompile(`(?i)\bnow\s*\(\s*\)`),
		regexp.MustCompile(`(?i)\bcurrent_timestamp\b`),
		regexp.MustCompile(`(?i)\bcurrent_date\b`),
		regexp.MustCompile(`(?i)\bclock_timestamp\s*\(\s*\)`),
		regexp.MustCompile(`(?i)\bstatement_timestamp\s*\(\s*\)`),
		regexp.MustCompile(`(?i)\btransaction_timestamp\s*\(\s*\)`),
		regexp.MustCompile(`(?i)\bcurrent_time\b`),
	}

	// External dependency patterns (#9)
	s.externalPatterns = []*regexp.Regexp{
		regexp.MustCompile(`(?i)\bdblink\s*\(`),
		regexp.MustCompile(`(?i)\bdblink_exec\s*\(`),
		regexp.MustCompile(`(?i)\bpg_notify\s*\(`),
		regexp.MustCompile(`(?i)\bnotify\s+\w`),
		regexp.MustCompile(`(?i)\bfrom\s+\w+\.\w+\.\w+`), // FDW three-part names
		regexp.MustCompile(`(?i)\blo_import\s*\(`),
		regexp.MustCompile(`(?i)\blo_export\s*\(`),
		// Issue #13: Advisory locks can cause deadlocks on the target DB when
		// replayed, since the lock semantics depend on the original application's
		// coordination which is not present during replay.
		regexp.MustCompile(`(?i)\bpg_advisory_lock\s*\(`),
		regexp.MustCompile(`(?i)\bpg_advisory_xact_lock\s*\(`),
		regexp.MustCompile(`(?i)\bpg_try_advisory_lock\s*\(`),
		regexp.MustCompile(`(?i)\bpg_try_advisory_xact_lock\s*\(`),
	}

	// Table include patterns (#18)
	for _, t := range cfg.IncludeTables {
		pat := regexp.MustCompile(`(?i)\b` + regexp.QuoteMeta(t) + `\b`)
		s.tablePatterns = append(s.tablePatterns, pat)
	}
	for _, t := range cfg.ExcludeTables {
		pat := regexp.MustCompile(`(?i)\b` + regexp.QuoteMeta(t) + `\b`)
		s.excludeTablePat = append(s.excludeTablePat, pat)
	}

	// User include filter (#18): case-insensitive match on source user.
	if len(cfg.IncludeUsers) > 0 {
		s.includeUsers = make(map[string]struct{}, len(cfg.IncludeUsers))
		for _, u := range cfg.IncludeUsers {
			s.includeUsers[strings.ToLower(u)] = struct{}{}
		}
	}

	// Mask patterns (#14)
	for _, m := range cfg.MaskPatterns {
		re, err := regexp.Compile(m.Pattern)
		if err == nil {
			s.maskPatterns = append(s.maskPatterns, compiledMask{re: re, replacement: m.Replacement})
		}
	}

	return s
}

// Decision represents the safeguard's verdict on an event.
type Decision int

const (
	Allow Decision = iota
	Block          // drop the event entirely
	Rewrite        // event SQL was modified
)

// Stats tracks safeguard decisions for observability.
type Stats struct {
	Allowed   uint64
	Blocked   uint64
	Rewritten uint64
}

// Apply applies all safeguards to an event. Returns the decision and
// optionally modifies ev.SQL in place (for rewrites/masking).
// This method is safe for concurrent use.
func (s *Guard) Apply(ev *core.SQLEvent) Decision {
	sql := ev.SQL

	// Pre-compute stripped SQL (without string literal contents) for accurate
	// pattern matching that won't trigger on user data in quoted strings.
	// Optimization: skip stripping if no quotes or dollar signs exist (common case).
	hasQuotes := strings.IndexByte(sql, '\'') >= 0 || hasDollarQuote(sql)
	stripped := sql
	if hasQuotes {
		stripped = stripStringLiterals(sql)
	}

	// Data masking (#14) runs first, before any Block decision, so that if
	// the original SQL is logged upstream it has already been sanitized.
	rewritten := false
	for _, m := range s.maskPatterns {
		if m.re.MatchString(sql) {
			sql = m.re.ReplaceAllString(sql, m.replacement)
			rewritten = true
		}
	}
	if rewritten {
		ev.SQL = sql
		// Update stripped version after masking
		hasQuotes = strings.IndexByte(sql, '\'') >= 0 || hasDollarQuote(sql)
		if hasQuotes {
			stripped = stripStringLiterals(sql)
		} else {
			stripped = sql
		}
	}

	// Selective replay: percentage filter (#18)
	if s.cfg.ReplayPercentage > 0 && s.cfg.ReplayPercentage < 100 {
		cnt := atomic.AddUint64(&s.counter, 1)
		if float64(cnt%100) >= s.cfg.ReplayPercentage {
			return Block
		}
	}

	// User filter (#18): only replay from configured users.
	if len(s.includeUsers) > 0 {
		if _, ok := s.includeUsers[strings.ToLower(ev.User)]; !ok {
			return Block
		}
	}

	// Read-only mode (#18)
	if s.cfg.ReadOnly {
		if !isSelect(sql) {
			return Block
		}
	}

	// DDL filter (#4)
	if s.cfg.ExcludeDDL && s.ddlPattern.MatchString(sql) {
		return Block
	}

	// External dependencies (#9)
	if s.cfg.SkipExternalDeps {
		for _, pat := range s.externalPatterns {
			if pat.MatchString(stripped) {
				return Block
			}
		}
	}

	// Non-deterministic functions (#8): always replay as-is.
	// random()/gen_random_uuid() will produce different values on the shadow DB,
	// but skipping them would cause missing rows and cascading failures.

	// Sequence functions (#6): always replay as-is.
	// The shadow DB has independent sequences that advance in lockstep with
	// production when replaying the same SQL in order.

	// Table filters (#18): match against stripped SQL to avoid false positives
	// from table names appearing in string literals or comments.
	// Session-state commands (SET, RESET, DISCARD) always pass through table
	// filters because they affect the session context for subsequent queries
	// (e.g., SET timezone changes how time values are interpreted). Blocking
	// them would desync the shadow DB's session state from production (#7).
	if (len(s.tablePatterns) > 0 || len(s.excludeTablePat) > 0) && !isSessionCommand(sql) {
		if len(s.tablePatterns) > 0 {
			matched := false
			for _, pat := range s.tablePatterns {
				if pat.MatchString(stripped) {
					matched = true
					break
				}
			}
			if !matched {
				return Block
			}
		}
		if len(s.excludeTablePat) > 0 {
			for _, pat := range s.excludeTablePat {
				if pat.MatchString(stripped) {
					return Block
				}
			}
		}
	}

	// Time function rewriting (#7)
	if s.cfg.RewriteTimeFunctions && ev.Timestamp != (time.Time{}) {
		tsTimestamptz := ev.Timestamp.Format("'2006-01-02 15:04:05.000000-07:00'::timestamptz")
		tsDate := ev.Timestamp.Format("'2006-01-02'::date")
		tsTime := ev.Timestamp.Format("'15:04:05.000000-07:00'::timetz")
		for i, pat := range s.timePatterns {
			// Select the appropriate type cast for each time function:
			// index 2 = current_date → ::date
			// index 6 = current_time → ::timetz
			// all others → ::timestamptz
			var ts string
			switch i {
			case 2: // current_date
				ts = tsDate
			case 6: // current_time
				ts = tsTime
			default:
				ts = tsTimestamptz
			}
			// Re-strip after each replacement so that e.g. current_time
			// won't match inside already-replaced current_timestamp text.
			// Optimization: if no quotes, sql == stripped, use directly.
			curStripped := sql
			if strings.IndexByte(sql, '\'') >= 0 || hasDollarQuote(sql) {
				curStripped = stripStringLiterals(sql)
			}
			if pat.MatchString(curStripped) {
				if curStripped == sql {
					// No string literals — simple replace is safe
					sql = pat.ReplaceAllString(sql, ts)
				} else {
					sql = replaceOutsideStrings(sql, pat, ts)
				}
				rewritten = true
			}
		}
	}

	if rewritten {
		ev.SQL = sql
		return Rewrite
	}

	return Allow
}

// CheckBackpressure returns true if replay should be paused due to
// queue depth or replay lag exceeding configured thresholds (#10).
func (s *Guard) CheckBackpressure(queueDepth int, replayLagSec float64) bool {
	shouldPause := false

	if s.cfg.MaxQueueDepth > 0 && queueDepth > s.cfg.MaxQueueDepth {
		shouldPause = true
	}
	if s.cfg.MaxReplayLagSeconds > 0 && replayLagSec > s.cfg.MaxReplayLagSeconds {
		shouldPause = true
	}

	s.mu.Lock()
	if shouldPause && !s.paused {
		s.paused = true
		s.pausedAt = time.Now()
	} else if !shouldPause && s.paused {
		s.paused = false
	}
	s.mu.Unlock()

	return shouldPause
}

// IsPaused returns current backpressure state.
func (s *Guard) IsPaused() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.paused
}

// CheckTransaction returns Block if the transaction exceeds configured limits (#11).
func (s *Guard) CheckTransaction(txStartTime time.Time, stmtCount int) Decision {
	if s.cfg.MaxTransactionDuration > 0 && time.Since(txStartTime) > s.cfg.MaxTransactionDuration {
		return Block
	}
	if s.cfg.MaxStatementsPerTx > 0 && stmtCount > s.cfg.MaxStatementsPerTx {
		return Block
	}
	return Allow
}

// stripStringLiterals replaces the content of single-quoted string literals
// and dollar-quoted strings ($$...$$ or $tag$...$tag$) with empty strings, so
// regex matching does not operate on user data.
// Handles escaped quotes ('') inside single-quoted literals.
func stripStringLiterals(sql string) string {
	var b strings.Builder
	b.Grow(len(sql))
	inString := false
	for i := 0; i < len(sql); i++ {
		if inString {
			if sql[i] == '\'' {
				if i+1 < len(sql) && sql[i+1] == '\'' {
					// Escaped quote inside string literal, skip both
					i++
					continue
				}
				// End of string literal
				inString = false
				b.WriteByte('\'')
			}
			// Skip characters inside string literals
		} else {
			if sql[i] == '\'' {
				inString = true
				b.WriteByte('\'')
			} else if sql[i] == '$' {
				// Check for dollar-quoted string: $$ or $tag$
				tag := parseDollarTag(sql, i)
				if tag != "" {
					// Write the opening tag, skip content, write closing tag
					b.WriteString(tag)
					// Advance past opening tag
					i += len(tag)
					// Find closing tag
					end := strings.Index(sql[i:], tag)
					if end >= 0 {
						i += end + len(tag) - 1 // -1 because loop will i++
						b.WriteString(tag)
					} else {
						// Unterminated dollar-quote — emit rest as-is
						b.WriteString(sql[i:])
						return b.String()
					}
				} else {
					b.WriteByte(sql[i])
				}
			} else {
				b.WriteByte(sql[i])
			}
		}
	}
	return b.String()
}

// parseDollarTag checks if sql[pos] starts a dollar-quote tag ($$ or $tag$).
// Returns the full tag string (e.g. "$$" or "$fn$") if valid, empty otherwise.
func parseDollarTag(sql string, pos int) string {
	if pos >= len(sql) || sql[pos] != '$' {
		return ""
	}
	// Find the closing $ of the tag
	for j := pos + 1; j < len(sql); j++ {
		if sql[j] == '$' {
			// Tag is sql[pos:j+1]
			return sql[pos : j+1]
		}
		// Dollar-quote tags can only contain letters, digits, underscore
		c := sql[j]
		if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_') {
			return ""
		}
	}
	return ""
}

// hasDollarQuote does a quick scan for any potential dollar-quote opening tag
// ($$ or $tag$) in the SQL string. Used as a fast-path check before invoking
// the full stripStringLiterals parser.
func hasDollarQuote(sql string) bool {
	for i := 0; i < len(sql); i++ {
		if sql[i] == '$' && parseDollarTag(sql, i) != "" {
			return true
		}
	}
	return false
}

// replaceOutsideStrings replaces regex matches in sql, but only those that
// occur outside single-quoted string literals and dollar-quoted strings.
// Matches inside any quoted context are left untouched.
func replaceOutsideStrings(sql string, pat *regexp.Regexp, replacement string) string {
	var result strings.Builder
	result.Grow(len(sql) + 64)

	inString := false
	segStart := 0

	for i := 0; i < len(sql); i++ {
		if inString {
			if sql[i] == '\'' {
				if i+1 < len(sql) && sql[i+1] == '\'' {
					i++ // skip escaped quote
					continue
				}
				// End of string literal — emit the literal segment as-is
				result.WriteString(sql[segStart : i+1])
				segStart = i + 1
				inString = false
			}
		} else {
			if sql[i] == '\'' {
				// Entering a string literal — apply regex to the segment before it
				segment := sql[segStart:i]
				result.WriteString(pat.ReplaceAllString(segment, replacement))
				result.WriteByte('\'')
				segStart = i + 1
				inString = true
			} else if sql[i] == '$' {
				// Check for dollar-quoted string
				tag := parseDollarTag(sql, i)
				if tag != "" {
					// Apply regex to the segment before the dollar-quote
					segment := sql[segStart:i]
					result.WriteString(pat.ReplaceAllString(segment, replacement))
					// Write opening tag
					result.WriteString(tag)
					// Advance past opening tag
					inner := i + len(tag)
					// Find closing tag
					end := strings.Index(sql[inner:], tag)
					if end >= 0 {
						// Write content as-is (including closing tag)
						result.WriteString(sql[inner : inner+end])
						result.WriteString(tag)
						i = inner + end + len(tag) - 1 // -1 for loop i++
						segStart = i + 1
					} else {
						// Unterminated — emit rest as-is
						result.WriteString(sql[inner:])
						return result.String()
					}
				}
			}
		}
	}

	// Handle remaining segment
	remaining := sql[segStart:]
	if inString {
		// Unterminated string literal — emit as-is
		result.WriteString(remaining)
	} else {
		result.WriteString(pat.ReplaceAllString(remaining, replacement))
	}

	return result.String()
}

func isSelect(sql string) bool {
	trimmed := strings.TrimSpace(sql)
	upper := strings.ToUpper(trimmed)
	return strings.HasPrefix(upper, "SELECT") ||
		strings.HasPrefix(upper, "WITH") ||
		strings.HasPrefix(upper, "EXPLAIN")
}

// isSessionCommand returns true for session-state commands (SET, RESET, DISCARD)
// that should always pass through table filters to maintain session consistency
// between production and shadow DB (#7).
func isSessionCommand(sql string) bool {
	trimmed := strings.TrimSpace(sql)
	upper := strings.ToUpper(trimmed)
	return strings.HasPrefix(upper, "SET ") ||
		strings.HasPrefix(upper, "RESET ") ||
		strings.HasPrefix(upper, "DISCARD ")
}
