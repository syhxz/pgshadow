package filter

import (
	"strings"
	"sync"
)

// This file implements the statement classifier (task 5.1, R5.1 and R5.3).
//
// Classify performs case-insensitive leading-keyword classification into a
// StmtClass. For SELECT statements it applies the Procedure_Invoking_SELECT
// heuristic (IsProcInvokingSelect) to distinguish a side-effecting
// procedure/function invocation from a read-only Plain_SELECT.
//
// Robustness: the classifier tokenizes SQL while skipping leading whitespace,
// line comments (-- ... ), block comments (/* ... */, nestable), single-quoted
// string literals, dollar-quoted strings ($tag$ ... $tag$) and treats
// double-quoted identifiers as identifiers. This prevents parentheses or
// keywords inside literals/comments from confusing the heuristic.

// defaultBuiltinSet is the set form of DefaultBuiltinFunctions, built once.
var defaultBuiltinSet = func() map[string]bool {
	m := make(map[string]bool, len(DefaultBuiltinFunctions))
	for _, f := range DefaultBuiltinFunctions {
		m[strings.ToLower(f)] = true
	}
	return m
}()

// sqlKeywordsBeforeParen are tokens that may legitimately appear immediately
// before '(' but are NOT user-defined function/procedure invocations. They are
// excluded from the proc-invoking heuristic so that constructs such as
// "IN (...)", "VALUES (...)", subqueries ("FROM (SELECT ...)"), and type
// specifiers ("numeric(10,2)") are not misread as procedure calls.
var sqlKeywordsBeforeParen = map[string]bool{
	// clause / predicate keywords
	"as": true, "in": true, "exists": true, "all": true, "any": true,
	"some": true, "values": true, "over": true, "filter": true,
	"within": true, "from": true, "where": true, "and": true, "or": true,
	"not": true, "on": true, "using": true, "when": true, "then": true,
	"case": true, "by": true, "having": true, "group": true, "order": true,
	"select": true, "union": true, "intersect": true, "except": true,
	"join": true, "into": true, "distinct": true, "between": true,
	"like": true, "ilike": true, "similar": true, "is": true, "null": true,
	"returning": true, "do": true, "else": true, "end": true, "limit": true,
	"offset": true, "fetch": true, "with": true, "recursive": true,
	"left": true, "right": true, "full": true, "inner": true, "outer": true,
	"cross": true, "lateral": true, "natural": true, "asc": true,
	"desc": true, "nulls": true, "first": true, "last": true, "for": true,
	"of": true, "only": true, "tablesample": true, "window": true,
	"partition": true, "range": true, "rows": true, "groups": true,
	"unbounded": true, "preceding": true, "following": true, "current": true,
	"row": true, "grouping": true, "cube": true, "rollup": true,
	"sets": true, "array": true, "set": true, "to": true, "if": true,
	// type names that take a parenthesized precision/length
	"numeric": true, "decimal": true, "dec": true, "char": true,
	"character": true, "varchar": true, "varbit": true, "bit": true,
	"time": true, "timestamp": true, "timestamptz": true, "interval": true,
	"float": true, "double": true, "real": true, "int": true,
	"integer": true, "bigint": true, "smallint": true, "money": true,
	"bool": true, "boolean": true, "text": true, "bytea": true,
	"uuid": true, "json": true, "jsonb": true, "xml": true,
}

// classifier is the default Classifier implementation.
type classifier struct {
	builtins map[string]bool
}

// NewClassifier returns a Classifier whose proc-invoking heuristic excludes the
// baseline DefaultBuiltinFunctions plus any additional names supplied via
// extraBuiltins (Config.BuiltinFunctions, R5.3). Matching is case-insensitive.
// Passing nil/empty uses the baseline set only.
func NewClassifier(extraBuiltins []string) Classifier {
	if len(extraBuiltins) == 0 {
		return &classifier{builtins: defaultBuiltinSet}
	}
	m := make(map[string]bool, len(defaultBuiltinSet)+len(extraBuiltins))
	for k := range defaultBuiltinSet {
		m[k] = true
	}
	for _, f := range extraBuiltins {
		m[strings.ToLower(f)] = true
	}
	return &classifier{builtins: m}
}

// Classify performs case-insensitive leading-keyword classification (R5.1),
// splitting SELECT into ClassPlainSelect vs ClassProcSelect via the heuristic
// (R5.3). Unrecognized or empty input yields ClassUnknown.
func (c *classifier) Classify(sql string) StmtClass {
	toks := lex(sql)
	defer tokPool.Put(toks[:0])
	leading := firstWord(toks)
	if leading == "" {
		return ClassUnknown
	}
	switch leading {
	case "call":
		return ClassCall
	case "insert", "update", "delete":
		return ClassDML
	case "create", "alter", "drop":
		return ClassDDL
	case "begin", "start":
		return ClassBegin
	case "commit", "rollback", "end":
		return ClassCommitRollback
	case "set", "reset", "discard":
		return ClassUtility
	case "copy":
		// COPY ... FROM is replayable; COPY ... TO is a read and is not.
		if depthZeroHasWord(toks, "from") {
			return ClassCopyFrom
		}
		return ClassUnknown
	case "select":
		if c.isProcInvoking(toks) {
			return ClassProcSelect
		}
		return ClassPlainSelect
	case "with":
		// Resolve the primary command governed by the WITH (CTE) prefix.
		switch primaryCommandAfterWith(toks) {
		case "select":
			if c.isProcInvoking(toks) {
				return ClassProcSelect
			}
			return ClassPlainSelect
		case "insert", "update", "delete":
			return ClassDML
		default:
			return ClassUnknown
		}
	default:
		return ClassUnknown
	}
}

// IsProcInvokingSelect implements the heuristic of R5.3. It returns true only
// for SELECT statements (including WITH ... SELECT) whose text contains a
// user-defined function or stored-procedure invocation: a non-builtin,
// non-keyword identifier (optionally schema-qualified) immediately followed by
// a parenthesized argument list, e.g. SELECT func(...), SELECT * FROM proc(...),
// or SELECT schema.do_work(...). Calls to known PostgreSQL built-in/aggregate
// functions (now, count, generate_series, ...) do not qualify.
func (c *classifier) IsProcInvokingSelect(sql string) bool {
	toks := lex(sql)
	defer tokPool.Put(toks[:0])
	leading := firstWord(toks)
	isSelect := leading == "select"
	if leading == "with" {
		isSelect = primaryCommandAfterWith(toks) == "select"
	}
	if !isSelect {
		return false
	}
	return c.isProcInvoking(toks)
}

// isProcInvoking scans the token stream for a non-builtin, non-keyword
// identifier (possibly schema-qualified) immediately followed by '('.
func (c *classifier) isProcInvoking(toks []token) bool {
	for i := 0; i < len(toks); i++ {
		if toks[i].kind != tokWord {
			continue
		}
		// Walk a qualified name: word (dot word)* ; the function name is the
		// final segment.
		firstSeg := strings.ToLower(toks[i].text)
		lastSeg := firstSeg
		j := i
		for j+2 < len(toks) && toks[j+1].kind == tokDot && toks[j+2].kind == tokWord {
			lastSeg = strings.ToLower(toks[j+2].text)
			j += 2
		}
		// Is this identifier (chain) immediately followed by '('?
		if j+1 < len(toks) && toks[j+1].kind == tokLParen {
			if !sqlKeywordsBeforeParen[firstSeg] &&
				!sqlKeywordsBeforeParen[lastSeg] &&
				!c.builtins[lastSeg] {
				return true
			}
		}
		i = j
	}
	return false
}

// firstWord returns the lowercased text of the first word token, or "".
func firstWord(toks []token) string {
	for _, t := range toks {
		if t.kind == tokWord {
			return strings.ToLower(t.text)
		}
	}
	return ""
}

// depthZeroHasWord reports whether the given lowercased word appears as a word
// token at parenthesis depth zero.
func depthZeroHasWord(toks []token, word string) bool {
	depth := 0
	for _, t := range toks {
		switch t.kind {
		case tokLParen:
			depth++
		case tokRParen:
			if depth > 0 {
				depth--
			}
		case tokWord:
			if depth == 0 && strings.ToLower(t.text) == word {
				return true
			}
		}
	}
	return false
}

// primaryCommandAfterWith resolves the governing command of a WITH (CTE)
// statement by returning the first of select/insert/update/delete found among
// the depth-zero word tokens. CTE bodies live at depth > 0 and are skipped.
func primaryCommandAfterWith(toks []token) string {
	depth := 0
	for _, t := range toks {
		switch t.kind {
		case tokLParen:
			depth++
		case tokRParen:
			if depth > 0 {
				depth--
			}
		case tokWord:
			if depth == 0 {
				switch strings.ToLower(t.text) {
				case "select", "insert", "update", "delete":
					return strings.ToLower(t.text)
				}
			}
		}
	}
	return ""
}

// --- lightweight SQL lexer ---------------------------------------------------

// tokPool recycles token slices so the hot-path lex() avoids a heap allocation
// per SQL statement. Each slice is reset to zero length before reuse.
var tokPool = sync.Pool{
	New: func() any { return make([]token, 0, 32) },
}

type tokKind int

const (
	tokWord   tokKind = iota // identifier or keyword (also unquoted)
	tokLParen                // (
	tokRParen                // )
	tokDot                   // .
	tokString                // string / dollar-quoted literal (content elided)
	tokOther                 // any other punctuation/operator
)

type token struct {
	kind tokKind
	text string // populated for tokWord only
}

func isIdentStart(c byte) bool {
	return c == '_' ||
		(c >= 'a' && c <= 'z') ||
		(c >= 'A' && c <= 'Z') ||
		c >= 0x80 // allow UTF-8 identifier bytes
}

func isIdentPart(c byte) bool {
	return isIdentStart(c) || (c >= '0' && c <= '9')
}

// lex tokenizes SQL, dropping whitespace and comments and eliding the content
// of string and dollar-quoted literals. The returned slice is borrowed from a
// pool and must be returned via putTokens when no longer needed.
func lex(sql string) []token {
	toks := tokPool.Get().([]token)[:0]
	i, n := 0, len(sql)
	for i < n {
		c := sql[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' || c == '\v':
			i++
		case c == '-' && i+1 < n && sql[i+1] == '-':
			i += 2
			for i < n && sql[i] != '\n' {
				i++
			}
		case c == '/' && i+1 < n && sql[i+1] == '*':
			i += 2
			depth := 1
			for i < n && depth > 0 {
				if i+1 < n && sql[i] == '/' && sql[i+1] == '*' {
					depth++
					i += 2
					continue
				}
				if i+1 < n && sql[i] == '*' && sql[i+1] == '/' {
					depth--
					i += 2
					continue
				}
				i++
			}
		case c == '\'':
			i++
			for i < n {
				if sql[i] == '\'' {
					if i+1 < n && sql[i+1] == '\'' { // escaped quote
						i += 2
						continue
					}
					i++
					break
				}
				i++
			}
			toks = append(toks, token{kind: tokString})
		case c == '"':
			i++
			var sb strings.Builder
			for i < n {
				if sql[i] == '"' {
					if i+1 < n && sql[i+1] == '"' { // escaped quote
						sb.WriteByte('"')
						i += 2
						continue
					}
					break
				}
				sb.WriteByte(sql[i])
				i++
			}
			i++ // consume closing quote (if present)
			toks = append(toks, token{kind: tokWord, text: sb.String()})
		case c == '$':
			if tag, adv, ok := dollarTag(sql, i); ok {
				closeTag := "$" + tag + "$"
				i += adv
				if idx := strings.Index(sql[i:], closeTag); idx < 0 {
					i = n
				} else {
					i += idx + len(closeTag)
				}
				toks = append(toks, token{kind: tokString})
			} else {
				// $1-style parameter placeholder or stray '$'
				i++
				for i < n && sql[i] >= '0' && sql[i] <= '9' {
					i++
				}
				toks = append(toks, token{kind: tokOther})
			}
		case c == '(':
			toks = append(toks, token{kind: tokLParen})
			i++
		case c == ')':
			toks = append(toks, token{kind: tokRParen})
			i++
		case c == '.':
			toks = append(toks, token{kind: tokDot})
			i++
		case isIdentStart(c):
			start := i
			i++
			for i < n && isIdentPart(sql[i]) {
				i++
			}
			toks = append(toks, token{kind: tokWord, text: sql[start:i]})
		default:
			toks = append(toks, token{kind: tokOther})
			i++
		}
	}
	return toks
}

// dollarTag detects a dollar-quote opener at sql[i] (sql[i] == '$'). It returns
// the tag (possibly empty for $$), the number of bytes consumed for the opener,
// and whether an opener was recognized. A digit immediately after '$' indicates
// a parameter placeholder ($1), not a dollar quote.
func dollarTag(sql string, i int) (string, int, bool) {
	n := len(sql)
	// sql[i] == '$'
	j := i + 1
	for j < n && isIdentPart(sql[j]) {
		j++
	}
	if j < n && sql[j] == '$' {
		tag := sql[i+1 : j]
		// A tag must be empty or start with a non-digit (letter/underscore).
		if tag == "" || isIdentStart(tag[0]) {
			return tag, (j - i) + 1, true
		}
	}
	return "", 0, false
}
