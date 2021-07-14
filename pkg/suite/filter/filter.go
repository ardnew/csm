package filter

import (
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"

	"github.com/ardnew/csm/log"
)

type Filter struct {
	valid bool
	field string
	op    FilterOp
	args  string
}

type Filters []Filter

func (f Filter) String() string {
	return fmt.Sprintf("{ %q %s %q }", f.field, f.op, f.args)
}

func (f *Filter) SetValid(valid bool) { f.valid = valid }
func (f Filter) Valid() bool          { return f.valid }
func (f Filter) Field() string        { return f.field }
func (f Filter) Args() string         { return f.args }

func (f *Filter) Eval(v string) bool {
	invalidate := func() {
		log.Msg(log.Warn, "filter", "disabling invalid filter: %s", f.String())
		f.SetValid(false)
	}

	if f.op == opRE {
		ok, err := regexp.MatchString(f.args, v)
		if nil != err {
			invalidate()
			return false
		}
		return ok
	}

	// Unsigned int
	uv, uve := strconv.ParseUint(v, 0, 64)
	ua, uae := strconv.ParseUint(f.args, 0, 64)
	// Signed int
	iv, ive := strconv.ParseInt(v, 0, 64)
	ia, iae := strconv.ParseInt(f.args, 0, 64)
	// Float
	fv, fve := strconv.ParseFloat(v, 64)
	fa, fae := strconv.ParseFloat(f.args, 64)

	switch f.op {
	case opEQ:
		// Bool
		bv, bve := strconv.ParseBool(v)
		ba, bae := strconv.ParseBool(f.args)
		if nil == bve && nil == bae {
			return bv == ba
		}
		// Unsigned int
		if nil == uve && nil == uae {
			return uv == ua
		}
		// Signed int
		if nil == ive && nil == iae {
			return iv == ia
		}
		// Float
		if nil == fve && nil == fae {
			return math.Abs(fa-fv) < 1e-8
		}
		// String
		return v == f.args

	case opGT:
		// Unsigned int
		if nil == uve && nil == uae {
			return uv > ua
		}
		// Signed int
		if nil == ive && nil == iae {
			return iv > ia
		}
		// Float
		if nil == fve && nil == fae {
			return fv > fa
		}
		// String
		return v > f.args

	case opGE:
		// Unsigned int
		if nil == uve && nil == uae {
			return uv >= ua
		}
		// Signed int
		if nil == ive && nil == iae {
			return iv >= ia
		}
		// Float
		if nil == fve && nil == fae {
			return fv >= fa
		}
		// String
		return v >= f.args

	case opLT:
		// Unsigned int
		if nil == uve && nil == uae {
			return uv < ua
		}
		// Signed int
		if nil == ive && nil == iae {
			return iv < ia
		}
		// Float
		if nil == fve && nil == fae {
			return fv < fa
		}
		// String
		return v < f.args

	case opLE:
		// Unsigned int
		if nil == uve && nil == uae {
			return uv <= ua
		}
		// Signed int
		if nil == ive && nil == iae {
			return iv <= ia
		}
		// Float
		if nil == fve && nil == fae {
			return fv <= fa
		}
		// String
		return v <= f.args

	case opIN:
		// TODO
		invalidate()
		return false

	default:
		invalidate()
		return false
	}
}

func (f Filters) String() string {
	fs := []string{}
	for _, s := range f {
		fs = append(fs, s.String())
	}
	return strings.Join(fs, ",")

}

func (f *Filters) Set(s string) error {
	if field, args, op := splitFilter(s); opError != op {
		*f = append(*f, Filter{field: field, op: op, args: args})
		return nil
	}
	return fmt.Errorf("unrecognized filter: %q", s)
}

type FilterOp int

const (
	opError FilterOp = iota
	opEQ             // ==
	opGT             // >>
	opGE             // >=
	opLT             // <<
	opLE             // <=
	opRE             // =~
	opIN             // ..
	opCount
)

func ParseOp(s string) FilterOp {
	s = strings.TrimSpace(s)
	for o := FilterOp(0); o < opCount; o++ {
		if o.String() == s {
			return o
		}
	}
	return opError
}

func (o FilterOp) String() string {
	switch o {
	case opEQ:
		return "=="
	case opGT:
		return ">>"
	case opGE:
		return ">="
	case opLT:
		return "<<"
	case opLE:
		return "<="
	case opRE:
		return "=~"
	case opIN:
		return ".."
	}
	return ""
}

func splitFilter(s string) (field, args string, op FilterOp) {
	m := map[string]FilterOp{}
	for o := FilterOp(0); o < opCount; o++ {
		m[o.String()] = o
	}
	for tok, op := range m {
		if n := strings.Index(s, tok); n > 0 && n+2 < len(s) {
			return strings.TrimSpace(s[:n]), strings.TrimSpace(s[n+2:]), op
		}
	}
	return "", "", opError
}
