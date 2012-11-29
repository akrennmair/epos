package epos

import (
	"testing"
)

func TestExpressionParser(t *testing.T) {
	testdata := []struct {
		Expr string
		ShouldFail bool
	} {
		{ "(id 1)", false },
		{ "(foobar)", true },
		{ "(or (id 23) (id 42))", false },
		{ "(eq id_str 3738888)", false },
		{ "(eq)", true },
		{ "(id)", true },
		{ "(eq foo)", true },
		{ "(or)", true },
		{ "(and)", true },
	}

	for i, tt := range testdata {
		_, err := Expression(tt.Expr)
		if !tt.ShouldFail {
			if err != nil {
				t.Errorf("%d. parsing expression '%s' failed: %v", i, tt.Expr, err)
			}
		} else {
			if err == nil {
				t.Errorf("%d. expression '%s' should have delivered an error, but parses fine.", i, tt.Expr)
			}
		}
	}
}
