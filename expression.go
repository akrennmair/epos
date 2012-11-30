package epos

import (
	"errors"
	"fmt"
	"github.com/feyeleanor/atomiser"
	"github.com/feyeleanor/chain"
	"strconv"
	"strings"
)

// Expression converts a S-Expr-based query to a structure of Condition objects.
// The following symbols are available for queries:
//
//    (id 1)					query entry with ID 1
//    (eq field-name value)		query all entries where field-name equals value
//    (or expr...)              OR all query sub-expressions
//    (and expr...)             AND all query sub-expressions
func Expression(s string) (Condition, error) {
	expr := atomiser.NewAtomiser(strings.NewReader(s)).ReadList()

	if expr == nil {
		return nil, fmt.Errorf("parsing '%s' failed", s)
	}

	return parseExpressionToCondition(expr)
}

func parseExpressionToCondition(expr *chain.Cell) (Condition, error) {
	sym, ok := expr.Car().(atomiser.Symbol)
	if !ok {
		return nil, fmt.Errorf("expected symbol, got '%v' instead", expr.Car())
	}

	sym = atomiser.Symbol(strings.ToLower(string(sym)))

	switch sym {
	case "and":
		return parseAnd(expr.Cdr())
	case "or":
		return parseOr(expr.Cdr())
	case "eq":
		return parseEqual(expr.Cdr())
	case "id":
		return parseId(expr.Cdr())
	}
	return nil, fmt.Errorf("unknown symbol '%s'", sym)
}

func parseAnd(expr *chain.Cell) (Condition, error) {
	cond := &And{}

	cur := expr
	for cur != nil {
		if subexpr, ok := cur.Car().(*chain.Cell); ok {
			if subcond, err := parseExpressionToCondition(subexpr); err != nil {
				return nil, err
			} else {
				*cond = append(*cond, subcond)
			}
		}
		cur = cur.Cdr()
	}

	if len(*cond) == 0 {
		return nil, errors.New("empty and expression")
	}

	return cond, nil
}

func parseOr(expr *chain.Cell) (Condition, error) {
	cond := &Or{}

	cur := expr
	for cur != nil {
		if subexpr, ok := cur.Car().(*chain.Cell); ok {
			if subcond, err := parseExpressionToCondition(subexpr); err != nil {
				return nil, err
			} else {
				*cond = append(*cond, subcond)
			}
		}
		cur = cur.Cdr()
	}

	if len(*cond) == 0 {
		return nil, errors.New("empty or expression")
	}

	return cond, nil
}

func parseEqual(expr *chain.Cell) (Condition, error) {
	cond := &Equals{}

	if expr == nil {
		return nil, errors.New("missing arguments in eq")
	}

	if field, ok := expr.Car().(atomiser.Symbol); !ok {
		return nil, fmt.Errorf("expected field name, got '%#v' instead", expr.Car())
	} else {
		cond.Field = string(field)
	}

	expr = expr.Cdr()
	if expr == nil {
		return nil, fmt.Errorf("missing value in (eq %s) expression", cond.Field)
	}

	cond.Value = fmt.Sprintf("%v", expr.Car())

	return cond, nil
}

func parseId(expr *chain.Cell) (Condition, error) {
	if expr == nil {
		return nil, fmt.Errorf("missing ID value in (id) expression")
	}

	id := new(Id)

	if id_str, ok := expr.Car().(atomiser.Symbol); !ok {
		return nil, fmt.Errorf("expected ID, got '%#v' instead", expr.Car())
	} else {
		parsed_id, err := strconv.ParseInt(string(id_str), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("couldn't parse numeric ID in id expression")
		}
		*id = Id(parsed_id)
	}

	return id, nil
}
