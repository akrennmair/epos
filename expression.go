package epos

import (
	"errors"
	"fmt"
	"github.com/feyeleanor/atomiser"
	"github.com/feyeleanor/chain"
	"strconv"
	"strings"
)

func Expression(s string) (Condition, error) {
	expr := atomiser.NewAtomiser(strings.NewReader(s)).ReadList()

	if expr == nil {
		return nil, fmt.Errorf("parsing '%s' failed", s)
	}

	return parseExpressionToCondition(expr)
}


func parseExpressionToCondition(expr *chain.Cell) (Condition, error) {
	sym, ok := expr.Head.(atomiser.Symbol)
	if !ok {
		return nil, fmt.Errorf("expected symbol, got '%v' instead", expr.Head)
	}

	sym = atomiser.Symbol(strings.ToLower(string(sym)))

	switch (sym) {
	case "and":
		return parseAnd(expr.Tail)
	case "or":
		return parseOr(expr.Tail)
	case "eq":
		return parseEqual(expr.Tail)
	case "id":
		return parseId(expr.Tail)
	}
	return nil, fmt.Errorf("unknown symbol '%s'", sym)
}

func parseAnd(expr *chain.Cell) (Condition, error) {
	cond := &And{}

	cur := expr
	for cur != nil {
		if subexpr, ok := cur.Head.(*chain.Cell); ok {
			if subcond, err := parseExpressionToCondition(subexpr); err != nil {
				return nil, err
			} else {
				*cond = append(*cond, subcond)
			}
		}
		cur = cur.Tail
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
		if subexpr, ok := cur.Head.(*chain.Cell); ok {
			if subcond, err := parseExpressionToCondition(subexpr); err != nil {
				return nil, err
			} else {
				*cond = append(*cond, subcond)
			}
		}
		cur = cur.Tail
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

	if field, ok := expr.Head.(atomiser.Symbol); !ok {
		return nil, fmt.Errorf("expected field name, got '%#v' instead", expr.Head)
	} else {
		cond.Field = string(field)
	}

	expr = expr.Tail
	if expr == nil {
		return  nil, fmt.Errorf("missing value in (eq %s) expression", cond.Field)
	}

	cond.Value = fmt.Sprintf("%v", expr.Head)

	return cond, nil
}

func parseId(expr *chain.Cell) (Condition, error) {
	if expr == nil {
		return nil, fmt.Errorf("missing ID value in (id) expression")
	}

	id := new(Id)

	if id_str, ok := expr.Head.(atomiser.Symbol); !ok {
		return nil, fmt.Errorf("expected ID, got '%#v' instead", expr.Head)
	} else {
		parsed_id, err := strconv.ParseInt(string(id_str), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("couldn't parse numeric ID in id expression")
		}
		*id = Id(parsed_id)
	}

	return id, nil
}
