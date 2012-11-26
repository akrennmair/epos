package epos

type Condition interface {
	Matches(data map[string]interface{}) bool
}

type And []Condition

func (c *And) Matches(data map[string]interface{}) bool {
	for _, cond := range *c {
		if !cond.Matches(data) {
			return false
		}
	}
	return true
}

type Or []Condition

func (c *Or) Matches(data map[string]interface{}) bool {
	for _, cond := range *c {
		if cond.Matches(data) {
			return true
		}
	}
	return false
}

type Equals struct {
	Field string
	Value interface{}
}

func (c *Equals) Matches(data map[string]interface{}) bool {
	return data[c.Field] == c.Value // TODO: do type checks etc.
}
