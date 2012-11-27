package epos

type Result struct{}

func (r *Result) Next(result interface{}) bool {
	return false
}
