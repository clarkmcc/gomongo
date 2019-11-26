package aggregate

import (
	"github.com/clarkmcc/gomongo/condition"
	"github.com/clarkmcc/gomongo/util"
	"testing"
)

func TestPipe(t *testing.T) {
	pipeline := Pipe(
		Match(Operation(
			condition.Pipe(
				condition.ObjectIdMatch(condition.Condition{
					Key:   "_id",
					Value: "5c7836b73a8de34c78fec399"}),
				condition.EqualTo(condition.Condition{
					Key:   "status",
					Value: 1,
				}),
				condition.StringStartsWith(condition.Condition{
					Key:   "model",
					Value: "T654",
				}),
			),
		)),
		Project(Operation{
			"name":  1,
			"make":  1,
			"model": 1,
		}),
	)
	util.PrintJson(pipeline)
}
