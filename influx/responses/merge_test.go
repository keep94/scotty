package responses

import (
	"encoding/json"
	. "github.com/smartystreets/goconvey/convey"
	"strconv"
	"testing"
)

func newPoint2(x int64, val float64) []interface{} {
	return []interface{}{
		json.Number(strconv.FormatInt(x, 10)),
		json.Number(strconv.FormatFloat(val, 'g', -1, 64))}
}

func TestQuotient(t *testing.T) {
	Convey("dividing", t, func() {
		s1 := [][]interface{}{
			newPoint2(13000, 92.6),
			newPoint2(13100, 38.4),
			newPoint2(13200, 46.5),
			newPoint2(13300, 66.0),
		}
		s2 := [][]interface{}{
			newPoint2(13100, 0.0),
			newPoint2(13200, 3.0),
			newPoint2(13300, 4.0),
			newPoint2(13400, 5.3),
		}
		quotient, err := piecewiseDivide(s1, s2)
		So(err, ShouldBeNil)
		So(quotient, ShouldResemble, [][]interface{}{
			newPoint2(13200, 15.5),
			newPoint2(13300, 16.5),
		})
	})
}

func TestSum(t *testing.T) {
	Convey("Summing serires", t, func() {
		s1 := [][]interface{}{
			newPoint2(13000, 92.4),
			newPoint2(13100, 38.9),
			newPoint2(13200, 45.5),
			newPoint2(13300, 62.1),
		}
		s2 := [][]interface{}{
			newPoint2(13200, 10.0),
			newPoint2(13300, 20.0),
			newPoint2(13400, 5.3),
		}
		s3 := [][]interface{}{
			newPoint2(13700, 8.9),
		}
		s4 := [][]interface{}{}
		sum, err := sumTogether(s1, s2, s3, s4)
		So(err, ShouldBeNil)
		So(sum, ShouldResemble, [][]interface{}{
			newPoint2(13000, 92.4),
			newPoint2(13100, 38.9),
			newPoint2(13200, 55.5),
			newPoint2(13300, 82.1),
			newPoint2(13400, 5.3),
			newPoint2(13700, 8.9),
		})
	})
}
