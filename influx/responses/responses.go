package responses

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/Symantec/scotty/tsdb"
	"github.com/Symantec/scotty/tsdbjson"
	"github.com/influxdata/influxdb/client/v2"
	"github.com/influxdata/influxdb/models"
	"reflect"
	"sort"
	"strconv"
)

const (
	kInfluxHostName = "host"
	kInfluxAppName  = "appname"
)

var (
	errPartialNotSupported = errors.New("Partial rows not supported")
)

type pointFactoryType func(ts int64) int64

func (p pointFactoryType) New(
	tsInSeconds int64, value interface{}) []interface{} {
	return []interface{}{p(tsInSeconds), value}
}

func extractDownsampledValues(
	values tsdb.TimeSeries,
	start float64,
	end float64,
	downSample float64,
	pf pointFactoryType) (results [][]interface{}) {
	startInt := int64(start)
	endInt := int64(end)
	downSampleInt := int64(downSample)
	startInt = (startInt / downSampleInt) * downSampleInt
	endInt = ((endInt + downSampleInt - 1) / downSampleInt) * downSampleInt
	srcIdx, destIdx := 0, 0
	destTs := startInt
	srcLen := len(values)
	destLen := int((endInt - startInt) / downSampleInt)
	results = make([][]interface{}, destLen)
	for destIdx < destLen {
		// If our source values are exhaused, use nil in destination
		if srcIdx >= srcLen {
			results[destIdx] = pf.New(destTs, nil)
			destIdx++
			destTs += downSampleInt
			continue
		}
		srcTs := int64(values[srcIdx].Ts)
		// If our source timestamp is less than our dest timestamp, advance
		// source one position
		if srcTs < destTs {
			srcIdx++
			continue
		}
		// If our source timestamp is bigger than our dest timestamp, use
		// nil in destination
		if srcTs > destTs {
			results[destIdx] = pf.New(destTs, nil)
			destIdx++
			destTs += downSampleInt
			continue
		}
		// If our source and dest timestamp are equal, use the corresponding
		// source value in the destination
		results[destIdx] = pf.New(destTs, values[srcIdx].Value)
		destIdx++
		destTs += downSampleInt
		srcIdx++
	}
	return
}

func fromTaggedTimeSeriesSet(
	seriesSet *tsdb.TaggedTimeSeriesSet,
	colNames []string,
	pq *tsdbjson.ParsedQuery,
	pf pointFactoryType) (result client.Result) {
	// Defensive copy of colNames because it becomes part of returned data
	// structure.
	colNamesCopy := make([]string, len(colNames))
	copy(colNamesCopy, colNames)

	name := seriesSet.MetricName
	result.Series = make([]models.Row, len(seriesSet.Data))
	for i, series := range seriesSet.Data {
		tags := make(map[string]string)
		if seriesSet.GroupedByHostName {
			tags[kInfluxHostName] = series.Tags.HostName
		}
		if seriesSet.GroupedByAppName {
			tags[kInfluxAppName] = series.Tags.AppName
		}
		downSample := pq.Aggregator.DownSample
		var values [][]interface{}
		if downSample != nil {
			values = extractDownsampledValues(
				series.Values,
				pq.Start,
				pq.End,
				downSample.DurationInSeconds,
				pf)
		} else {
			values := make([][]interface{}, len(series.Values))
			for j, tsValue := range series.Values {
				values[j] = pf.New(int64(tsValue.Ts), tsValue.Value)
			}
		}
		result.Series[i] = models.Row{
			Name:    name,
			Tags:    tags,
			Columns: colNamesCopy,
			Values:  values}
	}
	sort.Sort(rowListType(result.Series))
	return
}

func fromTaggedTimeSeriesSets(
	seriesList []*tsdb.TaggedTimeSeriesSet,
	colNames [][]string,
	pqs []tsdbjson.ParsedQuery,
	epochConversion func(ts int64) int64) *client.Response {
	var results []client.Result
	for i, series := range seriesList {
		if series == nil {
			results = append(results, client.Result{})
			continue
		}
		results = append(
			results, fromTaggedTimeSeriesSet(
				series, colNames[i], &pqs[i], epochConversion))
	}
	return &client.Response{Results: results}
}

func customMergeResponses(
	responses []*client.Response,
	mergeResultsFunc func([]client.Result) (client.Result, error)) (
	*client.Response, error) {
	if len(responses) == 0 {
		return &client.Response{}, nil
	}
	// Check for any errors
	for _, response := range responses {
		if response.Error() != nil {
			return &client.Response{Err: response.Error().Error()}, nil
		}
	}

	// Insist that all responses have the same number of results.
	resultLen := len(responses[0].Results)
	for _, response := range responses[1:] {
		if len(response.Results) != resultLen {
			return nil, errors.New(
				"Responses should have equal number of results")
		}
	}

	// Merge the results piecewise
	mergedResultList := make([]client.Result, resultLen)
	for i := range mergedResultList {
		resultsToMerge := make([]client.Result, len(responses))
		for j := range resultsToMerge {
			resultsToMerge[j] = responses[j].Results[i]
		}
		var err error
		mergedResultList[i], err = mergeResultsFunc(resultsToMerge)
		if err != nil {
			return nil, err
		}
	}
	return &client.Response{Results: mergedResultList}, nil
}

func mergeResponses(
	responses []*client.Response) (*client.Response, error) {
	return customMergeResponses(responses, mergeResults)
}

func mergePreferred(
	response, preferred *client.Response) (*client.Response, error) {
	return customMergeResponses(
		[]*client.Response{response, preferred},
		mergePreferredResults)
}

func mergePreferredResults(results []client.Result) (
	merged client.Result, err error) {
	var mergedMessages []*client.Message
	original := toRowPtrList(results[0].Series)
	preferred := toRowPtrList(results[1].Series)
	for _, result := range results {
		mergedMessages = append(mergedMessages, result.Messages...)
	}
	sort.Sort(original)
	sort.Sort(preferred)
	var mergedRows []models.Row
	for original.Peek() != nil || preferred.Peek() != nil {
		if original.Peek() == nil {
			mergedRows = append(mergedRows, *preferred.Next())
		} else if preferred.Peek() == nil {
			mergedRows = append(mergedRows, *original.Next())
		} else {
			compRes := compareRows(original.Peek(), preferred.Peek())
			if compRes < 0 {
				mergedRows = append(mergedRows, *original.Next())
			} else if compRes > 0 {
				mergedRows = append(mergedRows, *preferred.Next())
			} else {
				// rows in original and preferred are same
				// they must be merged
				orig := original.Next()
				aRow, err := mergePreferredRows(
					orig, preferred.Next())
				if err != nil {
					// If anything goes wrong, use original
					aRow = *orig
				}
				mergedRows = append(mergedRows, aRow)
			}
		}
	}
	return client.Result{Series: mergedRows, Messages: mergedMessages}, nil
}

func mergeResults(results []client.Result) (merged client.Result, err error) {
	var mergedMessages []*client.Message
	var rowPtrs rowPtrListType
	for _, result := range results {
		mergedMessages = append(mergedMessages, result.Messages...)
		rowPtrs = append(rowPtrs, toRowPtrList(result.Series)...)
	}
	// We need to preserve order of rows because we want to prefer values
	// from rows from the last result
	sort.Stable(rowPtrs)
	var mergedRows []models.Row

	// Next we have to reduce multiple row instances for the same time series
	// down into one.
	mergedRows, err = reduceRows(rowPtrs)
	if err != nil {
		return
	}
	return client.Result{Series: mergedRows, Messages: mergedMessages}, nil
}

func checkColumnsMatch(row1, row2 *models.Row) error {
	if !reflect.DeepEqual(
		row1.Columns, row2.Columns) {
		return fmt.Errorf(
			"Columns don't match for Name: %v, Tags: %v",
			row1.Name, row1.Tags)
	}
	return nil
}

func toInt64(val interface{}) int64 {
	jsonNumber, ok := val.(json.Number)
	if !ok {
		return 0
	}
	result, _ := jsonNumber.Int64()
	return result
}

func isZero(val interface{}) bool {
	jsonNumber, ok := val.(json.Number)
	// Then its a string or some other complex type
	if !ok {
		return false
	}
	floatValue, _ := jsonNumber.Float64()
	return floatValue == 0.0
}

func mergePreferredRows(original, preferred *models.Row) (models.Row, error) {
	if original.Partial || preferred.Partial {
		return models.Row{}, errPartialNotSupported
	}
	if err := checkColumnsMatch(original, preferred); err != nil {
		return models.Row{}, err
	}
	originalCopy := *original
	// If not two columsn give up
	if len(originalCopy.Columns) != 2 {
		return models.Row{}, errors.New("Rows have more than 2 columns")
	}
	timeIndex := indexByName(originalCopy.Columns, "time")
	// If no time column give up
	if timeIndex == -1 {
		return models.Row{}, errors.New("No time column")
	}
	// sort values of original by time
	originalCopy.Values = nil
	originalCopy.Values = append(
		originalCopy.Values, original.Values...)
	if err := sortByTime(originalCopy.Values, timeIndex); err != nil {
		return models.Row{}, err
	}

	// Sort values of preferred by time storing in preferredValues
	var preferredValues [][]interface{}
	preferredValues = append(
		preferredValues, preferred.Values...)
	if err := sortByTime(preferredValues, timeIndex); err != nil {
		return models.Row{}, err
	}
	valueIndex := 1 - timeIndex
	var startOfPreferred = -1
	for i := range preferredValues {
		if preferredValues[i][valueIndex] != nil && !isZero(
			preferredValues[i][valueIndex]) {
			startOfPreferred = i
			break
		}
	}
	preferredStartTimeAsNumber := toInt64(preferredValues[startOfPreferred][timeIndex])
	endOfOriginal := sort.Search(len(originalCopy.Values),
		func(i int) bool {
			return toInt64(originalCopy.Values[i][timeIndex]) >= preferredStartTimeAsNumber
		})
	originalCopy.Values = originalCopy.Values[:endOfOriginal]
	originalCopy.Values = append(
		originalCopy.Values, preferredValues[startOfPreferred:]...)

	return originalCopy, nil
}

func reduceRows(rows rowPtrListType) (reduced []models.Row, err error) {
	currentRow := rows.Next()
	for currentRow != nil {
		if currentRow.Partial {
			return nil, errPartialNotSupported
		}
		currentRowCopy := *currentRow
		currentRowCopy.Values = nil
		currentRowCopy.Values = append(
			currentRowCopy.Values, currentRow.Values...)
		nextRow := rows.Next()
		for nextRow != nil && compareRows(currentRow, nextRow) == 0 {
			if nextRow.Partial {
				return nil, errPartialNotSupported
			}
			if err := checkColumnsMatch(currentRow, nextRow); err != nil {
				return nil, err
			}
			currentRowCopy.Values = append(
				currentRowCopy.Values, nextRow.Values...)
			nextRow = rows.Next()
		}
		timeIdx := indexByName(currentRowCopy.Columns, "time")
		if timeIdx != -1 {
			if err := sortByTimeAndUniquify(&currentRowCopy.Values, timeIdx); err != nil {
				return nil, err
			}
		}
		reduced = append(reduced, currentRowCopy)
		currentRow = nextRow
	}
	return
}

func indexByName(names []string, nameToFind string) int {
	for i, name := range names {
		if name == nameToFind {
			return i
		}
	}
	return -1
}

type sortTimeSeriesType struct {
	valuesToSort [][]interface{}
	times        []int64
}

func newSortTimeSeriesType(
	valuesToSort [][]interface{}, timeIdx int) (
	result *sortTimeSeriesType, err error) {
	times := make([]int64, len(valuesToSort))
	for i := range times {
		num, ok := valuesToSort[i][timeIdx].(json.Number)
		if !ok {
			err = fmt.Errorf(
				"Time wrong format %v",
				valuesToSort[i][timeIdx])
			return
		}
		times[i], err = num.Int64()
		if err != nil {
			return
		}
	}
	return &sortTimeSeriesType{valuesToSort: valuesToSort, times: times}, nil
}

func (s *sortTimeSeriesType) Len() int {
	return len(s.valuesToSort)
}

func (s *sortTimeSeriesType) Swap(i, j int) {
	s.valuesToSort[i], s.valuesToSort[j] = s.valuesToSort[j], s.valuesToSort[i]
	s.times[i], s.times[j] = s.times[j], s.times[i]
}

func (s *sortTimeSeriesType) Less(i, j int) bool {
	return s.times[i] < s.times[j]
}

func (s *sortTimeSeriesType) uniquify() [][]interface{} {
	if len(s.times) == 0 {
		return s.valuesToSort
	}
	idx := 0
	tlen := len(s.times)
	for i := 1; i < tlen; i++ {
		if s.times[i] != s.times[idx] {
			idx++
		}
		s.valuesToSort[idx] = s.valuesToSort[i]
		s.times[idx] = s.times[i]
	}
	s.times = s.times[:idx+1]
	s.valuesToSort = s.valuesToSort[:idx+1]
	return s.valuesToSort
}

func sortByTimeAndUniquify(values *[][]interface{}, timeIdx int) error {
	s, err := newSortTimeSeriesType(*values, timeIdx)
	if err != nil {
		return err
	}
	sort.Stable(s)
	*values = s.uniquify()
	return nil
}

func sortByTime(values [][]interface{}, timeIdx int) error {
	s, err := newSortTimeSeriesType(values, timeIdx)
	if err != nil {
		return err
	}
	sort.Sort(s)
	return nil
}

type rowListType []models.Row

func (l rowListType) Len() int { return len(l) }

func (l rowListType) Swap(i, j int) {
	l[i], l[j] = l[j], l[i]
}

func (l rowListType) Less(i, j int) bool {
	return compareRows(&l[i], &l[j]) < 0
}

type rowPtrListType []*models.Row

func toRowPtrList(rows []models.Row) rowPtrListType {
	result := make(rowPtrListType, len(rows))
	for i := range rows {
		result[i] = &rows[i]
	}
	return result
}

func (l rowPtrListType) Len() int { return len(l) }

func (l rowPtrListType) Swap(i, j int) {
	l[i], l[j] = l[j], l[i]
}

func (l rowPtrListType) Less(i, j int) bool {
	return compareRows(l[i], l[j]) < 0
}

func (l *rowPtrListType) Next() *models.Row {
	if len(*l) == 0 {
		return nil
	}
	result := (*l)[0]
	*l = (*l)[1:]
	return result
}

func (l rowPtrListType) Peek() *models.Row {
	if len(l) == 0 {
		return nil
	}
	return l[0]
}

func compareRows(lhs, rhs *models.Row) int {
	if lhs.Name < rhs.Name {
		return -1
	}
	if lhs.Name > rhs.Name {
		return 1
	}
	return compareTags(lhs.Tags, rhs.Tags)
}

func compareTags(lhs, rhs map[string]string) int {
	lkeys := tagKeys(lhs)
	rkeys := tagKeys(rhs)
	return compareTagsByKeys(lkeys, rkeys, lhs, rhs)
}

func compareTagsByKeys(lkeys, rkeys []string, lhs, rhs map[string]string) int {
	llength := len(lkeys)
	rlength := len(rkeys)
	for i := 0; i < llength && i < rlength; i++ {
		if lkeys[i] < rkeys[i] {
			return -1
		}
		if lkeys[i] > rkeys[i] {
			return 1
		}
		lval, rval := lhs[lkeys[i]], rhs[rkeys[i]]
		if lval < rval {
			return -1
		}
		if lval > rval {
			return 1
		}
	}
	if llength < rlength {
		return -1
	}
	if llength > rlength {
		return 1
	}
	return 0
}

func tagKeys(m map[string]string) (result []string) {
	for k := range m {
		result = append(result, k)
	}
	sort.Strings(result)
	return
}

func piecewiseDivide(lhs, rhs [][]interface{}) (
	[][]interface{}, error) {
	var result [][]interface{}
	lindex, rindex := 0, 0
	llen, rlen := len(lhs), len(rhs)
	for lindex < llen && rindex < rlen {
		lts := toInt64(lhs[lindex][0])
		rts := toInt64(rhs[rindex][0])
		if lts < rts {
			lindex++
		} else if lts > rts {
			rindex++
		} else { // timestamps are equal, divide
			lvalue, ok := lhs[lindex][1].(json.Number)
			if !ok {
				return nil, fmt.Errorf(
					"Time wrong format %v", lhs[lindex][0])
			}
			rvalue, ok := rhs[rindex][1].(json.Number)
			if !ok {
				return nil, fmt.Errorf(
					"Time wrong format %v", rhs[rindex][0])
			}
			lv, err := lvalue.Float64()
			if err != nil {
				return nil, err
			}
			rv, err := rvalue.Float64()
			if err != nil {
				return nil, err
			}
			if rv != 0.0 {
				quotient := lv / rv
				quotientvalue := json.Number(
					strconv.FormatFloat(quotient, 'g', -1, 64))
				result = append(
					result, []interface{}{lhs[lindex][0], quotientvalue})
			}
			lindex++
			rindex++
		}
	}
	return result, nil
}

// sumTogether sums up timeSeriesList into a single time series.
// Values falling on the same timestamp get added together when merged.
// [x][0] is always timestamp; [x][1] is always value of the xth row of
// time series
func sumTogether(timeSeriesList ...[][]interface{}) ([][]interface{}, error) {
	// TODO: write this better
	var result [][]interface{}
	for _, timeSeries := range timeSeriesList {
		var err error
		result, err = sumTogether2(result, timeSeries)
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

func sumTogether2(lhs, rhs [][]interface{}) ([][]interface{}, error) {
	var result [][]interface{}
	lindex, rindex := 0, 0
	llen, rlen := len(lhs), len(rhs)
	for lindex < llen && rindex < rlen {
		lts := toInt64(lhs[lindex][0])
		rts := toInt64(rhs[rindex][0])
		if lts < rts {
			result = append(result, lhs[lindex])
			lindex++
		} else if lts > rts {
			result = append(result, rhs[rindex])
			rindex++
		} else { // timestamps are equal, add them
			lvalue, ok := lhs[lindex][1].(json.Number)
			if !ok {
				return nil, fmt.Errorf(
					"Time wrong format %v", lhs[lindex][0])
			}
			rvalue, ok := rhs[rindex][1].(json.Number)
			if !ok {
				return nil, fmt.Errorf(
					"Time wrong format %v", rhs[rindex][0])
			}
			lv, err := lvalue.Float64()
			if err != nil {
				return nil, err
			}
			rv, err := rvalue.Float64()
			if err != nil {
				return nil, err
			}
			sum := lv + rv
			sumvalue := json.Number(strconv.FormatFloat(sum, 'g', -1, 64))
			result = append(result, []interface{}{lhs[lindex][0], sumvalue})
			lindex++
			rindex++
		}
	}
	if lindex < llen {
		result = append(result, lhs[lindex:]...)
	} else {
		result = append(result, rhs[rindex:]...)
	}
	return result, nil
}
