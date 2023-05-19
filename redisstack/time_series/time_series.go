package redisstack

import (
	"strconv"
	"time"

	"github.com/ldeng7/go-redis-stack/redisstack"
)

type DupPolicy byte

const (
	DupPolicyNone = DupPolicy(iota)
	DupPolicyBlock
	DupPolicyFirst
	DupPolicyLast
	DupPolicyMin
	DupPolicyMax
	DupPolicySum
)

var dupPolicyNames = map[DupPolicy]string{
	DupPolicyBlock: "BLOCK",
	DupPolicyFirst: "FIRST",
	DupPolicyLast:  "LAST",
	DupPolicyMin:   "MIN",
	DupPolicyMax:   "MAX",
	DupPolicySum:   "SUM",
}

type Option struct {
	Retention    *time.Duration
	Uncompressed bool
	ChunkSize    *int64
	DupPolicy    DupPolicy
	Labels       [][2]string
}

func (opt *Option) appendArgs(args []any, altering bool, dupPolicyTag string) []any {
	if opt == nil {
		return args
	}
	if opt.Retention != nil {
		args = append(args, "RETENTION", opt.Retention.Milliseconds())
	}
	if opt.Uncompressed {
		args = append(args, "ENCODING", "UNCOMPRESSED")
	}
	if opt.ChunkSize != nil {
		args = append(args, *opt.ChunkSize)
	}
	if opt.DupPolicy != DupPolicyNone {
		args = append(args, dupPolicyTag, dupPolicyNames[opt.DupPolicy])
	}
	if len(opt.Labels) > 0 || (altering && opt.Labels != nil) {
		args = append(args, "LABELS")
		for _, label := range opt.Labels {
			args = append(args, label[0], label[1])
		}
	}
	return args
}

type AggregateType byte

const (
	AggregateTypeNone = AggregateType(iota)
	AggregateTypeAvg
	AggregateTypeFirst
	AggregateTypeLast
	AggregateTypeMin
	AggregateTypeMax
	AggregateTypeSum
	AggregateTypeRange
	AggregateTypeCount
	AggregateTypeStdP
	AggregateTypeStdS
	AggregateTypeVarP
	AggregateTypeVarS
	AggregateTypeTWA
)

var aggregateTypeNames = map[AggregateType]string{
	AggregateTypeAvg:   "AVG",
	AggregateTypeFirst: "FIRST",
	AggregateTypeLast:  "LAST",
	AggregateTypeMin:   "MIN",
	AggregateTypeMax:   "MAX",
	AggregateTypeSum:   "SUM",
	AggregateTypeRange: "RANGE",
	AggregateTypeCount: "COUNT",
	AggregateTypeStdP:  "STD.P",
	AggregateTypeStdS:  "STD.S",
	AggregateTypeVarP:  "VAR.P",
	AggregateTypeVarS:  "VAR.S",
	AggregateTypeTWA:   "TWA",
}

type Sample struct {
	Key   string
	Time  *time.Time
	Value float64
}

type MultiSample struct {
	Labels  [][2]string
	Samples []*Sample
}

type MultiQueryAggregation struct {
	Align           string
	Aggregator      AggregateType
	BucketDuration  time.Duration
	BucketTimestamp string
	Empty           bool
}

func (agg *MultiQueryAggregation) appendArgs(args []any) []any {
	if agg == nil {
		return args
	}
	if len(agg.Align) > 0 {
		args = append(args, "ALIGN", agg.Align)
	}
	args = append(args, "AGGREGATION", aggregateTypeNames[agg.Aggregator], agg.BucketDuration.Milliseconds())
	if len(agg.BucketTimestamp) > 0 {
		args = append(args, "BUCKETTIMESTAMP", agg.BucketTimestamp)
	}
	if agg.Empty {
		args = append(args, "EMPTY")
	}
	return args
}

type MultiQuery struct {
	FromTime         *time.Time
	ToTime           *time.Time
	Latest           bool
	FiltersByTime    []*time.Time
	FilterByValueMin *float64
	FilterByValueMax *float64
	WithLabels       bool
	SelectedLabels   []string
	Count            *int
	Aggregation      *MultiQueryAggregation
	Filters          []string
	GroupByLabel     string
	Reducer          AggregateType
}

func (q *MultiQuery) appendFiltersByTime(args []any) []any {
	if len(q.FiltersByTime) > 0 {
		args = append(args, "FILTER_BY_TS")
		for _, t := range q.FiltersByTime {
			args = append(args, t.UnixMilli())
		}
	}
	return args
}

func (q *MultiQuery) appendFilterByValue(args []any) []any {
	if q.FilterByValueMin != nil && q.FilterByValueMax != nil {
		args = append(args, "FILTER_BY_VALUE", *q.FilterByValueMin, *q.FilterByValueMax)
	}
	return args
}

func (q *MultiQuery) appendLabelSelection(args []any) []any {
	if q.WithLabels {
		args = append(args, "WITHLABELS")
	} else if len(q.SelectedLabels) > 0 {
		args = append(args, "SELECTED_LABELS")
		for _, label := range q.SelectedLabels {
			args = append(args, label)
		}
	}
	return args
}

func (q *MultiQuery) appendFilters(args []any) []any {
	args = append(args, "FILTER")
	for _, filter := range q.Filters {
		args = append(args, filter)
	}
	return args
}

func (q *MultiQuery) appendGroupByLabel(args []any) []any {
	if len(q.GroupByLabel) > 0 && q.Reducer != AggregateTypeNone {
		args = append(args, "GROUPBY", q.GroupByLabel, "REDUCE", aggregateTypeNames[q.Reducer])
	}
	return args
}

func AddArgs(sample *Sample, option *Option) []any {
	args := make([]any, 0, 13+len(option.Labels)*2)
	args = append(args, "TS.ADD", sample.Key, sample.Time.UnixMilli(), sample.Value)
	return option.appendArgs(args, false, "ON_DUPLICATE")
}

func AlterArgs(key string, option *Option) []any {
	args := make([]any, 0, 11+len(option.Labels)*2)
	args = append(args, "TS.ALTER", key)
	return option.appendArgs(args, true, "DUPLICATE_POLICY")
}

func CreateArgs(key string, option *Option) []any {
	args := make([]any, 0, 11+len(option.Labels)*2)
	args = append(args, "TS.CREATE", key)
	return option.appendArgs(args, false, "DUPLICATE_POLICY")
}

func CreateRuleArgs(srcKey string, destKey string, aggregateType AggregateType, alignTime *time.Duration) []any {
	args := make([]any, 0, 7)
	args = append(args, "TS.CREATERULE", srcKey, destKey, "AGGREGATION", aggregateTypeNames[aggregateType])
	if alignTime != nil {
		args = append(args, alignTime.Milliseconds())
	}
	return args
}

//TODO: TS.DECRBY

func DelArgs(key string, fromTime *time.Time, toTime *time.Time) []any {
	return []any{"TS.DEL", key, fromTime.UnixMilli(), toTime.UnixMilli()}
}

func DelResult(val any) (int64, error) {
	return redisstack.ParseScalar[int64](val)
}

func DeleteRuleArgs(srcKey string, destKey string) []any {
	return []any{"TS.DELETERULE", srcKey, destKey}
}

func GetArgs(key string) []any {
	return []any{"TS.GET", key}
}

func GetResult(val any) (*Sample, error) {
	arr, err := redisstack.ParseArray(val, 2)
	if err != nil {
		return nil, err
	}
	mt, _ := arr[0].(int64)
	t := time.UnixMilli(mt)
	v, _ := arr[1].(string)
	f, _ := strconv.ParseFloat(v, 64)
	s := &Sample{
		Time:  &t,
		Value: f,
	}
	return s, nil
}

//TODO: TS.INCRBY
//TODO: TS.INFO

func MAddArgs(samples []*Sample) []any {
	args := make([]any, 1+len(samples)*3)
	args[0] = "TS.MADD"
	for i, sample := range samples {
		args[i*3+1], args[i*3+2], args[i*3+3] = sample.Key, sample.Time.UnixMilli(), sample.Value
	}
	return args
}

func MAddResult(val any) ([]*time.Time, error) {
	return redisstack.ParseToMappedArray(val, 0, func(e any) (*time.Time, error) {
		if e1, ok := e.(int64); ok {
			t := time.UnixMilli(e1)
			return &t, nil
		}
		return nil, nil
	})
}

func MGetArgs(q *MultiQuery) []any {
	args := make([]any, 0, 4+len(q.SelectedLabels)+len(q.Filters))
	args = append(args, "TS.MGET")
	if q.Latest {
		args = append(args, "LATEST")
	}
	args = q.appendLabelSelection(args)
	args = q.appendFilters(args)
	return args
}

func MGetResult(val any) (map[string]*MultiSample, error) {
	arr, err := redisstack.ParseArray(val, 0)
	if err != nil {
		return nil, err
	}
	res := make(map[string]*MultiSample, len(arr))
	for _, e := range arr {
		arr1, err := redisstack.ParseArray(e, 3)
		if err != nil {
			return nil, err
		}
		key, _ := arr1[0].(string)

		labels, err := redisstack.ParseStringPairArray(arr1[1], 0)
		if err != nil {
			return nil, err
		}
		sample, err := GetResult(arr1[2])
		if err != nil {
			return nil, err
		}
		res[key] = &MultiSample{Labels: labels, Samples: []*Sample{sample}}
	}
	return res, nil
}

func MRangeArgs(q *MultiQuery) []any {
	args := make([]any, 0, 24+len(q.FiltersByTime)+len(q.SelectedLabels)+len(q.Filters))
	args = append(args, "TS.MRANGE")
	args = append(args, q.FromTime.UnixMilli(), q.ToTime.UnixMilli())
	if q.Latest {
		args = append(args, "LATEST")
	}
	args = q.appendFiltersByTime(args)
	args = q.appendFilterByValue(args)
	args = q.appendLabelSelection(args)
	if q.Count != nil {
		args = append(args, "COUNT", *q.Count)
	}
	args = q.Aggregation.appendArgs(args)
	args = q.appendFilters(args)
	args = q.appendGroupByLabel(args)
	return args
}

func MRangeResult(val any) (map[string]*MultiSample, error) {
	arr, err := redisstack.ParseArray(val, 0)
	if err != nil {
		return nil, err
	}
	res := make(map[string]*MultiSample, len(arr))
	for _, e := range arr {
		arr1, err := redisstack.ParseArray(e, 3)
		if err != nil {
			return nil, err
		}
		key, _ := arr1[0].(string)

		labels, err := redisstack.ParseStringPairArray(arr1[1], 0)
		if err != nil {
			return nil, err
		}

		samples, err := redisstack.ParseToMappedArray(arr1[2], 0, GetResult)
		if err != nil {
			return nil, err
		}

		res[key] = &MultiSample{Labels: labels, Samples: samples}
	}
	return res, nil
}

func MRevRangeArgs(q *MultiQuery) []any {
	args := MRangeArgs(q)
	args[0] = "TS.MREVRANGE"
	return args
}

func MRevRangeResult(val any) (map[string]*MultiSample, error) {
	return MRangeResult(val)
}

func QueryIndexArgs(filters []string) []any {
	args := make([]any, 1+len(filters))
	args[0] = "TS.QUERYINDEX"
	for i, filter := range filters {
		args[i+1] = filter
	}
	return args
}

func QueryIndexResult(val any) ([]string, error) {
	return redisstack.ParseScalarArray[string](val, 0)
}

func RangeArgs(key string, q *MultiQuery) []any {
	args := make([]any, 0, 19+len(q.FiltersByTime))
	args = append(args, "TS.RANGE", key)
	args = append(args, q.FromTime.UnixMilli(), q.ToTime.UnixMilli())
	if q.Latest {
		args = append(args, "LATEST")
	}
	args = q.appendFiltersByTime(args)
	args = q.appendFilterByValue(args)
	if q.Count != nil {
		args = append(args, "COUNT", *q.Count)
	}
	args = q.Aggregation.appendArgs(args)
	return args
}

func RangeResult(val any) ([]*Sample, error) {
	return redisstack.ParseToMappedArray(val, 0, GetResult)
}

func RevRangeArgs(key string, q *MultiQuery) []any {
	args := RangeArgs(key, q)
	args[0] = "TS.REVRANGE"
	return args
}

func RevRangeResult(val any) ([]*Sample, error) {
	return RangeResult(val)
}
