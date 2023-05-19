package redisstack

import "github.com/ldeng7/go-redis-stack/redisstack"

type Info struct {
	Width int64
	Depth int64
	Count int64
}

func IncrByArgs(key string, itemAmounts []redisstack.ItemAmount) []any {
	return redisstack.ArgsByKeyAndItemAmounts("CMS.INCRBY", key, itemAmounts)
}

func IncrByResult(val any) ([]int64, error) {
	return redisstack.ParseScalarArray[int64](val, 0)
}

func InfoArgs(key string) []any {
	return []any{"CMS.INFO", key}
}

func InfoResult(val any) (*Info, error) {
	arr, err := redisstack.ParseArray(val, 6)
	if err != nil {
		return nil, err
	}
	res := &Info{
		Width: arr[1].(int64),
		Depth: arr[3].(int64),
		Count: arr[5].(int64),
	}
	return res, nil
}

func InitByDimArgs(key string, width int64, depth int64) []any {
	return []any{"CMS.INITBYDIM", key, width, depth}
}

func InitByProbArgs(key string, errorRate float64, probability float64) []any {
	return []any{"CMS.INITBYPROB", key, errorRate, probability}
}

func MergeArgs(destKey string, srcKeys []string, weights []int64) []any {
	args := make([]any, 0, 4+len(srcKeys)+len(weights))
	args = append(args, "CMS.MERGE", destKey, len(srcKeys))
	for _, srcKey := range srcKeys {
		args = append(args, srcKey)
	}
	if len(weights) > 0 {
		args = append(args, "WEIGHTS")
		for _, weight := range weights {
			args = append(args, weight)
		}
	}
	return args
}

func QueryArgs(key string, items []string) []any {
	return redisstack.ArgsByKeyAndItems("CMS.QUERY", key, items)
}

func QueryResult(val any) ([]int64, error) {
	return redisstack.ParseScalarArray[int64](val, 0)
}
