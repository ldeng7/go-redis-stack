package top_k

import "github.com/ldeng7/go-redis-stack/redisstack"

type Info struct {
	K     int64
	Width int64
	Depth int64
	Decay float64
}

func AddArgs(key string, items []string) []any {
	return redisstack.ArgsByKeyAndItems("TOPK.ADD", key, items)
}

func AddResult(val any) ([]*string, error) {
	return redisstack.ParseNullableScalarArray[string](val, 0)
}

func CountArgs(key string, items []string) []any {
	return redisstack.ArgsByKeyAndItems("TOPK.COUNT", key, items)
}

func CountResult(val any) ([]int64, error) {
	return redisstack.ParseScalarArray[int64](val, 0)
}

func IncrByArgs(key string, itemAmounts []redisstack.ItemAmount) []any {
	return redisstack.ArgsByKeyAndItemAmounts("TOPK.INCRBY", key, itemAmounts)
}

func IncrByResult(val any) ([]*string, error) {
	return redisstack.ParseNullableScalarArray[string](val, 0)
}

func InfoArgs(key string) []any {
	return []any{"TOPK.INFO", key}
}

func InfoResult(val any) (*Info, error) {
	arr, err := redisstack.ParseArray(val, 8)
	if err != nil {
		return nil, err
	}
	res := &Info{
		K:     arr[1].(int64),
		Width: arr[3].(int64),
		Depth: arr[5].(int64),
		Decay: arr[7].(float64),
	}
	return res, nil
}

func ListArgs(key string) []any {
	return []any{"TOPK.LIST", key}
}

func ListResult(val any) ([]string, error) {
	return redisstack.ParseScalarArray[string](val, 0)
}

func ListWithCountArgs(key string) []any {
	return []any{"TOPK.LIST", key, "WITHCOUNT"}
}

func ListWithCountResult(val any) ([]redisstack.ItemAmount, error) {
	return redisstack.ParseItemAmountInterlacedArray(val, 0)
}

func QueryArgs(key string, items []string) []any {
	return redisstack.ArgsByKeyAndItems("TOPK.QUERY", key, items)
}

func QueryResult(val any) ([]bool, error) {
	return redisstack.ParseIntBoolArray(val, 0)
}

func ReserveArgs(key string, topK int64, info *Info) []any {
	args := make([]any, 0, 6)
	args = append(args, "TOPK.RESERVE", key, topK)
	if info != nil {
		args = append(args, info.Width, info.Depth, info.Decay)
	}
	return args
}
