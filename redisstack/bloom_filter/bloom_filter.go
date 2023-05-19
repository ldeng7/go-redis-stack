package redisstack

import (
	"context"

	"github.com/go-redis/redis/v9"
	"github.com/ldeng7/go-redis-stack/redisstack"
)

type Info struct {
	Capacity      int64
	Size          int64
	NumFilters    int64
	NumItems      int64
	ExpansionRate int64
}

type Option struct {
	ErrorRate     *float64
	Capacity      *int64
	ExpansionRate *int64
	NonScaling    bool
}

type ScanDump struct {
	Iter int64
	Data string
}

func AddArgs(key string, item string) []any {
	return []any{"BF.ADD", key, item}
}

func AddResult(val any) (bool, error) {
	return redisstack.ParseIntBool(val)
}

func ExistsArgs(key string, item string) []any {
	return []any{"BF.EXISTS", key, item}
}

func ExistsResult(val any) (bool, error) {
	return redisstack.ParseIntBool(val)
}

func InfoArgs(key string) []any {
	return []any{"BF.INFO", key}
}

func InfoResult(val any) (*Info, error) {
	arr, err := redisstack.ParseArray(val, 10)
	if err != nil {
		return nil, err
	}
	res := &Info{
		Capacity:      arr[1].(int64),
		Size:          arr[3].(int64),
		NumFilters:    arr[5].(int64),
		NumItems:      arr[7].(int64),
		ExpansionRate: arr[9].(int64),
	}
	return res, nil
}

func InsertArgs(key string, option *Option, noCreate bool, items []string) []any {
	args := make([]any, 0, 11+len(items))
	args = append(args, "BF.INSERT", key)
	if option != nil {
		if option.Capacity != nil {
			args = append(args, "CAPACITY", *option.Capacity)
		}
		if option.ErrorRate != nil {
			args = append(args, "ERROR", *option.ErrorRate)
		}
		if option.ExpansionRate != nil {
			args = append(args, "EXPANSION", *option.ExpansionRate)
		}
		if option.NonScaling {
			args = append(args, "NONSCALING")
		}
	}
	if noCreate {
		args = append(args, "NOCREATE")
	}
	args = append(args, "ITEMS")
	for _, item := range items {
		args = append(args, item)
	}
	return args
}

func InsertResult(val any) ([]bool, error) {
	return redisstack.ParseIntBoolArray(val, 0)
}

func LoadChunkArgs(key string, iter int64, data string) []any {
	return []any{"BF.LOADCHUNK", key, iter, data}
}

func MAddArgs(key string, items []string) []any {
	return redisstack.ArgsByKeyAndItems("BF.MADD", key, items)
}

func MAddResult(val any) ([]bool, error) {
	return redisstack.ParseIntBoolArray(val, 0)
}

func MExistsArgs(key string, items []string) []any {
	return redisstack.ArgsByKeyAndItems("BF.MEXISTS", key, items)
}

func MExistsResult(val any) ([]bool, error) {
	return redisstack.ParseIntBoolArray(val, 0)
}

func ReserveArgs(key string, option *Option) []any {
	args := make([]any, 0, 7)
	args = append(args, "BF.RESERVE", key, *option.ErrorRate, *option.Capacity)
	if option.ExpansionRate != nil {
		args = append(args, "EXPANSION", *option.ExpansionRate)
	}
	if option.NonScaling {
		args = append(args, "NONSCALING")
	}
	return args
}

func ScanDumpArgs(key string, iter int64) []any {
	return []any{"BF.SCANDUMP", key, iter}
}

func ScanDumpResult(val any) (*ScanDump, error) {
	arr, err := redisstack.ParseArray(val, 2)
	if err != nil {
		return nil, err
	}
	res := &ScanDump{}
	res.Iter, _ = arr[0].(int64)
	if res.Iter != 0 {
		res.Data, _ = arr[1].(string)
	}
	return res, nil
}

func DumpBatch(ctx context.Context, red *redis.Client, key string) ([]*ScanDump, error) {
	res := []*ScanDump{}
	for iter := int64(0); ; {
		cmd := red.Do(ctx, ScanDumpArgs(key, iter)...)
		if err := cmd.Err(); err != nil {
			return nil, err
		}
		res1, err := ScanDumpResult(cmd.Val())
		if err != nil {
			return nil, err
		} else if res1.Iter == 0 {
			break
		}
		res = append(res, res1)
	}
	return res, nil
}
