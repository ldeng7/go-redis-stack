package redisstack

import (
	"context"

	"github.com/go-redis/redis/v9"
	"github.com/ldeng7/go-redis-stack/redisstack"
)

type Info struct {
	Size             int64
	NumBuckets       int64
	NumFilters       int64
	NumItemsInserted int64
	NumItemsDeleted  int64
	BucketSize       int64
	ExpansionRate    int64
	MaxIteration     int64
}

type ScanDump struct {
	Iter int64
	Data string
}

func AddArgs(key string, item string) []any {
	return []any{"CF.ADD", key, item}
}

func AddNXArgs(key string, item string) []any {
	return []any{"CF.ADDNX", key, item}
}

func AddNXResult(val any) (bool, error) {
	return redisstack.ParseIntBool(val)
}

func CountArgs(key string, item string) []any {
	return []any{"CF.COUNT", key, item}
}

func CountResult(val any) (int64, error) {
	return redisstack.ParseScalar[int64](val)
}

func DelArgs(key string, item string) []any {
	return []any{"CF.DEL", key, item}
}

func DelResult(val any) (bool, error) {
	return redisstack.ParseIntBool(val)
}

func ExistsArgs(key string, item string) []any {
	return []any{"CF.EXISTS", key, item}
}

func ExistsResult(val any) (bool, error) {
	return redisstack.ParseIntBool(val)
}

func InfoArgs(key string) []any {
	return []any{"CF.INFO", key}
}

func InfoResult(val any) (*Info, error) {
	arr, err := redisstack.ParseArray(val, 16)
	if err != nil {
		return nil, err
	}
	res := &Info{
		Size:             arr[1].(int64),
		NumBuckets:       arr[3].(int64),
		NumFilters:       arr[5].(int64),
		NumItemsInserted: arr[7].(int64),
		NumItemsDeleted:  arr[9].(int64),
		BucketSize:       arr[11].(int64),
		ExpansionRate:    arr[13].(int64),
		MaxIteration:     arr[15].(int64),
	}
	return res, nil
}

func insertArgsInternal(command string, key string, capacity *int64, noCreate bool, items []string) []any {
	args := make([]any, 0, 6+len(items))
	args = append(args, command, key)
	if capacity != nil {
		args = append(args, "CAPACITY", *capacity)
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

func InsertArgs(key string, capacity *int64, noCreate bool, items []string) []any {
	return insertArgsInternal("CF.INSERT", key, capacity, noCreate, items)
}

func InsertNXArgs(key string, capacity *int64, noCreate bool, items []string) []any {
	return insertArgsInternal("CF.INSERTNX", key, capacity, noCreate, items)
}

func InsertNXResult(val any) ([]int64, error) {
	return redisstack.ParseScalarArray[int64](val, 0)
}

func LoadChunkArgs(key string, iter int64, data string) []any {
	return []any{"CF.LOADCHUNK", key, iter, data}
}

func MExistsArgs(key string, items []string) []any {
	return redisstack.ArgsByKeyAndItems("CF.MEXISTS", key, items)
}

func MExistsResult(val any) ([]bool, error) {
	return redisstack.ParseIntBoolArray(val, 0)
}

func ReserveArgs(key string, capacity int64, bucketSize *int64, maxIterations *int64, expansionRate *int64) []any {
	args := make([]any, 0, 9)
	args = append(args, "CF.RESERVE", key, capacity)
	if bucketSize != nil {
		args = append(args, "BUCKETSIZE", *bucketSize)
	}
	if maxIterations != nil {
		args = append(args, "MAXITERATIONS", *maxIterations)
	}
	if expansionRate != nil {
		args = append(args, "EXPANSION", *expansionRate)
	}
	return args
}

func ScanDumpArgs(key string, iter int64) []any {
	return []any{"CF.SCANDUMP", key, iter}
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
