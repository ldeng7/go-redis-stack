package graph

import "github.com/ldeng7/go-redis-stack/redisstack"

func DeleteArgs(key string) []any {
	return []any{"GRAPH.DELETE", key}
}

func ListResult(val any) ([]string, error) {
	return redisstack.ParseScalarArray[string](val, 0)
}

func QueryArgs(key string, query string) []any {
	return []any{"GRAPH.QUERY", key, query}
}

type ResultSet struct {
	Header []string
	Rows   [][]any
}

func QueryResult(val any) (*ResultSet, error) {
	arr, err := redisstack.ParseArray(val, 2)
	if err != nil {
		return nil, err
	}
	res := &ResultSet{}
	if res.Header, err = redisstack.ParseScalarArray[string](arr[0], 0); err != nil {
		return nil, err
	}

	nCols := len(res.Header)
	arr1, err := redisstack.ParseArray(arr[1], 0)
	if err != nil {
		return nil, err
	}
	rows := make([][]any, len(arr1))
	for i, e := range arr1 {
		rows[i], err = redisstack.ParseArray(e, nCols)
		if err != nil {
			return nil, err
		}
	}
	res.Rows = rows

	return res, nil
}

type Node struct {
	ID         int64
	Label      *string
	Properties []redisstack.StringAnyPair
}

func ParseNode(val any) (*Node, error) {
	arr, err := redisstack.ParseArray(val, 3)
	if err != nil {
		return nil, err
	}

	res := &Node{}
	var arr1, arr2 []any
	if arr1, err = redisstack.ParseArray(arr[0], 2); err != nil {
		return nil, err
	}
	res.ID, _ = arr1[1].(int64)

	if arr1, err = redisstack.ParseArray(arr[1], 2); err != nil {
		return nil, err
	} else if arr2, err = redisstack.ParseArray(arr1[1], 0); err != nil {
		return nil, err
	}
	if len(arr2) > 0 {
		label, _ := arr2[0].(string)
		res.Label = &label
	}

	if arr1, err = redisstack.ParseArray(arr[2], 2); err != nil {
		return nil, err
	} else if arr2, err = redisstack.ParseArray(arr1[1], 0); err != nil {
		return nil, err
	} else if res.Properties, err = redisstack.ParseStringAnyPairArray(arr2, 0); err != nil {
		return nil, err
	}

	return res, nil
}

type Relationship struct {
	ID         int64
	Type       string
	SrcNodeID  int64
	DestNodeID int64
	Properties []redisstack.StringAnyPair
}

func ParseRelationship(val any) (*Relationship, error) {
	arr, err := redisstack.ParseArray(val, 5)
	if err != nil {
		return nil, err
	}

	res := &Relationship{}
	var arr1, arr2 []any
	if arr1, err = redisstack.ParseArray(arr[0], 2); err != nil {
		return nil, err
	}
	res.ID, _ = arr1[1].(int64)

	if arr1, err = redisstack.ParseArray(arr[1], 2); err != nil {
		return nil, err
	}
	res.Type, _ = arr1[1].(string)

	if arr1, err = redisstack.ParseArray(arr[2], 2); err != nil {
		return nil, err
	}
	res.SrcNodeID, _ = arr1[1].(int64)

	if arr1, err = redisstack.ParseArray(arr[3], 2); err != nil {
		return nil, err
	}
	res.DestNodeID, _ = arr1[1].(int64)

	if arr1, err = redisstack.ParseArray(arr[4], 2); err != nil {
		return nil, err
	} else if arr2, err = redisstack.ParseArray(arr1[1], 0); err != nil {
		return nil, err
	} else if res.Properties, err = redisstack.ParseStringAnyPairArray(arr2, 0); err != nil {
		return nil, err
	}

	return res, nil
}
