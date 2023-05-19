package redisstack

import (
	"errors"
)

var ErrInvalidType = errors.New("invalid type")
var ErrInvalidData = errors.New("invalid data")

type StringAnyPair struct {
	Key   string
	Value any
}

type ItemAmount struct {
	Item   string
	Amount int64
}

func ArgsByKeyAndItems(command string, key string, items []string) []any {
	args := make([]any, 2+len(items))
	args[0], args[1] = command, key
	for i, item := range items {
		args[i+2] = item
	}
	return args
}

func ArgsByKeyAndItemAmounts(command string, key string, itemAmounts []ItemAmount) []any {
	args := make([]any, 2+len(itemAmounts)*2)
	args[0], args[1] = command, key
	for i, itemAmount := range itemAmounts {
		args[i*2+2], args[i*2+3] = itemAmount.Item, itemAmount.Amount
	}
	return args
}

func ParseScalar[T any](val any) (T, error) {
	t, ok := val.(T)
	if !ok {
		var t T
		return t, ErrInvalidType
	}
	return t, nil
}

func ParseIntBool(val any) (bool, error) {
	i, err := ParseScalar[int64](val)
	return i != 0, err
}

func ParseArray(val any, minLen int) ([]any, error) {
	arr, ok := val.([]any)
	if !ok {
		return nil, ErrInvalidType
	} else if minLen > 0 && len(arr) < minLen {
		return nil, ErrInvalidData
	}
	return arr, nil
}

func ParseToMappedArray[T any](val any, minLen int, f func(any) (T, error)) ([]T, error) {
	arr, err := ParseArray(val, minLen)
	if err != nil {
		return nil, err
	}
	res := make([]T, len(arr))
	for i, e := range arr {
		if res[i], err = f(e); err != nil {
			return nil, err
		}
	}
	return res, nil
}

func ParseScalarArray[T any](val any, minLen int) ([]T, error) {
	return ParseToMappedArray(val, minLen, ParseScalar[T])
}

func ParseNullableScalarArray[T any](val any, minLen int) ([]*T, error) {
	return ParseToMappedArray(val, minLen, func(e any) (*T, error) {
		if e1, ok := e.(T); ok {
			return &e1, nil
		}
		return nil, nil
	})
}

func ParseIntBoolArray(val any, minLen int) ([]bool, error) {
	return ParseToMappedArray(val, minLen, ParseIntBool)
}

func ParseStringPairArray(val any, minLen int) ([][2]string, error) {
	return ParseToMappedArray(val, minLen, func(e any) ([2]string, error) {
		arr, err := ParseArray(e, 2)
		if err != nil {
			return [2]string{}, err
		}
		s0, _ := arr[0].(string)
		s1, _ := arr[1].(string)
		return [2]string{s0, s1}, nil
	})
}

func ParseStringAnyPairArray(val any, minLen int) ([]StringAnyPair, error) {
	return ParseToMappedArray(val, minLen, func(e any) (StringAnyPair, error) {
		arr, err := ParseArray(e, 2)
		if err != nil {
			return StringAnyPair{}, err
		}
		k, _ := arr[0].(string)
		return StringAnyPair{k, arr[1]}, nil
	})
}

func ParseToInterlacedMappedArray[T any](val any, minLen int, f func(any, any) (T, error)) ([]T, error) {
	arr, err := ParseArray(val, minLen*2)
	if err != nil {
		return nil, err
	}
	l := len(arr) / 2
	res := make([]T, l)
	for i := 0; i < l; i++ {
		if res[i], err = f(arr[i*2], arr[i*2+1]); err != nil {
			return nil, err
		}
	}
	return res, nil
}

func ParseItemAmountInterlacedArray(val any, minLen int) ([]ItemAmount, error) {
	return ParseToInterlacedMappedArray(val, minLen, func(e1, e2 any) (ItemAmount, error) {
		item, _ := e1.(string)
		amount, _ := e2.(int64)
		return ItemAmount{item, amount}, nil
	})
}
