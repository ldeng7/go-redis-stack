package opencypher

import (
	"strconv"
	"strings"

	"github.com/ldeng7/go-redis-stack/redisstack"
)

type QueryWritable interface {
	WriteToQuery(sb *strings.Builder) error
}

func QueryWritableToString[T QueryWritable](qw T) (string, error) {
	sb := &strings.Builder{}
	if err := qw.WriteToQuery(sb); err != nil {
		return "", err
	}
	return sb.String(), nil
}

var stringPropertyQuote byte = '"'

func writePropertyValueToQuery(value any, sb *strings.Builder) error {
	if value == nil {
		sb.WriteString("null")
		return nil
	}
	switch v := value.(type) {
	case string:
		sb.WriteByte(stringPropertyQuote)
		sb.WriteString(v)
		sb.WriteByte(stringPropertyQuote)
	case []byte:
		sb.WriteByte(stringPropertyQuote)
		sb.Write(v)
		sb.WriteByte(stringPropertyQuote)
	case bool:
		if v {
			sb.WriteString("true")
		} else {
			sb.WriteString("false")
		}
	case int64:
		sb.WriteString(strconv.FormatInt(v, 10))
	case float64:
		sb.WriteString(strconv.FormatFloat(v, 'f', -1, 64))
	case ArrayPropertyValue:
		if err := v.WriteToQuery(sb); err != nil {
			return err
		}
	case []any:
		if err := ArrayPropertyValue(v).WriteToQuery(sb); err != nil {
			return err
		}
	case MapPropertyValue:
		if err := v.WriteToQuery(sb); err != nil {
			return err
		}
	case map[string]any:
		if err := MapPropertyValue(v).WriteToQuery(sb); err != nil {
			return err
		}
	default:
		return redisstack.ErrInvalidType
	}
	return nil
}

type ArrayPropertyValue []any

func (pv ArrayPropertyValue) WriteToQuery(sb *strings.Builder) error {
	sb.WriteByte('[')
	if len(pv) > 0 {
		if err := writePropertyValueToQuery(pv[0], sb); err != nil {
			return err
		}
	}
	for i := 1; i < len(pv); i++ {
		sb.WriteByte(',')
		if err := writePropertyValueToQuery(pv[i], sb); err != nil {
			return err
		}
	}
	sb.WriteByte(']')
	return nil
}

type MapPropertyValue map[string]any

func (pv MapPropertyValue) WriteToQuery(sb *strings.Builder) error {
	first := true
	sb.WriteByte('{')
	for k, v := range pv {
		if !first {
			sb.WriteByte(',')
		} else {
			first = false
		}
		sb.WriteByte(stringPropertyQuote)
		sb.WriteString(k)
		sb.WriteByte(stringPropertyQuote)
		sb.WriteByte(':')
		if err := writePropertyValueToQuery(v, sb); err != nil {
			return err
		}
	}
	sb.WriteByte('}')
	return nil
}

type baseEntity struct {
	Alias      string
	Label      string
	Properties MapPropertyValue
}

func (e *baseEntity) WriteToQuery(sb *strings.Builder) error {
	sb.WriteString(e.Alias)
	if len(e.Label) > 0 {
		sb.WriteByte(':')
		sb.WriteString(e.Label)
	}
	if len(e.Properties) > 0 {
		sb.WriteByte(' ')
		if err := e.Properties.WriteToQuery(sb); err != nil {
			return err
		}
	}
	return nil
}

type Node struct {
	baseEntity
}

func (n *Node) WriteToQuery(sb *strings.Builder) error {
	sb.WriteByte('(')
	if err := n.baseEntity.WriteToQuery(sb); err != nil {
		return err
	}
	sb.WriteByte(')')
	return nil
}

type Range struct {
	Min int
	Max int
}

type Relationship struct {
	baseEntity
	Hops *Range
}

func (r *Relationship) WriteToQuery(sb *strings.Builder) error {
	sb.WriteByte('[')
	if err := r.baseEntity.WriteToQuery(sb); err != nil {
		return err
	}
	if len(r.Properties) == 0 && r.Hops != nil {
		sb.WriteByte('*')
		if r.Hops.Min > 0 {
			sb.WriteString(strconv.FormatInt(int64(r.Hops.Min), 10))
		}
		sb.WriteString("..")
		if r.Hops.Max > 0 {
			sb.WriteString(strconv.FormatInt(int64(r.Hops.Max), 10))
		}
	}
	sb.WriteByte(']')
	return nil
}

type EntityConnetion string

const (
	EntityConnetionBi    EntityConnetion = "-"
	EntityConnetionLeft  EntityConnetion = "->"
	EntityConnetionRight EntityConnetion = "<-"
)

type RelationshipNodePair struct {
	Conn1        EntityConnetion
	Node         *Node
	Conn2        EntityConnetion
	Relationship *Relationship
}

func (p *RelationshipNodePair) WriteToQuery(sb *strings.Builder) error {
	sb.WriteString(string(p.Conn1))
	if err := p.Node.WriteToQuery(sb); err != nil {
		return err
	}
	sb.WriteString(string(p.Conn2))
	return p.Relationship.WriteToQuery(sb)
}

type Path struct {
	Head *Node
	Tail []RelationshipNodePair
}

func (p *Path) WriteToQuery(sb *strings.Builder) error {
	if err := p.Head.WriteToQuery(sb); err != nil {
		return err
	}
	for _, pair := range p.Tail {
		if err := pair.WriteToQuery(sb); err != nil {
			return err
		}
	}
	return nil
}
