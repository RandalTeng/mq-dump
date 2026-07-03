package amqp

import (
	"fmt"
	"time"

	"github.com/goccy/go-json"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/RandalTeng/mq-dump/model"
)

// Properties 是 AMQP 驱动私有的消息属性(含原始路由),序列化进 Message.Properties。
type Properties struct {
	Exchange      string     `json:"exchange,omitempty"`
	RoutingKey    string     `json:"routing_key,omitempty"`
	ContentType   string     `json:"content_type,omitempty"`
	DeliveryMode  uint8      `json:"delivery_mode,omitempty"`
	CorrelationID string     `json:"correlation_id,omitempty"`
	Priority      uint8      `json:"priority,omitempty"`
	Expiration    string     `json:"expiration,omitempty"`
	MessageID     string     `json:"message_id,omitempty"`
	Type          string     `json:"type,omitempty"`
	AMQPHeaders   amqp.Table `json:"amqp_headers,omitempty"`
}

// taggedField 携带 AMQP 字段的原始 Go 类型标签,使 amqp.Table 中的值能在
// JSON 往返后保留精确类型(否则 interface{} 会把所有数字解回 float64)。
type taggedField struct {
	T string          `json:"t"`
	V json.RawMessage `json:"v,omitempty"`
}

// propsAlias 用于在自定义 (Un)MarshalJSON 中复用标准字段编解码而不递归。
type propsAlias Properties

// propsJSON 用类型标签映射覆盖内嵌别名中的原始 amqp.Table 字段(浅层字段胜出)。
type propsJSON struct {
	propsAlias
	AMQPHeaders map[string]taggedField `json:"amqp_headers,omitempty"`
}

// MarshalJSON 编码属性,并把 AMQP headers 转成类型标签映射以保留精确类型。
func (p Properties) MarshalJSON() ([]byte, error) {
	enc, err := encodeTable(p.AMQPHeaders)
	if err != nil {
		return nil, err
	}
	return json.Marshal(propsJSON{propsAlias: propsAlias(p), AMQPHeaders: enc})
}

// UnmarshalJSON 解码属性,并从类型标签映射还原 AMQP headers 的精确类型。
func (p *Properties) UnmarshalJSON(data []byte) error {
	var aux propsJSON
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	*p = Properties(aux.propsAlias)
	tbl, err := decodeTable(aux.AMQPHeaders)
	if err != nil {
		return err
	}
	p.AMQPHeaders = tbl
	return nil
}

// encodeTable 把 amqp.Table 转成带类型标签的可 JSON 化映射。
func encodeTable(t amqp.Table) (map[string]taggedField, error) {
	if len(t) == 0 {
		return nil, nil
	}
	out := make(map[string]taggedField, len(t))
	for k, v := range t {
		tf, err := encodeField(v)
		if err != nil {
			return nil, fmt.Errorf("amqp: encode header %q: %w", k, err)
		}
		out[k] = tf
	}
	return out, nil
}

// decodeTable 从类型标签映射还原 amqp.Table。
func decodeTable(m map[string]taggedField) (amqp.Table, error) {
	if len(m) == 0 {
		return nil, nil
	}
	out := make(amqp.Table, len(m))
	for k, tf := range m {
		v, err := decodeField(tf)
		if err != nil {
			return nil, fmt.Errorf("amqp: decode header %q: %w", k, err)
		}
		out[k] = v
	}
	return out, nil
}

// encodeField 按 AMQP 支持的字段类型集打标签并序列化值。
func encodeField(v any) (taggedField, error) {
	var tf taggedField
	var raw any
	switch val := v.(type) {
	case nil:
		return taggedField{T: "null"}, nil
	case bool:
		tf.T, raw = "bool", val
	case byte: // uint8
		tf.T, raw = "byte", val
	case int8:
		tf.T, raw = "int8", val
	case int16:
		tf.T, raw = "int16", val
	case int:
		tf.T, raw = "int", val
	case int32:
		tf.T, raw = "int32", val
	case int64:
		tf.T, raw = "int64", val
	case uint16:
		tf.T, raw = "uint16", val
	case uint32:
		tf.T, raw = "uint32", val
	case float32:
		tf.T, raw = "float32", val
	case float64:
		tf.T, raw = "float64", val
	case string:
		tf.T, raw = "string", val
	case []byte:
		tf.T, raw = "bytes", val // base64
	case time.Time:
		tf.T, raw = "time", val.Format(time.RFC3339Nano)
	case amqp.Decimal:
		tf.T, raw = "decimal", val
	case amqp.Table:
		m, err := encodeTable(val)
		if err != nil {
			return tf, err
		}
		tf.T, raw = "table", m
	case []any:
		arr := make([]taggedField, len(val))
		for i, e := range val {
			ef, err := encodeField(e)
			if err != nil {
				return tf, err
			}
			arr[i] = ef
		}
		tf.T, raw = "array", arr
	default:
		return tf, fmt.Errorf("unsupported header type %T", v)
	}
	b, err := json.Marshal(raw)
	if err != nil {
		return tf, err
	}
	tf.V = b
	return tf, nil
}

// unmarshalAs 把标签值解码为具体类型 T,再作为 any 返回,保留精确 Go 类型。
func unmarshalAs[T any](raw json.RawMessage) (any, error) {
	var v T
	if err := json.Unmarshal(raw, &v); err != nil {
		return nil, err
	}
	return v, nil
}

// decodeField 依据类型标签把值还原为对应的 Go 类型。
func decodeField(tf taggedField) (any, error) {
	switch tf.T {
	case "null":
		return nil, nil
	case "bool":
		return unmarshalAs[bool](tf.V)
	case "byte":
		return unmarshalAs[byte](tf.V)
	case "int8":
		return unmarshalAs[int8](tf.V)
	case "int16":
		return unmarshalAs[int16](tf.V)
	case "int":
		return unmarshalAs[int](tf.V)
	case "int32":
		return unmarshalAs[int32](tf.V)
	case "int64":
		return unmarshalAs[int64](tf.V)
	case "uint16":
		return unmarshalAs[uint16](tf.V)
	case "uint32":
		return unmarshalAs[uint32](tf.V)
	case "float32":
		return unmarshalAs[float32](tf.V)
	case "float64":
		return unmarshalAs[float64](tf.V)
	case "string":
		return unmarshalAs[string](tf.V)
	case "bytes":
		return unmarshalAs[[]byte](tf.V)
	case "decimal":
		return unmarshalAs[amqp.Decimal](tf.V)
	case "time":
		var s string
		if err := json.Unmarshal(tf.V, &s); err != nil {
			return nil, err
		}
		return time.Parse(time.RFC3339Nano, s)
	case "table":
		var m map[string]taggedField
		if err := json.Unmarshal(tf.V, &m); err != nil {
			return nil, err
		}
		return decodeTable(m)
	case "array":
		var arr []taggedField
		if err := json.Unmarshal(tf.V, &arr); err != nil {
			return nil, err
		}
		out := make([]any, len(arr))
		for i, e := range arr {
			dv, err := decodeField(e)
			if err != nil {
				return nil, err
			}
			out[i] = dv
		}
		return out, nil
	default:
		return nil, fmt.Errorf("amqp: unknown header type tag %q", tf.T)
	}
}

// deliveryToMessage 把 broker 投递转换为通用信封 + AMQP Properties。
func deliveryToMessage(d amqp.Delivery) model.Message {
	p := Properties{
		Exchange: d.Exchange, RoutingKey: d.RoutingKey,
		ContentType: d.ContentType, DeliveryMode: d.DeliveryMode,
		CorrelationID: d.CorrelationId, Priority: d.Priority,
		Expiration: d.Expiration, MessageID: d.MessageId, Type: d.Type,
		AMQPHeaders: d.Headers,
	}
	raw, _ := json.Marshal(p)
	return model.Message{Body: d.Body, Timestamp: d.Timestamp, Properties: raw}
}

// messageToPublishing 从通用信封重建 amqp.Publishing(不含路由目标,由 target() 决定)。
func messageToPublishing(m model.Message) amqp.Publishing {
	var p Properties
	_ = json.Unmarshal(m.Properties, &p)
	return amqp.Publishing{
		ContentType: p.ContentType, DeliveryMode: p.DeliveryMode,
		CorrelationId: p.CorrelationID, Priority: p.Priority,
		Expiration: p.Expiration, MessageId: p.MessageID, Type: p.Type,
		Headers: p.AMQPHeaders, Body: m.Body, Timestamp: m.Timestamp,
	}
}
