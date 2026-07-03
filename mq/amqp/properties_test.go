package amqp

import (
	"testing"

	"github.com/goccy/go-json"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/RandalTeng/mq-dump/model"
)

func TestDeliveryToMessageRoundTrip(t *testing.T) {
	d := amqp.Delivery{
		Exchange:      "a",
		RoutingKey:    "k",
		ContentType:   "text/plain",
		DeliveryMode:  2,
		CorrelationId: "c1",
		Body:          []byte("hi"),
		Headers:       amqp.Table{"x": int32(7)},
	}
	m := deliveryToMessage(d)
	if string(m.Body) != "hi" {
		t.Fatalf("body = %q", m.Body)
	}
	var p Properties
	if err := json.Unmarshal(m.Properties, &p); err != nil {
		t.Fatal(err)
	}
	if p.Exchange != "a" || p.RoutingKey != "k" || p.ContentType != "text/plain" || p.DeliveryMode != 2 || p.CorrelationID != "c1" {
		t.Errorf("props = %+v", p)
	}
	if p.AMQPHeaders["x"] != int32(7) {
		t.Errorf("headers = %+v", p.AMQPHeaders)
	}
}

func TestMessageToPublishing(t *testing.T) {
	p := Properties{ContentType: "application/json", DeliveryMode: 2, CorrelationID: "c1", AMQPHeaders: amqp.Table{"x": int32(7)}}
	pb, _ := json.Marshal(p)
	pub := messageToPublishing(model.Message{Body: []byte("hi"), Properties: pb})
	if pub.ContentType != "application/json" || pub.DeliveryMode != 2 || pub.CorrelationId != "c1" {
		t.Errorf("pub = %+v", pub)
	}
	if string(pub.Body) != "hi" || pub.Headers["x"] != int32(7) {
		t.Errorf("pub body/headers = %+v", pub)
	}
}
