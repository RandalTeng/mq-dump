package mq

import (
	"context"
	"testing"

	"github.com/RandalTeng/mq-dump/config"
	"github.com/RandalTeng/mq-dump/model"
)

type stubFactory struct{}

func (stubFactory) NewConfig() any                          { return &struct{}{} }
func (stubFactory) ConfigTemplate() string                  { return "stub: {}" }
func (stubFactory) Open(config.Common, any) (Driver, error) { return stubDriver{}, nil }

type stubDriver struct{}

func (stubDriver) Export(context.Context, func(model.Message) error) error           { return nil }
func (stubDriver) Import(context.Context, func() (model.Message, bool, error)) error { return nil }
func (stubDriver) Close() error                                                      { return nil }

func TestRegisterAndGet(t *testing.T) {
	Register("stub", stubFactory{})
	if _, ok := Get("stub"); !ok {
		t.Error("registered driver not found")
	}
	if _, ok := Get("nope"); ok {
		t.Error("unknown driver should not be found")
	}
	found := false
	for _, n := range Names() {
		if n == "stub" {
			found = true
		}
	}
	if !found {
		t.Error("Names() missing stub")
	}
}

func TestRegisterDuplicatePanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("duplicate Register should panic")
		}
	}()
	Register("dup", stubFactory{})
	Register("dup", stubFactory{})
}
