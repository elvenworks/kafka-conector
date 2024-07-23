// Code generated by mockery v2.36.0. DO NOT EDIT.

package producer_async

import (
	mock "github.com/stretchr/testify/mock"

	sarama "github.com/Shopify/sarama"
)

// IProducerAsync is an autogenerated mock type for the IProducerAsync type
type ProducerAsyncMock struct {
	mock.Mock
}

// GetConfig provides a mock function with given fields:
func (_m *ProducerAsyncMock) GetConfig() *sarama.Config {
	ret := _m.Called()

	var r0 *sarama.Config
	if rf, ok := ret.Get(0).(func() *sarama.Config); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*sarama.Config)
		}
	}

	return r0
}

// Produce provides a mock function with given fields: topic, message
func (_m *ProducerAsyncMock) Produce(topic string, message interface{}) {
	_m.Called(topic, message)
}

// ProduceWithMessageKey provides a mock function with given fields: topic, key, message
func (_m *ProducerAsyncMock) ProduceWithMessageKey(topic string, key interface{}, message interface{}) {
	_m.Called(topic, key, message)
}

// NewIProducerAsync creates a new instance of IProducerAsync. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewIProducerAsync(t interface {
	mock.TestingT
	Cleanup(func())
}) *ProducerAsyncMock {
	mock := &ProducerAsyncMock{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
