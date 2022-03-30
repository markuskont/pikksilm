package models

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEntrySet(t *testing.T) {
	e1 := Entry{
		"foo": "bar",
		"baz": Entry{
			"a": 1,
		},
	}
	e1.Set(13, "baz", "b")
	get1, ok := e1.Get("baz", "b")
	assert.True(t, ok)
	assert.Equal(t, 13, get1)

	e2 := e1
	val2 := Entry{
		"zzz": Entry{},
	}
	e2.Set(val2, "ddd")
	get2, ok := e2.Get("ddd")
	assert.True(t, ok)
	assert.Equal(t, val2, get2)

	e3 := e2
	val3 := Entry{
		"zzz": Entry{},
	}
	e3.Set(val3, "ddd", "lll", "kkk")
	get3, ok := e3.Get("ddd", "lll", "kkk")
	assert.True(t, ok)
	assert.Equal(t, val3, get3)

	e4 := e3
	e4.Set(val3, "foo")
	get4, ok := e4.Get("foo")
	assert.True(t, ok)
	assert.Equal(t, val3, get4)
}
