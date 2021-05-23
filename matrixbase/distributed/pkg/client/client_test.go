package client

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSet(t *testing.T) {
	c := New("127.0.0.1:8081")

	key := []byte("key")
	value := []byte("value")
	err := c.Set(key, value)
	assert.NoError(t, err, "TestSet failed")
}

func TestGet(t *testing.T) {
	c := New("127.0.0.1:8081")

	key := []byte("key")
	value := []byte("value")
	err := c.Set(key, value)
	assert.NoError(t, err, "TestGet failed")

	v, err := c.Get(key)
	assert.NoError(t, err, "TestGet failed")
	assert.Equal(t, string(value), string(v), "TestGet failed")
}

func TestGetAll(t *testing.T) {
	c := New("127.0.0.1:8083")
	for i:=0; i < 1000; i++ {
		k := []byte("testkey" + fmt.Sprintf("%d",i))
		v := "testvalue" + fmt.Sprintf("%d",i)
		val, err := c.Get(k)
		assert.NoError(t, err, "TestGet failed")

		assert.Equal(t, string(v), string(val), "TestGet failed, key:", string(k), "value:", string(val))
	}
}

func TestDelete(t *testing.T) {
	c := New("127.0.0.1:8081")

	key := []byte("key")
	value := []byte("value")
	err := c.Set(key, value)
	assert.NoError(t, err, "TestDelete failed")

	v, err := c.Get(key)
	assert.NoError(t, err, "TestDelete failed")
	assert.Equal(t, string(value), string(v), "TestDelete failed")

	assert.NoError(t, c.Delete(key), "TestDelete failed")

	v, err = c.Get(key)
	assert.NoError(t, err, "TestDelete failed")
	assert.Empty(t, v, "TestDelete failed")
}
