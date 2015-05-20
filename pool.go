// Copyright 2015 Hansel Dunlop
// See LICENSE for licensing information

// This package contains a Redis connection pool, and using an interface that
// has a switchable backend. At the moment the only other backend is TestBackend
// which lets you use this pool in a unit test environment

package pmemory

import (
	"github.com/garyburd/redigo/redis"
	"time"
)

type Pool struct {
	back Backend
}

func (p *Pool) Do(cmd string, args ...interface{}) (r interface{}, e error) {
	return p.back.Do(cmd, args...)
}

func (p *Pool) Send(cmd string, args ...interface{}) (e error) {
	return p.back.Send(cmd, args...)
}

func (p *Pool) Close() {
	p.back.Close()
}

type Backend interface {
	setup()
	Do(cmd string, args ...interface{}) (interface{}, error)
	Send(cmd string, args ...interface{}) error
	Close()
}

type RedisBackend struct {
	pool *redis.Pool
	conn redis.Conn
}

func (b *RedisBackend) Do(cmd string, args ...interface{}) (r interface{}, e error) {
	if b.conn == nil {
		b.conn = b.pool.Get()
	}
	r, e = b.conn.Do(cmd, args...)
	return
}

func (b *RedisBackend) Send(cmd string, args ...interface{}) (e error) {
	if b.conn == nil {
		b.conn = b.pool.Get()
	}
	e = b.conn.Send(cmd, args...)
	return
}

func (b *RedisBackend) Close() {
	b.conn.Close()
	b.conn = nil
}

func (b *RedisBackend) setup() {
	b.pool = &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", ":6379")
			if err != nil {
				return nil, err
			}
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}
}

type TestBackend struct{}

func (b *TestBackend) Do(cmd string, args ...interface{}) (r interface{}, e error) {
	switch {
	case cmd == "SMEMBERS":
		return [][]byte{[]byte("12"), []byte("234"), []byte("2")}, nil
	case cmd == "EXEC":
		return []map[string]string{map[string]string{"dear": "me"}}, nil
	}
	return nil, nil
}

func (p *TestBackend) Send(cmd string, args ...interface{}) (e error) {

	return nil
}

func (b *TestBackend) Close() {}

func (b *TestBackend) setup() {}

func New(b Backend) (c *Pool) {
	b.setup()
	return &Pool{back: b}
}
