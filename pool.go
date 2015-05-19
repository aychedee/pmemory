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

type Backend interface {
	setup()
	Do(cmd string, args ...interface{}) (interface{}, error)
}

type RedisBackend struct {
	pool *redis.Pool
	conn redis.Conn
}

func (b *RedisBackend) Do(cmd string, args ...interface{}) (r interface{}, e error) {
	b.conn = b.pool.Get()
	defer b.conn.Close()
	r, e = b.conn.Do(cmd, args...)
	return
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
	return nil, nil
}

func (b *TestBackend) setup() {}

func New(b Backend) (c *Pool) {
	b.setup()
	return &Pool{back: b}
}
