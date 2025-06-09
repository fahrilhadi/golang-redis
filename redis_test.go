package golang_redis

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
)

var client = redis.NewClient(&redis.Options{
	Addr: "localhost:6379",
	DB: 0,
})

func TestConnection(t *testing.T)  {
	assert.NotNil(t, client)

	// err := client.Close()
	// assert.Nil(t, err)
}

var ctx = context.Background()

func TestPing(t *testing.T)  {
	result, err := client.Ping(ctx).Result()
	assert.Nil(t, err)
	assert.Equal(t, "PONG", result)
}

func TestString(t *testing.T)  {
	client.SetEx(ctx, "name", "Fahril Hadi", 3 * time.Second)

	result, err := client.Get(ctx, "name").Result()
	assert.Nil(t, err)
	assert.Equal(t, "Fahril Hadi", result)

	time.Sleep(5 * time.Second)

	result, err = client.Get(ctx, "name").Result()
	assert.NotNil(t, err)
}

func TestList(t *testing.T)  {
	client.RPush(ctx, "name", "Fahril")
	client.RPush(ctx, "name", "Hadi")

	assert.Equal(t, "Fahril", client.LPop(ctx, "names").Val())
	assert.Equal(t, "Hadi", client.LPop(ctx, "names").Val())

	client.Del(ctx, "names")
}

func TestSet(t *testing.T)  {
	client.SAdd(ctx, "students", "Fahril")
	client.SAdd(ctx, "students", "Fahril")
	client.SAdd(ctx, "students", "Hadi")
	client.SAdd(ctx, "students", "Hadi")

	assert.Equal(t, int64(2), client.SCard(ctx, "students").Val())
	assert.Equal(t, []string{"Fahril", "Hadi"}, client.SMembers(ctx, "students").Val())
}