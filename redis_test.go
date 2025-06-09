package golang_redis

import (
	"context"
	"fmt"
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

func TestSortedSet(t *testing.T)  {
	client.ZAdd(ctx, "scores", redis.Z{Score: 100, Member: "Fahril"})
	client.ZAdd(ctx, "scores", redis.Z{Score: 85, Member: "Abu"})
	client.ZAdd(ctx, "scores", redis.Z{Score: 95, Member: "Fadli"})

	assert.Equal(t, []string{"Abu", "Fadli", "Fahril"}, client.ZRange(ctx, "scores", 0, -1).Val())

	assert.Equal(t, "Fahril", client.ZPopMax(ctx, "scores").Val()[0].Member)
	assert.Equal(t, "Abu", client.ZPopMax(ctx, "scores").Val()[0].Member)
	assert.Equal(t, "Fadli", client.ZPopMax(ctx, "scores").Val()[0].Member)
}

func TestHash(t *testing.T)  {
	client.HSet(ctx, "user:1", "id", "1")
	client.HSet(ctx, "user:1", "name", "Fahril")
	client.HSet(ctx, "user:1", "email", "fahril@example.com")

	user := client.HGetAll(ctx, "user:1").Val()

	assert.Equal(t, "1", user["id"])
	assert.Equal(t, "Fahril", user["name"])
	assert.Equal(t, "fahril@example.com", user["email"])

	client.Del(ctx, "user:1")
}

func TestGeoPoint(t *testing.T)  {
	client.GeoAdd(ctx, "sellers", &redis.GeoLocation{
		Name: "Toko A",
		Longitude: 101.368330,
		Latitude: 0.509187,
	})

	client.GeoAdd(ctx, "sellers", &redis.GeoLocation{
		Name: "Toko B",
		Longitude: 101.394572,
		Latitude: 0.478720,
	})

	distance := client.GeoDist(ctx, "sellers", "Toko A", "Toko B", "km").Val()
	assert.Equal(t, 1, distance)

	sellers := client.GeoSearch(ctx, "sellers", &redis.GeoSearchQuery{
		Longitude: 101.394572,
		Latitude: 0.478720,
		Radius: 5,
		RadiusUnit: "km",
	}).Val()

	assert.Equal(t, []string{"Toko A", "Toko B"}, sellers)
}

func TestHyperLogLog(t *testing.T)  {
	client.PFAdd(ctx, "visitors", "fahril", "hadi")
	client.PFAdd(ctx, "visitors", "fahril", "abu")
	client.PFAdd(ctx, "visitors", "fadli", "abu")

	total := client.PFCount(ctx, "visitors").Val()
	assert.Equal(t, int64(6), total)
}

func TestPipeline(t *testing.T)  {
	_, err := client.Pipelined(ctx, func(pipeliner redis.Pipeliner) error {
		pipeliner.SetEx(ctx, "name", "fahril", 5 * time.Second)
		pipeliner.SetEx(ctx, "address", "indonesia", 5 * time.Second)
		return nil
	})
	assert.Nil(t, err)

	assert.Equal(t, "fahril", client.Get(ctx, "name").Val())
	assert.Equal(t, "indonesia", client.Get(ctx, "address").Val())
}

func TestTransaction(t *testing.T)  {
	_, err := client.TxPipelined(ctx, func(pipeliner redis.Pipeliner) error {
		pipeliner.SetEx(ctx, "name", "Abu", 5 * time.Second)
		pipeliner.SetEx(ctx, "address", "Pelalawan", 5 * time.Second)
		return nil
	})
	assert.Nil(t, err)

	assert.Equal(t, "Abu", client.Get(ctx, "name").Val())
	assert.Equal(t, "Pelalawan", client.Get(ctx, "address").Val())
}

func TestPublishStream(t *testing.T)  {
	for i := 0; i < 10; i++ {
		err := client.XAdd(ctx, &redis.XAddArgs{
			Stream: "member",
			Values: map[string]interface{}{
				"name": "fahril",
				"address": "indonesia",
			},
		}).Err()
		assert.Nil(t, err)
	}
}

func TestCreateConsumerGroup(t *testing.T)  {
	client.XGroupCreate(ctx, "members", "group-1", "0")
	client.XGroupCreateConsumer(ctx, "members", "group-1", "consumer-1")
	client.XGroupCreateConsumer(ctx, "members", "group-1", "consumer-2")
}

func TestConsumeStream(t *testing.T)  {
	streams := client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group: "group-1",
		Consumer: "consumer-1",
		Streams: []string{"members", ">"},
		Count: 2,
		Block: 5 * time.Second,
	}).Val()

	for _, stream := range streams {
		for _, message := range stream.Messages {
			fmt.Println(message.ID)
			fmt.Println(message.Values)
		}
	}
}