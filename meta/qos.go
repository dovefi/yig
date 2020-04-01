package meta

import (
	"fmt"
	"github.com/go-redis/redis/v7"
	"github.com/go-redis/redis_rate/v8"
	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/meta/client"
	"github.com/journeymidnight/yig/meta/types"
	"io"
	"time"
)

const (
	defaultReadQps       = 2000
	defaultWriteQps      = 1000
	defaultBandwidthKBps = 100 * 1024 // 100 MBps
)

type QosMeta struct {
	client      client.Client
	rateLimiter *redis_rate.Limiter

	// Not using a mutex to protect bucketUser or userQosLimit,
	// since it's acceptable to read stale or empty values.
	// bucket name -> user id
	bucketUser map[string]string
	// user id -> user qos limit
	userQosLimit map[string]types.UserQos
}

func NewQosMeta(client client.Client) *QosMeta {
	redisClient := redis.NewClient(&redis.Options{
		Addr:     helper.CONFIG.RedisAddress,
		Password: helper.CONFIG.RedisPassword,
	})
	limiter := redis_rate.NewLimiter(redisClient)
	m := &QosMeta{
		client:      client,
		rateLimiter: limiter,
	}
	go m.inMemoryCacheSync()
	return m
}

func (m *QosMeta) AllowReadQuery(bucketName string) (allow bool) {
	userID := m.bucketUser[bucketName]
	qps := m.userQosLimit[userID].ReadQps
	if qps <= 0 {
		qps = defaultReadQps
	}
	key := fmt.Sprintf("user_rqps_%s", userID)
	// the key actually used in redis would have a prefix "rate:"
	result, err := m.rateLimiter.Allow(key, redis_rate.PerSecond(qps))
	if err != nil {
		helper.Logger.Error("AllowReadQuery:", err)
		return true
	}
	return result.Allowed
}

func (m *QosMeta) AllowWriteQuery(bucketName string) (allow bool) {
	userID := m.bucketUser[bucketName]
	qps := m.userQosLimit[userID].WriteQps
	if qps <= 0 {
		qps = defaultWriteQps
	}
	key := fmt.Sprintf("user_wqps_%s", userID)
	// the key actually used in redis would have a prefix "rate:"
	result, err := m.rateLimiter.Allow(key, redis_rate.PerSecond(qps))
	if err != nil {
		helper.Logger.Error("AllowWriteQuery:", err)
		return true
	}
	return result.Allowed
}

func (m *QosMeta) ThrottleReader(bucketName string, reader io.Reader) ThrottleReader {
	userID := m.bucketUser[bucketName]
	bandwidthKBps := m.userQosLimit[userID].Bandwidth
	if bandwidthKBps <= 0 {
		bandwidthKBps = defaultBandwidthKBps
	}
	return ThrottleReader{
		reader:      reader,
		rateLimiter: m.rateLimiter,
		userID:      userID,
		kbpsLimit:   bandwidthKBps,
	}
}

// I believe it's OK to load all data into memory when user count < 10k, so...
func (m *QosMeta) inMemoryCacheSync() {
	for {
		bucketUser, err := m.client.GetAllUserBuckets()
		if err != nil {
			helper.Logger.Error("GetAllUserBuckets error:", err)
			bucketUser = nil
		}
		userQpsLimit, err := m.client.GetAllUserQos()
		if err != nil {
			helper.Logger.Error("GetAllUserQos error:", err)
			userQpsLimit = nil
		}
		if bucketUser != nil {
			m.bucketUser = bucketUser
		}
		if userQpsLimit != nil {
			m.userQosLimit = userQpsLimit
		}

		time.Sleep(10 * time.Minute)
	}
}

type ThrottleReader struct {
	reader      io.Reader
	rateLimiter *redis_rate.Limiter
	userID      string
	kbpsLimit   int // KBps
}

func (r ThrottleReader) maybeWaitTokenN(n int) {
	key := fmt.Sprintf("user_bandwidth_%s", r.userID)
	for {
		result, err := r.rateLimiter.AllowN(key,
			redis_rate.PerSecond(r.kbpsLimit*1024), n)
		if err != nil {
			helper.Logger.Error("ThrottleReader:", err)
			return
		}
		if result.Allowed {
			return
		}
		time.Sleep(result.RetryAfter)
	}
}

// Note by test, if 1024 * kbpsLimit < len(p), which is rare,
// speed would always be 0, i.e. Read() would block forever
func (r ThrottleReader) Read(p []byte) (int, error) {
	r.maybeWaitTokenN(len(p))
	n, err := r.reader.Read(p)
	// TODO if n < len(p), we might need to refill tokens back
	return n, err
}
