package config

import (
	"github.com/zeromicro/go-queue/kq"
	"github.com/zeromicro/go-zero/core/stores/cache"
	"github.com/zeromicro/go-zero/core/stores/redis"
	"github.com/zeromicro/go-zero/zrpc"
)

type Config struct {
	zrpc.RpcServerConf
	DataSource            string
	CacheRedis            cache.CacheConf
	BizRedis              redis.RedisConf
	KqConsumerConf        kq.KqConf
	ArticleKqConsumerConf kq.KqConf
}
