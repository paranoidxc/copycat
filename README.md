# copycat

**go-zero 项目的微服务的实战项目** 

## grpc 微服务

- user:8080
- article:8480
- like

## Api/BFF层 微服务

- article:8980
- applet:8888

##  使用的其他服务

- mysql:13306
- redis:6379
- etcd:2479
- kafka:9092
- elasticsearch
- canal:11111

# 启动顺序

- etcd
- mysql 首次需要配置主从
- redis
- kafka 首次需要创建主题
- elasticsearch 首次需要创建索引
- canal  首次需要配置
- user-rpc
- article-rpc
- like-rpc
- article-bff
- applet-bff

# todo 

- [ ] admin 中台