# GoAnalysisDB

本项目用于从 Kafka 消费埋点数据并存储到 ClickHouse，支持页面浏览日志（pageview）和事件日志（eventlog）的自动解析与入库。

## 功能简介
- 消费 Kafka 消息队列中的埋点数据。
- 解析页面浏览日志和事件日志。
- 自动分析 UA、IP 信息，补充地理位置、设备、浏览器等字段。
- 数据写入 ClickHouse，支持自动建表。

## 目录结构
```
├── analysisdb.go           # 主程序入口
├── go.mod / go.sum         # Go 依赖管理
├── conf/config.yaml        # 配置文件
├── config/config.go        # 配置加载逻辑
├── GeoLite2-City.mmdb      # IP 地理库
├── logs/                   # 日志目录
```

## 依赖环境
- Go 1.18+
- Kafka
- ClickHouse
- [GeoLite2-City.mmdb](https://dev.maxmind.com/geoip/geolite2-free-geolocation-data)

## 主要依赖包
- github.com/segmentio/kafka-go
- github.com/uptrace/go-clickhouse/ch
- github.com/mssola/user_agent
- github.com/oschwald/geoip2-golang
- github.com/spf13/viper

## 快速开始
1. **配置文件**
   - 编辑 `conf/config.yaml`，配置 Kafka、ClickHouse 等参数。
2. **准备地理库**
   - 下载 `GeoLite2-City.mmdb` 并放置于项目根目录。
3. **安装依赖**
   ```sh
   go mod tidy
   ```
4. **运行服务**
   ```sh
   go run analysisdb.go
   ```

## 数据结构说明
- **PageViewLog**：页面浏览日志，包含页面、设备、地理、会话等信息。
- **Eventlog**：事件日志，记录用户行为事件。

## 典型流程
1. Kafka 消息到达，区分 `pageview` 或 `eventlog`。
2. 解析 JSON，补全 UA、IP 等信息。
3. 检查 ClickHouse 是否已存在该条记录，若无则插入。

## 维护者
- 建议使用 issue 反馈 bug 或需求。

---

如需扩展埋点类型或接入更多数据源，可参考 `analysisdb.go` 相关处理逻辑进行自定义开发。
