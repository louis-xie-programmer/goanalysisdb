package main

import (
	"analysisdb/config"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/url"
	"strings"
	"time"

	"github.com/mssola/user_agent"
	"github.com/oschwald/geoip2-golang"
	"github.com/segmentio/kafka-go"
	"github.com/spf13/viper"
	"github.com/uptrace/go-clickhouse/ch"
)

// 页面浏览日志结构
// vtype: 0,1,2 window.performance.navigation.type
type PageViewLog struct {
	ch.CHModel `ch:"table:pageviewlogs,partition:toYYYYMM(toDateTime(time))"`

	PId     string //页面id
	WebName string `ch:",lc"` //网站客户端名称
	Host    string `ch:",lc"` //网站域名
	Url     string `ch:",lc"` //网页链接
	Title   string //页面标题
	Status  int    //页面状态码

	MId        string //机器识别码
	MType      string //机器类型
	UA         string //useragen 代理
	AutoUA     bool   //是否是模拟的ua
	System     string `ch:",lc"` //系统名称
	Screen     string `ch:",lc"` //屏幕
	Browser    string `ch:",lc"` //浏览器（包括爬虫标识）
	BrowserVer string //浏览器版本
	Proto      string `ch:",lc"` //http 版本（http 1.1 , h2, h3）
	Robot      bool

	Lang      string //语言
	IP        string //ip
	Continent string //所属洲
	Country   string //国家
	Provinces string //省或州
	City      string //城市
	Location  string //坐标（纬度，经度）

	Sessionid string //会话识别码
	Depth     int    //流量深度
	Referer   string `ch:",lc"` //上一页面
	Viewtype  string //浏览类型

	Time int64 //时间
}

type PageView struct {
	PId     string `json:"i"` //页面id
	WebName string //网站客户端名称
	Url     string `json:"u"` //网页链接
	Title   string `json:"t"` //页面标题
	Status  int    `json:"s"` //页面状态码

	MId    string `json:"m"` //机器识别码
	UA     string //useragen 代理
	AutoUA bool   `json:"w"`  //是否是模拟的ua
	Screen string `json:"sc"` //屏幕
	Proto  string `json:"p"`  //http 版本（http 1.1 , h2, h3）

	Lang string //语言
	IP   string //ip

	Sessionid string //会话识别码
	Depth     int    //流量深度,只有在viewtype为正常访问才加深度
	Referer   string `json:"r"` //上一页面
	Viewtype  string `json:"v"` //浏览类型(需要对客户端发来的window.performance.navigation.type值进行语义化)

	Time int64 //时间
}

// 事件日志结构
type Eventlog struct {
	ch.CHModel `ch:"table:eventlogs,partition:toYYYYMM(toDateTime(time))"`

	Id      string //事件标识
	EType   string `ch:",lc"` //事件类型
	WebName string `ch:",lc"` //网页名称
	Url     string `ch:",lc"` //当前网页链接地址
	PId     string //浏览标识
	JsonDb  string `ch:",json_use_number"` //事件内容
	Time    int64  //时间
}

type EventlogModel struct {
	Id      string //事件标识
	EType   string `json:"e"` //事件类型
	WebName string //网页名称
	Url     string `json:"u"` //当前网页链接地址
	PId     string `json:"i"` //pageview id
	JsonDb  string `json:"c"` //事件内容
	Time    int64  //时间
}

// 事件日志结构
type EventL struct {
	Id      string //事件标识
	EType   string `json:"e"` //事件类型
	WebName string //网页名称
	Url     string `json:"u"` //当前网页链接地址
	PId     string `json:"i"` //pageview id
	JsonDb  string `json:"c"` //事件内容
	Time    int64  //时间
}

type Client struct {
	host     string
	instance *kafka.Conn
}

type CKClient struct {
	addr     string
	instance *ch.DB
}

// Connect 连接kafka服务
func (client *Client) Connect(topic string) {
	conn, err := kafka.DialLeader(context.Background(), "tcp", client.host, topic, 0)
	if err != nil {
		log.Println("failed to dial learder: " + err.Error())
	}

	//_ = conn.SetWriteDeadline(time.Now().Add(10 * time.Second))

	client.instance = conn
}

func (ckclient *CKClient) Connect() {
	pwd := viper.GetString("common.ch.pwd")

	if len(strings.TrimSpace(pwd)) == 0 {
		db := ch.Connect(
			ch.WithDSN("clickhouse://" + ckclient.addr + "?sslmode=disable"),
		)
		ckclient.instance = db
	} else {
		db := ch.Connect(
			ch.WithDSN("clickhouse://"+ckclient.addr+"?sslmode=disable"),
			ch.WithPassword(pwd),
		)
		ckclient.instance = db
	}
}

// NewClient 实例化
func NewClient(host string) *Client {
	return &Client{host: host}
}

func NewCKClient(addr string) *CKClient {
	return &CKClient{addr: addr}
}

type Consumer struct {
	client *Client
	db     *CKClient
}

// Receive 接收
func Receive(consumer *Consumer) {
	ctx := context.Background()

	res, err := consumer.db.instance.NewCreateTable().Model((*PageViewLog)(nil)).IfNotExists().
		Engine("MergeTree()").
		Order("time").
		Exec(ctx)

	if err != nil {
		log.Println(err)
	}

	res, err = consumer.db.instance.NewCreateTable().Model((*Eventlog)(nil)).IfNotExists().
		Engine("MergeTree()").
		Order("time").
		Exec(ctx)

	if err != nil {
		log.Println(err)
	}

	log.Println(res)

	// 保持监听
	for {
		message, err := consumer.client.instance.ReadMessage(10e3)
		if err != nil {
			log.Println("unable to read message:" + err.Error())
			time.Sleep(100 * time.Microsecond)
			consumer.client.Connect(viper.GetString("common.kafka.topic"))
			continue
		}
		// 接收到新消息
		//log.Println("receive message:", string(message.Key), string(message.Value))

		strkey := string(message.Key)

		if strkey == "pageview" {
			pageviewlog := RecervePageView(string(message.Value))

			plog := new(PageViewLog)

			err = consumer.db.instance.NewSelect().Model(plog).Where("pid = ?", pageviewlog.PId).Limit(1).Scan(context.Background())

			if err != nil {
				_, err = consumer.db.instance.NewInsert().Model(&pageviewlog).Exec(context.Background())

				if err != nil {
					log.Println("clickhouse insert pageviewlog error: ", err)
				}
			}
		}

		if strkey == "eventlog" {
			eventlog := RecerveEvenLog(string(message.Value))
			elog := new(Eventlog)

			err = consumer.db.instance.NewSelect().Model(elog).Where("id = ?", eventlog.Id).Limit(1).Scan(context.Background())

			if err != nil {
				log.Println(eventlog)
				_, err := consumer.db.instance.NewInsert().Model(&eventlog).Exec(context.Background())

				if err != nil {
					log.Println("clickhouse insert eventlog error: ", err)
				}
			}
		}

		time.Sleep(10 * time.Millisecond)
	}
}

// 分析ua
func analysisua(p *PageViewLog, str string) {

	ua := user_agent.New(str)

	p.System = ua.OSInfo().Name
	p.Browser, p.BrowserVer = ua.Browser()
	if ua.Mobile() {
		p.MType = "mobile"
	} else {
		ua.Platform()
	}
	p.Robot = ua.Bot()
}

func gethost(u string) string {
	url, err := url.Parse(u)
	if err != nil {
		return ""
	}
	return url.Hostname()
}

func analysisip(p *PageViewLog, ipaddr string) {
	db, err := geoip2.Open("GeoLite2-City.mmdb")

	if err != nil {
		log.Println("analysisip geoip2 error:" + err.Error())
	}
	defer db.Close()

	ip := net.ParseIP(ipaddr)

	record, err := db.City(ip)

	if err != nil {
		log.Println("analysisip city " + ipaddr + " error:" + err.Error())
	}

	p.Continent = record.Continent.Names["en"]
	p.Country = record.Country.Names["en"]

	if record.Subdivisions != nil {
		p.Provinces = record.Subdivisions[0].Names["en"]
	}

	p.City = record.City.Names["en"]
	p.Location = fmt.Sprintf("%f", record.Location.Latitude) + "," + fmt.Sprintf("%f", record.Location.Longitude)
}

func RecervePageView(str string) PageViewLog {
	pageview := PageView{}
	err := json.Unmarshal([]byte(str), &pageview)
	if err != nil {
		log.Println("RecervePageView json error:" + err.Error())
	}

	pageviewlog := PageViewLog{}

	pageviewlog.PId = pageview.PId
	pageviewlog.WebName = pageview.WebName
	pageviewlog.Url = pageview.Url
	pageviewlog.Title = pageview.Title
	pageviewlog.Host = gethost(pageview.Url)
	pageviewlog.Status = pageview.Status

	pageviewlog.MId = pageview.MId
	pageviewlog.UA = pageview.UA
	pageviewlog.AutoUA = pageview.AutoUA

	pageviewlog.Screen = pageview.Screen
	pageviewlog.Proto = pageview.Proto

	pageviewlog.Lang = pageview.Lang
	pageviewlog.IP = pageview.IP

	pageviewlog.Sessionid = pageview.Sessionid
	pageviewlog.Depth = pageview.Depth
	pageviewlog.Referer = pageview.Referer
	pageviewlog.Viewtype = pageview.Viewtype

	pageviewlog.Time = pageview.Time

	analysisua(&pageviewlog, pageview.UA)
	analysisip(&pageviewlog, pageview.IP)

	return pageviewlog
}

func RecerveEvenLog(str string) Eventlog {
	eventlogmodel := EventlogModel{}
	err := json.Unmarshal([]byte(str), &eventlogmodel)
	if err != nil {
		log.Println("RecerveEvenLog json error:" + err.Error())
	}

	eventlog := Eventlog{}

	eventlog.Id = eventlogmodel.Id
	eventlog.EType = eventlogmodel.EType
	eventlog.WebName = eventlogmodel.WebName
	eventlog.PId = eventlogmodel.PId
	eventlog.Url = eventlogmodel.Url
	eventlog.JsonDb = eventlogmodel.JsonDb
	eventlog.Time = eventlogmodel.Time

	return eventlog
}

func main() {
	if err := config.Init(); err != nil {
		panic(err)
	}
	//kafka 连接
	kafkaClient := NewClient(viper.GetString("common.kafka.addr"))
	db := NewCKClient(viper.GetString("common.ch.addr"))

	kafkaClient.Connect(viper.GetString("common.kafka.topic"))
	db.Connect()

	consumer := Consumer{
		client: kafkaClient,
		db:     db,
	}

	Receive(&consumer)
}
