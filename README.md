# Kofa - 去中心化框架


**使用方法**
- 创建一个服务

		s := kofa.New(
		"Test", //服务名
		true, //是否加入组
		prehandle.Kafka([]string{"10.43.123.172:9092"}, prehandle.NewOffset)//通信接口 目前使用kafka 可自定义
		//可增加自定义服务发现模块 框架提供内置
		)

- 创建一个结构体并提供服务方法

		type Account struct {}

		func (u *Account) Oauth(request kofa.Request) {

		log.Println("call logout oauth:", time.Now().UnixNano()-request.Message().Header().GetTimesTamp())

		}
		func (u *Account) Logout(request kofa.Request) {

		log.Println("call logout us:", time.Now().UnixNano()-request.Message().Header().GetTimesTamp())
		request.Message().Header().SetMsgId(3001)
		request.Send(request.Message())

		request.Send(request.Message())
		}

- 将结构体添加到服务路由中

		s.AddRouter(
		2000,// msgId kofa会根据结构体到方法生成msgId
		"Account", //路由别名 alias 
		5, //服务级别
		&Account{},//前面定义好的结构体
		//可增加requet的参数
		)

- 启动服务

		s.Serve()



- **message通信包**

1. GetKey() 获取通信包key
2. GetData() 获取通信包data
3. SetKey(key string) 设置通信包key
4. SetData(data []byte) 设置通信包data
5. Header() 通信包头
  
>  GetMsgId() uint64 获取通信msgId

>	GetProducer() string 获取调用者

>	SetMsgId(msgId uint64) 设置通信msgId 

>	SetProducer(producer string) 设置调用者 目前在通信接口自动调用

>	GetTimesTamp() int64 获取时间戳

>	SetTimesTamp(UnixNano int64) 设置时间戳 目前在通信接口自动调用

>	Encode() ([]byte, error) encode pd为bytes 目前在通信接口自动调用

>	Decode(data []byte) error decode pd 目前在通信接口自动调用
  
6. Property() 通信属性

> Get(key string) ([]byte, error) 获取属性

>	Set(key string, value []byte) 设置属性

>	Remove(key string) error 删除属性

>	Clear() 清除所有属性

>	SetMap(propertyMap map[string][]byte) 设置属性map

> GetMap() map[string][]byte 获取属性map


		msg := message.NewMessage()
		msg.Header().SetMsgId(2003) //设置通信id
		msg.SetKey("im key") // key
		msg.SetData([]byte("im data"))//data
		msg.Property().Set("key1", []byte("data1"))//设置属性
		msg.Property().Set("key2", []byte("data2"))//设置属性
		err := s.Send(msg)//发送通信包
			if err != nil {
				log.Println("send msg err:", err)
			}

- **request回调参数**
1. message通信包
2. send方法 









