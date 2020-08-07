# Kofa -- 基于kafka异步去中心化框架 qq:8788107

说明文档正在更新。。。


const ServiceName = "Kofa_"

func main() {

	k := kofa.NewServer(ServiceName, ServiceName, []string{"49.22.22.15:13900"})
	k.AddRouter("User", &User{})

	k.ListenRouter(k.GetServerId(), kofa.NewOffset)

	go func() {
		time.Sleep(time.Second * 10)
		err := k.Call(k.GetServerId(), "User.Login", []byte("hi kofa"))

		if err != nil {
			fmt.Println(err)
		}

	}()
	k.Serve()

}

type User struct {
}

func (u *User) Login(request kface.IRequest) {
	fmt.Println(request.GetProducer())
}

func (u *User) Reg(request kface.IRequest) {

}

