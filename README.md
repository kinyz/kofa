# Kofa -- 基于kafka异步去中心化框架

说明文档正在更新。。。qq:8788107


    const ServiceName = "Kofa_"

    func main() {

	k := kofa.NewServer(ServiceName, ServiceName, []string{"49.22.22.15:13900"})
	k.AddRouter("User", &User{})

	k.ListenRouter(k.GetServerId(), kofa.NewOffset)

	go func() {
		time.Sleep(time.Second * 10)
		err := k.Call(k.GetServerId(), "User.Login", []byte("hi kofa"))

		if err != nil {
			log.Println(err)
		}

	}()
	k.Serve()}

    type User struct {
    }

    func (u *User) Login(request kface.IRequest) {
	    log.Println(request.GetProducer())
    }

    func (u *User) Reg(request kface.IRequest) {

    }
