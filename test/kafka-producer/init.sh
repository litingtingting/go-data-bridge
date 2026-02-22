
# 修改环境变量-国内需要
go env -w GOPROXY=https://goproxy.cn,direct


# 初始化 go mod
go mod init kafka-producer

# 获取依赖
go get github.com/IBM/sarama
