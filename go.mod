module github.com/nsqio/nsq

go 1.25.0

require (
	github.com/BurntSushi/toml v1.6.0
	github.com/bitly/go-hostpool v0.1.1
	github.com/bitly/timer_metrics v1.0.0
	github.com/blang/semver/v4 v4.0.0
	github.com/bmizerany/perks v0.0.0-20230307044200-03f9df79da1e
	github.com/golang/snappy v1.0.0
	github.com/judwhite/go-svc v1.2.1
	github.com/julienschmidt/httprouter v1.3.0
	github.com/mreiferson/go-options v1.0.0
	github.com/nsqio/go-diskqueue v1.1.0
	github.com/nsqio/go-nsq v1.1.0
)

require (
	github.com/stretchr/testify v1.11.1 // indirect
	golang.org/x/sys v0.45.0 // indirect
)

replace github.com/judwhite/go-svc => github.com/mreiferson/go-svc v1.2.2-0.20210815184239-7a96e00010f6
