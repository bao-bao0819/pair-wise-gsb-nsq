package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sync"
	"syscall"

	"github.com/BurntSushi/toml"
	"github.com/judwhite/go-svc"
	"github.com/mreiferson/go-options"
	"github.com/nsqio/nsq/internal/lg"
	"github.com/nsqio/nsq/internal/version"
	"github.com/nsqio/nsq/nsqd"
)

type program struct {
	once sync.Once
	nsqd *nsqd.NSQD
}

func main() {
	prg := &program{}
	if err := svc.Run(prg, syscall.SIGINT, syscall.SIGTERM); err != nil {
		logFatal("%s", err)
	}
}

func (p *program) Init(env svc.Environment) error {
	opts := nsqd.NewOptions()

	flagSet := nsqdFlagSet(opts)
	if err := flagSet.Parse(os.Args[1:]); err != nil {
		return err
	}

	if flagSet.Lookup("version").Value.(flag.Getter).Get().(bool) {
		fmt.Println(version.String("nsqd"))
		os.Exit(0)
	}

	var cfg config
	configFile := flagSet.Lookup("config").Value.String()
	if configFile != "" {
		_, err := toml.DecodeFile(configFile, &cfg)
		if err != nil {
			logFatal("failed to load config file %s - %s", configFile, err)
		}
	}
	cfg.Validate()

	options.Resolve(opts, flagSet, cfg)
	applyBackwardCompatibility(opts, flagSet)

	nsqd, err := nsqd.New(opts)
	if err != nil {
		logFatal("failed to instantiate nsqd - %s", err)
	}
	p.nsqd = nsqd

	return nil
}

func (p *program) Start() error {
	err := p.nsqd.LoadMetadata()
	if err != nil {
		logFatal("failed to load metadata - %s", err)
	}
	err = p.nsqd.PersistMetadata()
	if err != nil {
		logFatal("failed to persist metadata - %s", err)
	}

	go func() {
		err := p.nsqd.Main()
		if err != nil {
			if stopErr := p.Stop(); stopErr != nil {
				logFatal("failed to stop nsqd - %s", stopErr)
			}
			os.Exit(1)
		}
	}()

	return nil
}

func (p *program) Stop() error {
	p.once.Do(func() {
		p.nsqd.Exit()
	})
	return nil
}

func (p *program) Handle(s os.Signal) error {
	return svc.ErrStop
}

// Context returns a context that will be canceled when nsqd initiates the shutdown
func (p *program) Context() context.Context {
	return p.nsqd.Context()
}

func logFatal(f string, args ...interface{}) {
	lg.LogFatal("[nsqd] ", f, args...)
}

// applyBackwardCompatibility applies backward compatibility rules to options after flag resolution
func applyBackwardCompatibility(opts *nsqd.Options, flagSet *flag.FlagSet) {
	// when max-defer-timeout was not explicitly set, refer to the max-req-timeout value
	if flag := flagSet.Lookup("max-defer-timeout"); flag != nil && flag.Value.String() == flag.DefValue {
		opts.MaxDeferTimeout = opts.MaxReqTimeout
	}

	// ... other backward compatibility rules can be added here
}
