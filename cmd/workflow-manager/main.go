package main

import (
	_ "expvar"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"path"
	"path/filepath"

	"github.com/pedro-r-marques/workflow/pkg/api"
	"github.com/pedro-r-marques/workflow/pkg/config"
	"github.com/pedro-r-marques/workflow/pkg/engine"
	"github.com/pedro-r-marques/workflow/pkg/mbus"
)

type options struct {
	DebugPort  int
	Port       int
	AMQPServer string
	Config     string
}

func (opt *options) Register() {
	execDir, err := filepath.Abs(filepath.Dir(os.Args[0]))
	if err != nil {
		log.Fatal(err)
	}

	flag.IntVar(&opt.DebugPort, "debug-port", 8090, "debug info port")
	flag.IntVar(&opt.Port, "port", 8080, "workflow-manager api port")
	// Example: "amqp://guest:guest@localhost:5672/"
	flag.StringVar(&opt.AMQPServer, "amqp-server", os.Getenv("AMQP_SERVER"), "AMQP server url")
	confPath := path.Join(execDir, "./etc/workflow-manager.conf")
	flag.StringVar(&opt.Config, "config", confPath, "Workflow configuration file")
}

func configure(wrkEngine engine.WorkflowEngine, mbus engine.MessageBus, workflows []config.Workflow) {
	for _, wconf := range workflows {
		for _, vhost := range wconf.VHosts {
			mbus.VHostInit(vhost)
		}
		if len(wconf.VHosts) == 0 {
			mbus.VHostInit("")
		}
		wrkEngine.Update(&wconf)
	}
}

func main() {
	var opt options
	opt.Register()
	flag.Parse()

	if opt.AMQPServer == "" {
		log.Fatal("amqp-server option required")
	}

	workflows, err := config.ParseConfig(opt.Config)
	if err != nil {
		log.Fatal(err)
	}

	// default handler
	if opt.DebugPort != 0 {
		go http.ListenAndServe(fmt.Sprintf(":%d", opt.DebugPort), nil)
	}

	mux := http.NewServeMux()

	mbus := mbus.NewRabbitMQBus(opt.AMQPServer)
	wrkEngine := engine.NewWorkflowEngine(mbus, nil)
	mbus.SetHandler(wrkEngine.OnEvent)

	configure(wrkEngine, mbus, workflows)

	apiServer := api.NewApiServer(wrkEngine)
	mux.Handle("/api/", apiServer)
	mux.Handle("/", http.FileServer(http.Dir("static/")))
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", opt.Port), mux))
}
