package main

import (
	_ "expvar"
	"flag"
	"fmt"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/pedro-r-marques/workflow/pkg/api"
	"github.com/pedro-r-marques/workflow/pkg/config"
	"github.com/pedro-r-marques/workflow/pkg/engine"
	"github.com/pedro-r-marques/workflow/pkg/mbus"
	"github.com/pedro-r-marques/workflow/pkg/store"
)

type options struct {
	DebugPort  int
	Port       int
	AMQPServer string
	Config     string
	HTMLDir    string
	Database   string
	Debug      bool
}

func (opt *options) Register() {
	execDir, err := filepath.Abs(filepath.Dir(os.Args[0]))
	if err != nil {
		log.Fatal().Err(err)
	}

	flag.IntVar(&opt.DebugPort, "debug-port", 8090, "debug info port")
	flag.IntVar(&opt.Port, "port", 8080, "workflow-manager api port")
	// Example: "amqp://guest:guest@localhost:5672/"
	flag.StringVar(&opt.AMQPServer, "amqp-server", os.Getenv("AMQP_SERVER"), "AMQP server url")
	confPath := path.Join(execDir, "./etc/workflow-manager.conf")
	flag.StringVar(&opt.Config, "config", confPath, "Workflow configuration file")
	flag.StringVar(&opt.HTMLDir, "html", path.Join(execDir, "static"), "Directory containing html/js debug UI")
	flag.StringVar(&opt.Database, "db", "", "Job status Database")
	flag.BoolVar(&opt.Debug, "debug", false, "Enable debug level logging")
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

	if opt.Debug {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	}

	if opt.AMQPServer == "" {
		log.Fatal().Msg("amqp-server option required")
	}

	workflows, err := config.ParseConfig(opt.Config)
	if err != nil {
		log.Fatal().Err(err)
	}

	// default handler
	if opt.DebugPort != 0 {
		go http.ListenAndServe(fmt.Sprintf(":%d", opt.DebugPort), nil)
	}

	mux := http.NewServeMux()

	mbus := mbus.NewRabbitMQBus(opt.AMQPServer)
	var storage engine.JobStore
	if opt.Database != "" {
		switch {
		case strings.HasPrefix(opt.Database, "sqlite3:"):
			filename := opt.Database[len("sqlite3:"):]
			storage, err = store.NewSqliteStore(filename)
			if err != nil {
				log.Fatal().Err(err)
			}
		default:
			log.Fatal().Msgf("unsupported database uri: %s", opt.Database)
		}
	}

	wrkEngine := engine.NewWorkflowEngine(mbus, storage)
	mbus.SetHandler(wrkEngine.OnEvent)

	configure(wrkEngine, mbus, workflows)

	if storage != nil {
		wrkEngine.RecoverRunningJobs()
	}

	apiServer := api.NewApiServer(wrkEngine, storage)
	mux.Handle("/api/", apiServer)
	mux.Handle("/", http.FileServer(http.Dir(opt.HTMLDir)))
	log.Fatal().Err(http.ListenAndServe(fmt.Sprintf(":%d", opt.Port), mux))
}
