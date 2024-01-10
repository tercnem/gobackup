package database

import (
	"context"
	"fmt"
	"path"
	"strings"

	"github.com/google/shlex"
	"github.com/rgzr/sshtun"
	"github.com/spf13/viper"

	"github.com/gobackup/gobackup/config"
	"github.com/gobackup/gobackup/helper"
	"github.com/gobackup/gobackup/logger"
)

// Base database
type Base struct {
	model            config.ModelConfig
	dbConfig         config.SubConfig
	viper            *viper.Viper
	name             string
	dumpPath         string
	sshHost          string
	sshPort          int
	sshUser          string
	sshPassword      string
	sshKeyFile       string
	tunnelLocalPort  int
	tunnelRemotePort int
	tunnelDbHost     string
	cTunnelEnd       chan struct{}
	cTunnelStart     chan struct{}
}

// Database interface
type Database interface {
	// Init database config, prepare all things
	init() error
	// Peform backup
	perform() error
}

func newBase(model config.ModelConfig, dbConfig config.SubConfig) (base Base) {
	base = Base{
		model:    model,
		dbConfig: dbConfig,
		viper:    dbConfig.Viper,
		name:     dbConfig.Name,
	}
	base.sshHost = viper.GetString("ssh_host")
	base.sshPort = viper.GetInt("ssh_port")
	base.sshUser = viper.GetString("ssh_user")
	base.sshPassword = viper.GetString("ssh_password")
	base.sshKeyFile = viper.GetString("ssh_key_file")
	base.tunnelDbHost = viper.GetString("tunnel_db_host")
	base.tunnelRemotePort = viper.GetInt("tunnel_remote_port")
	base.tunnelLocalPort = viper.GetInt("tunnel_local_port")
	base.dumpPath = path.Join(model.DumpPath, dbConfig.Type, base.name)
	if err := helper.MkdirP(base.dumpPath); err != nil {
		logger.Errorf("Failed to mkdir dump path %s: %v", base.dumpPath, err)
		return
	}
	return
}

func runHook(action, script string) error {
	logger := logger.Tag("Database")
	if len(script) == 0 {
		return nil
	}
	logger.Infof("Run %s", action)
	ignoreError := strings.HasPrefix(script, "-")
	script = strings.TrimPrefix(script, "-")
	c, err := shlex.Split(script)
	if err != nil {
		if ignoreError {
			logger.Infof("Skip %s with error: %v", action, err)
		} else {
			return err
		}
	} else {
		if _, err := helper.Exec(c[0], c[1:]...); err != nil {
			if ignoreError {
				logger.Infof("Run %s failed: %v, ignore it", action, err)
			} else {
				return fmt.Errorf("Run %s failed: %v", action, err)
			}
		} else {
			logger.Infof("Run %s succeeded", action)
		}
	}

	return nil
}

// New - initialize Database
func runModel(model config.ModelConfig, dbConfig config.SubConfig) (err error) {
	logger := logger.Tag("Database")

	base := newBase(model, dbConfig)
	var db Database
	switch dbConfig.Type {
	case "mysql":
		db = &MySQL{Base: base}
	case "redis":
		db = &Redis{Base: base}
	case "postgresql":
		db = &PostgreSQL{Base: base}
	case "mongodb":
		db = &MongoDB{Base: base}
	case "sqlite":
		db = &SQLite{Base: base}
	case "mssql":
		db = &MSSQL{Base: base}
	case "influxdb2":
		db = &InfluxDB2{Base: base}
	default:
		logger.Warn(fmt.Errorf("model: %s databases.%s config `type: %s`, but is not implement", model.Name, dbConfig.Name, dbConfig.Type))
		return
	}

	logger.Infof("=> database | %v: %v", dbConfig.Type, base.name)

	// before perform
	beforeScript := dbConfig.Viper.GetString("before_script")
	if err := runHook("dump before_script", beforeScript); err != nil {
		return err
	}

	afterScript := dbConfig.Viper.GetString("after_script")
	onExit := dbConfig.Viper.GetString("on_exit")

	// perform
	if err = db.init(); err != nil {
		return
	}

	if base.sshHost != "" {
		base.openTunneling()
		<-base.cTunnelStart
	}

	err = db.perform()
	if base.sshHost != "" {
		base.closeTunneling()
	}
	if err != nil {
		logger.Info("Dump failed")
		if len(afterScript) == 0 {
			return
		} else if len(onExit) != 0 {
			switch onExit {
			case "always":
				logger.Info("on_exit is always, start to run after_script")
			case "success":
				logger.Info("on_exit is success, skip run after_script")
				return
			case "failure":
				logger.Info("on_exit is failure, start to run after_script")
			default:
				// skip after
				return
			}
		} else {
			return
		}
	} else {
		logger.Info("Dump succeeded")
	}

	// after perform
	if err := runHook("dump after_script", afterScript); err != nil {
		return err
	}

	return
}

func (db *Base) closeTunneling() {
	db.cTunnelEnd <- struct{}{}

}

func (db *Base) openTunneling() error {
	db.cTunnelEnd = make(chan struct{}, 1)
	db.cTunnelStart = make(chan struct{}, 1)

	logger := logger.Tag("TUNNELING")
	var err error
	sshTun := sshtun.New(db.tunnelLocalPort, db.sshHost, db.tunnelRemotePort)
	sshTun.SetPort(db.sshPort)
	sshTun.SetUser(db.sshUser)
	sshTun.SetPassword(db.sshPassword)
	if db.sshKeyFile != "" {
		sshTun.SetKeyFile(db.sshKeyFile)
	}
	//
	sshTun.SetRemoteEndpoint(sshtun.NewTCPEndpoint(db.tunnelDbHost, db.tunnelRemotePort))
	sshTun.SetLocalEndpoint(sshtun.NewTCPEndpoint("localhost", db.tunnelLocalPort))

	sshTun.SetTunneledConnState(func(tun *sshtun.SSHTun, state *sshtun.TunneledConnState) {
		logger.Infof("tunneling state %+v", state)
	})

	// We set a callback to know when the tunnel is ready
	sshTun.SetConnState(func(tun *sshtun.SSHTun, state sshtun.ConnState) {
		switch state {
		case sshtun.StateStarting:
			logger.Infof("Tunneling is Starting")
		case sshtun.StateStarted:
			logger.Infof("Tunneling is Started")
			db.cTunnelStart <- struct{}{}
		case sshtun.StateStopped:
			logger.Infof("Tunneling is Stopped")
		}
	})

	go func() {
		<-db.cTunnelEnd
		logger.Info("tunneling is Stop")
		sshTun.Stop()
	}()
	go func() {
		err = sshTun.Start(context.Background())
		if err != nil {
			logger.Info("error tunneling:", err)

			if len(db.cTunnelStart) > 0 {
				<-db.cTunnelStart
			}
			return
		}
	}()

	return err
}

// Run databases
func Run(model config.ModelConfig) error {
	if len(model.Databases) == 0 {
		return nil
	}

	for _, dbCfg := range model.Databases {
		err := runModel(model, dbCfg)
		if err != nil {
			return err
		}
	}

	return nil
}
