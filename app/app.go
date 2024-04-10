package app

import (
	"math/rand"
	"vdo-cmps/config"
	"vdo-cmps/pkg/cesstash"
	"vdo-cmps/pkg/log"

	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/go-logr/logr"
	"github.com/pkg/errors"
	"github.com/urfave/cli"
	"github.com/vedhavyas/go-subkey/v2"
)

type CmpsApp struct {
	gin     *gin.Engine
	config  *config.AppConfig
	cestash *cesstash.CessStash
}

func setupGin(app *CmpsApp) error {
	gin.SetMode(gin.ReleaseMode)
	app.gin = gin.Default()
	app.gin.Use(gin.Logger())
	app.gin.Use(configedCors())
	addRoute(app)
	return nil
}

func configedCors() gin.HandlerFunc {
	config := cors.DefaultConfig()
	config.AllowAllOrigins = true
	config.AllowCredentials = true
	return cors.New(config)
}

func addRoute(app *CmpsApp) {
	g := app.gin
	g.POST("/bucket", app.CreateBucket)
	g.GET("/file-state/:fileHash", app.GetFileState)
	g.GET("/stored-miners/:fileHash", app.ListStoredMiners)
	g.PUT("/file", app.UploadFile)
	g.GET("/file/:fileHash", app.DownloadFile)
	g.DELETE("/file", app.DeleteFile)
	g.GET("/upload-progress/:uploadId", app.ListenerUploadProgress)
	g.GET("/upload-progress1", app.DebugUploadProgress)

	g.StaticFile("favicon.ico", "./static/favicon.ico")
	g.LoadHTMLFiles("./static/index.html")
	g.GET("/demo", func(c *gin.Context) {
		c.HTML(200, "index.html", nil)
	})
}

func buildCmpsApp(config *config.AppConfig) (*CmpsApp, error) {
	cesh, err := cesstash.New(config)
	if err != nil {
		return nil, errors.Wrap(err, "build filestash error")
	}
	cesh.SetStashWhenUpload(true)

	app := &CmpsApp{
		config:  config,
		cestash: cesh,
	}

	setupGin(app)
	go fetchStorageMinerLoop(cesh)
	return app, nil
}

var (
	App    *CmpsApp
	logger logr.Logger
)

func init() {
	logger = log.Logger.WithName("app")
}

func Run(c *cli.Context) {
	config, err := config.NewConfig(c.String("config"))
	if err != nil {
		panic(err)
	}
	a, err := buildCmpsApp(config)
	if err != nil {
		panic(err)
	}

	App = a
	addr := config.App.ListenerAddress
	logger.Info("http server listener on", "address", addr)
	go App.gin.Run(addr)

	signalHandle()
}

func signalHandle() {
	logger.Info("server startup success!")
	var ch = make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
	for {
		si := <-ch
		switch si {
		case syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT:
			logger.Info("stop the server process", "signal", si.String())

			logger.Info("server shutdown success!")
			return
		case syscall.SIGHUP:
		default:
			return
		}
	}
}

func fetchStorageMinerLoop(cesh *cesstash.CessStash) {
	for {
		list, err := cesh.CesSdkAdapter().QueryAllSminerAccount()
		if err != nil {
			logger.Error(err, "")
			time.Sleep(3 * time.Second)
			continue
		}
		logger.Info("active fetch miners", "minersCount", len(list))
		miners := make([]string, len(list))
		for i, a := range list {
			miners[i] = subkey.SS58Encode(a.ToBytes(), 11330)
		}
		rand.Shuffle(len(miners), func(i, j int) { miners[i], miners[j] = miners[j], miners[i] })
		_AllMiners = miners
		time.Sleep(time.Second * 600)
	}
}
