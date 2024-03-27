package app

import (
	"context"
	"math/rand"
	"vdo-cmps/config"
	"vdo-cmps/pkg/cesstash"
	"vdo-cmps/pkg/cesstash/shim/cesssc"
	"vdo-cmps/pkg/log"

	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/centrifuge/go-substrate-rpc-client/v4/signature"
	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/go-logr/logr"
	"github.com/pkg/errors"
	"github.com/urfave/cli"
	"github.com/vedhavyas/go-subkey/v2"

	cgs "github.com/CESSProject/cess-go-sdk"
	cespat "github.com/CESSProject/cess-go-sdk/core/pattern"
	cessdk "github.com/CESSProject/cess-go-sdk/core/sdk"
)

type CmpsApp struct {
	gin         *gin.Engine
	config      *config.AppConfig
	keyringPair *signature.KeyringPair
	cessc       cessdk.SDK
	cessstash   *cesstash.CessStash
}

func buildCesscc(cfg *config.CessSetting) (cessdk.SDK, error) {
	cc, err := cgs.New(
		context.Background(),
		cgs.Name("client"),
		cgs.ConnectRpcAddrs([]string{cfg.RpcUrl}),
		cgs.Mnemonic(cfg.SecretPhrase),
		cgs.TransactionTimeout(time.Second*10),
	)
	if err != nil {
		return nil, err
	}
	go fetchStorageMinerLoop(cc)
	return cc, nil
}

func buildCesssc(cfg *config.CessfscSetting, workDir string) (*cesssc.CessStorageClient, error) {
	return cesssc.New(cfg.P2pPort, workDir, cfg.BootAddrs, logger.WithName("cstorec"))
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
	kp, err := signature.KeyringPairFromSecret(config.Cess.SecretPhrase, 0)
	if err != nil {
		return nil, err
	}

	workDir := config.App.WorkDir
	if _, err := os.Stat(workDir); os.IsNotExist(err) {
		err = os.Mkdir(workDir, 0755)
		if err != nil {
			return nil, errors.Wrap(err, "make CMPS work dir error")
		}
	}

	c, err := buildCesscc(config.Cess)
	if err != nil {
		return nil, errors.Wrap(err, "build cess chain client error")
	}
	sc, err := buildCesssc(config.Cessfsc, workDir)
	if err != nil {
		return nil, errors.Wrap(err, "build cess storage client error")
	}
	fs, err := cesstash.NewFileStash(workDir, config.Cess.SecretPhrase, c, sc)
	if err != nil {
		return nil, errors.Wrap(err, "build filestash error")
	}
	fs.SetStashWhenUpload(true)

	app := &CmpsApp{
		config:      config,
		keyringPair: &kp,
		cessc:       c,
		cessstash:   fs,
	}

	setupGin(app)
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

func fetchStorageMinerLoop(cessc cessdk.SDK) {
	for {
		list, err := cessc.QueryAllSminerAccount()
		if err != nil {
			logger.Error(err, "")
			if errors.Is(cespat.ERR_RPC_CONNECTION, err) {
				time.Sleep(3 * time.Second)
				cessc.ReconnectRPC()
			}
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
