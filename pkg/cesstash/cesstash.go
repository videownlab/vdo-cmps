package cesstash

import (
	"context"
	"encoding/json"
	"io"

	"vdo-cmps/config"
	"vdo-cmps/pkg/cesstash/shim/cesssc"
	"vdo-cmps/pkg/log"
	"vdo-cmps/pkg/utils/hash"

	"os"
	"path/filepath"
	"time"

	cgs "github.com/CESSProject/cess-go-sdk"
	cesspat "github.com/CESSProject/cess-go-sdk/core/pattern"
	"github.com/centrifuge/go-substrate-rpc-client/v4/signature"
	"github.com/go-logr/logr"
	"github.com/pkg/errors"
)

var logger logr.Logger

func init() {
	logger = log.Logger.WithName("cesstash")
}

type CessStash struct {
	fileStashDir    string
	chunksDir       string
	keyringPair     signature.KeyringPair
	cessc           CesSdkAdapter
	cessfsc         *cesssc.CessStorageClient
	log             logr.Logger
	stashWhenUpload bool

	relayHandlers       map[FileHash]RelayHandler
	relayHandlerPutChan chan RelayHandler
}

type FileHash = hash.H256

const (
	_FileStashDirName = "stashs"
	_DataFilename     = "data"
	_MetaFilename     = "meta.json"
)

var Must _must

type _must struct {
}

func (t _must) New(config *config.AppConfig) *CessStash {
	fs, err := New(config)
	if err != nil {
		panic(err)
	}
	return fs
}

func New(config *config.AppConfig) (*CessStash, error) {
	cessCfg := config.Cess
	kp, err := signature.KeyringPairFromSecret(cessCfg.SecretPhrase, cessCfg.ChainId)
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
	fsd := filepath.Join(workDir, _FileStashDirName)
	if err := os.MkdirAll(fsd, 0755); err != nil {
		return nil, err
	}
	ckd := filepath.Join(workDir, _FileStashDirName, ".chunks")
	if err := os.MkdirAll(ckd, 0755); err != nil {
		return nil, err
	}

	cc, err := cgs.New(
		context.Background(),
		cgs.Name("client"),
		cgs.ConnectRpcAddrs([]string{cessCfg.RpcUrl}),
		cgs.Mnemonic(cessCfg.SecretPhrase),
		cgs.TransactionTimeout(time.Second*10),
	)
	if err != nil {
		return nil, err
	}

	cesfsCfg := config.Cessfsc
	cesfsc, err := cesssc.New(cesfsCfg.P2pPort, workDir, cesfsCfg.BootAddrs, logger.WithName("cstorec"))
	if err != nil {
		return nil, err
	}

	fsth := &CessStash{
		log:                 logger,
		fileStashDir:        fsd,
		chunksDir:           ckd,
		keyringPair:         kp,
		cessc:               CesSdkAdapter{cc},
		cessfsc:             cesfsc,
		relayHandlers:       make(map[FileHash]RelayHandler),
		relayHandlerPutChan: make(chan RelayHandler),
	}
	startCleanCompleteRelayHandlerTask(fsth)
	return fsth, nil
}

func startCleanCompleteRelayHandlerTask(fsth *CessStash) {
	go func() {
		for {
			for k, rh := range fsth.relayHandlers {
				if rh.IsProcessing() {
					continue
				}
				if rh.CanClean() {
					rh.Close()
					delete(fsth.relayHandlers, k)
					fsth.log.Info("clean relay handler", "uploadId", rh.Id())
				}
				time.Sleep(time.Second)
			}
			time.Sleep(time.Second)
		}
	}()
}

func (t *CessStash) CesSdkAdapter() *CesSdkAdapter { return &t.cessc }

func (t *CessStash) Dir() string { return t.fileStashDir }

func (t *CessStash) SetStashWhenUpload(value bool) { t.stashWhenUpload = value }

type FileBriefInfo struct {
	OriginName string
	CessFileId string
	FileHash   string
	FilePath   string
	Size       int64
}

type SimpleFileMeta struct {
	FileHash   string `json:"fileHash"`
	CessFileId string `json:"cessFileId"`
	OriginName string `json:"originName"`
}

func (t *CessStash) FileInfoById(cessFileId string) (*FileBriefInfo, error) {
	dataFilename := filepath.Join(t.fileStashDir, cessFileId, _DataFilename)
	fstat, err := os.Stat(dataFilename)
	if err != nil {
		return nil, err
	}
	r := FileBriefInfo{
		OriginName: cessFileId,
		CessFileId: cessFileId,
		FilePath:   dataFilename,
		Size:       fstat.Size(),
	}
	sfm, err := t.loadSimpleFileMeta(cessFileId)
	if sfm != nil && sfm.OriginName != "" {
		r.OriginName = sfm.OriginName
	}
	return &r, err
}

func (t *CessStash) loadSimpleFileMeta(cessFileId string) (*SimpleFileMeta, error) {
	metabs, err := os.ReadFile(filepath.Join(t.fileStashDir, cessFileId, _MetaFilename))
	if err != nil {
		return nil, err
	}

	var sfm SimpleFileMeta
	if err := json.Unmarshal(metabs, &sfm); err != nil {
		return nil, err
	}
	return &sfm, nil
}

func (t *CessStash) storeSimpleFileMeta(sfm *SimpleFileMeta) error {
	if sfm.CessFileId == "" {
		return errors.New("fileHash field must not be empty")
	}
	bytes, err := json.Marshal(sfm)
	if err != nil {
		return err
	}
	metaFilename := filepath.Join(t.fileStashDir, sfm.CessFileId, _MetaFilename)
	if err := os.WriteFile(metaFilename, bytes, os.ModePerm); err != nil {
		return err
	}
	return nil
}

func (t *CessStash) ensureCessFileDir(dirname string) (string, error) {
	fileHashDir := filepath.Join(t.fileStashDir, dirname)
	if _, err := os.Stat(fileHashDir); os.IsNotExist(err) {
		err = os.Mkdir(fileHashDir, 0755)
		if err != nil {
			return "", errors.Wrap(err, "make cess file dir error")
		}
	}
	return fileHashDir, nil
}

func (t *CessStash) createEmptyCessFile(cessFileId string) (*os.File, error) {
	dir, err := t.ensureCessFileDir(cessFileId)
	if err != nil {
		return nil, err
	}
	return os.Create(filepath.Join(dir, "data"))
}

func (t *CessStash) DownloadFile(cessFileId string) (*FileBriefInfo, error) {
	fmeta, err := t.cessc.QueryFileMetadata(cessFileId)
	if err != nil {
		return nil, err
	}
	t.log.V(1).Info("download file", "fileMeta", fmeta)

	if int(fmeta.State) != cesspat.Active {
		return nil, errors.New("the file is pending")
	}

	return t.downloadFile(cessFileId, &fmeta)
}

func (t *CessStash) RemoveFile(cessFileId string) error {
	fileHashDir := filepath.Join(t.fileStashDir, cessFileId)
	if _, err := os.Stat(fileHashDir); os.IsNotExist(err) {
		return nil
	}
	return os.RemoveAll(fileHashDir)
}

func (t *CessStash) stashFile(cessFileId string, src io.Reader, originName string) error {
	fhdf, err := t.createEmptyCessFile(cessFileId)
	if err != nil {
		return err
	}
	if _, err := io.Copy(fhdf, src); err != nil {
		return err
	}

	return t.storeSimpleFileMeta(&SimpleFileMeta{
		CessFileId: cessFileId,
		OriginName: filepath.Base(originName),
	})
}
