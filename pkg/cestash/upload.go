package cestash

import (
	"fmt"
	"io"
	"runtime/debug"

	"os"
	"path/filepath"

	"vdo-cmps/pkg/cestash/shim/segment"

	cesspat "github.com/CESSProject/cess-go-sdk/core/pattern"
	"github.com/centrifuge/go-substrate-rpc-client/v4/types"

	"github.com/pkg/errors"
)

func (t *Cestash) createFsm(file FileHeader, outputDir string) (*segment.FileSegmentMeta, error) {
	toUploadFile, err := file.Open()
	if err != nil {
		return nil, err
	}
	defer toUploadFile.Close()
	tmpFsmHome, err := os.MkdirTemp(outputDir, "fsm_tmp_*")
	if err != nil {
		return nil, err
	}

	fsm, err := segment.CreateByStream(toUploadFile, file.Size(), tmpFsmHome)
	if err != nil {
		return nil, errors.Wrap(err, "shard file error")
	}
	fsm.Name = file.Filename()

	normalizeDir := filepath.Join(outputDir, fmt.Sprintf("fsm-%s", fsm.RootHash.Hex()))
	fstat, _ := os.Stat(normalizeDir)
	// the same cessFileId file exist
	if fstat != nil {
		fsm.ChangeHomeDir(normalizeDir)
		os.RemoveAll(tmpFsmHome)
		return fsm, nil
	}

	if err := os.Rename(tmpFsmHome, normalizeDir); err == nil {
		fsm.ChangeHomeDir(normalizeDir)
	} else {
		t.log.Error(err, "rename fsm dir error")
	}

	if t.stashWhenUpload {
		_, err = toUploadFile.Seek(0, io.SeekStart)
		if err != nil {
			return nil, errors.Wrap(err, "seek file error when stash")
		}
		err = t.stashFile(fsm.RootHash.Hex(), toUploadFile, file.Filename())
		if err != nil {
			return nil, errors.Wrap(err, "stash file error")
		}
	}
	return fsm, nil
}

func checkHasAuthorizeToMe(ct *Cestash, acc types.AccountID) error {
	authTargets, err := ct.cesa.QueryAuthorizedAccounts(acc[:])
	if err != nil && err.Error() != cesspat.ERR_Empty {
		return errors.New("must to authorize to use your space first")
	}
	ka := ct.cesa.GetSignatureAcc()
	for _, at := range authTargets {
		if ka == at {
			return nil
		}
	}
	return errors.Errorf("please authorize to %s to use your space", ka)
}

func (t *Cestash) Upload(req UploadReq) (RelayHandler, error) {
	if err := checkHasAuthorizeToMe(t, req.AccountId); err != nil {
		return nil, err
	}
	fsm, err := t.createFsm(req.FileHeader, t.chunksDir)
	if err != nil {
		return nil, err
	}
	cessFileId := *fsm.RootHash
	rh := t.relayHandlers[cessFileId]
	if rh == nil {
		drh := NewSimpleRelayHandler(t, fsm, req)
		// drh := NewMockedRelayHandler(t, fsm, req)
		t.log.V(1).Info("allot relay handler", "cessFileId", cessFileId)
		rh = &drh
		t.relayHandlers[cessFileId] = rh
		go func() {
			t.relayHandlerPutChan <- rh
		}()
	}

	go func() {
		defer func() {
			if r := recover(); r != nil {
				err := r.(error)
				fmt.Printf("%+v\n", err)
				t.log.Error(err, "catch panic on relay process!", "stack", string(debug.Stack()))
			}
		}()
		rh.Relay()
	}()

	return rh, nil
}

func (t *Cestash) GetRelayHandler(fileHash FileHash) (RelayHandler, error) {
	rh, ok := t.relayHandlers[fileHash]
	if !ok {
		return nil, errors.New("relay handler not exists for upload")
	}
	return rh, nil
}

func (t *Cestash) AnyRelayHandler() <-chan RelayHandler {
	return t.relayHandlerPutChan
}
