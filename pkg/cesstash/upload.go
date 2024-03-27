package cesstash

import (
	"fmt"
	"io"
	"runtime/debug"

	"os"
	"path/filepath"

	"vdo-cmps/pkg/cesstash/shim/segment"

	"github.com/pkg/errors"
)

func (t *CessStash) createFsm(file FileHeader, outputDir string) (*segment.FileSegmentMeta, error) {
	toUploadFile, err := file.Open()
	if err != nil {
		return nil, err
	}
	defer toUploadFile.Close()
	fsmHome, err := os.MkdirTemp(outputDir, "fsm_tmp_*")
	if err != nil {
		return nil, err
	}

	fsm, err := segment.CreateByStream(toUploadFile, file.Size(), fsmHome)
	if err != nil {
		return nil, errors.Wrap(err, "shard file error")
	}
	fsm.Name = file.Filename()

	normalizeDir := filepath.Join(outputDir, fmt.Sprintf("fsm-%s", fsm.RootHash.Hex()))
	fstat, _ := os.Stat(normalizeDir)
	// the same cessFileId file exist
	if fstat != nil {
		fsm.OutputDir = normalizeDir
		os.RemoveAll(fsmHome)
		return fsm, nil
	}

	if err := os.Rename(fsmHome, normalizeDir); err == nil {
		fsm.OutputDir = normalizeDir
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

func (t *CessStash) Upload(req UploadReq) (RelayHandler, error) {
	fsm, err := t.createFsm(req.FileHeader, t.chunksDir)
	if err != nil {
		return nil, err
	}
	cessFileId := *fsm.RootHash
	rh := t.relayHandlers[cessFileId]
	if rh == nil {
		// drh := NewSimpleRelayHandler(t, fsm, req)
		drh := NewMockedRelayHandler(t, fsm, req)
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

func (t *CessStash) GetRelayHandler(fileHash FileHash) (RelayHandler, error) {
	rh, ok := t.relayHandlers[fileHash]
	if !ok {
		return nil, errors.New("relay handler not exists for upload")
	}
	return rh, nil
}

func (t *CessStash) AnyRelayHandler() <-chan RelayHandler {
	return t.relayHandlerPutChan
}
