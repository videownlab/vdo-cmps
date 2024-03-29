package cesstash

import (
	"math/rand"
	"time"
	"vdo-cmps/pkg/cesstash/shim/segment"

	cesspat "github.com/CESSProject/cess-go-sdk/core/pattern"
	"github.com/pkg/errors"
)

var (
	_ RelayHandler = &MockedRelayHandler{}
)

type MockedRelayHandler struct {
	*SimpleRelayHandlerBase
}

func NewMockedRelayHandler(fileStash *CessStash, fsm *segment.FileSegmentMeta, uploadReq UploadReq) MockedRelayHandler {
	base := SimpleRelayHandlerBase{
		id:         *fsm.RootHash,
		state:      RelayState{FileHash: fsm.RootHash.Hex()},
		log:        logger.WithValues("cessFileId", fsm.RootHash),
		fileStash:  fileStash,
		fsm:        fsm,
		accountId:  uploadReq.AccountId,
		bucketName: uploadReq.BucketName,
	}
	return MockedRelayHandler{&base}
}

// ReRelayIfAbort implements RelayHandler.
func (t *MockedRelayHandler) ReRelayIfAbort() bool {
	s := t.State()
	if !s.IsAbort() || s.storedButTxFailed {
		return false
	}
	//TODO: no-brain retry now, to be fix it!
	t.log.Info("new round relay()", "prevRetryRounds", s.retryRounds, "prevCompleteTime", s.CompleteTime)

	t.stateMutex.Lock()
	s.retryRounds++
	s.CompleteTime = time.Time{}
	t.stateMutex.Unlock()

	t.Relay()
	return true
}

func (t *MockedRelayHandler) Relay() (retErr error) {
	defer func() {
		if retErr != nil {
			EmitStep(t, _ABORT_STEP, retErr)
		} else {
			EmitStep(t, _FINISH_STEP)
			cleanChunks(t.fsm.HomeDir())
		}
	}()

	EmitStep(t, "bucketing")
	time.Sleep(time.Duration(randInRange(2, 4)) * time.Second)

	EmitStep(t, "uploading")
	t.log.V(1).Info("the fsm", "fsm", t.fsm)
	cessc := t.fileStash.cessc
	cessFileId := t.fsm.RootHash.Hex()
	storageOrder, err := cessc.QueryStorageOrder(cessFileId)
	if err != nil {
		if err.Error() == cesspat.ERR_Empty {
			txn, err := t.Declaration(t.fsm, true)
			if err != nil {
				return errors.Wrapf(err, "cessc.UploadDeclaration()")
			}
			EmitStep(t, "upload declared", map[string]any{"txn": txn})
		}
	}
	t.log.V(1).Info("storage order detail", "storageOrder", storageOrder)

	time.Sleep(time.Duration(randInRange(5, 11)) * time.Second)

	return nil
}

func randInRange(min, max int) int {
	return rand.Intn(max-min+1) + min
}
