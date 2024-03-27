package cesstash

import (
	"math/rand"
	"os"
	"time"
	"vdo-cmps/pkg/cesstash/shim/segment"

	"github.com/AstaFrode/go-libp2p/core/peer"
	cesspat "github.com/CESSProject/cess-go-sdk/core/pattern"
	"github.com/pkg/errors"
	"golang.org/x/exp/slices"
)

var (
	_ RelayHandler = &SimpleRelayHandler{}
)

type SimpleRelayHandler struct {
	*SimpleRelayHandlerBase

	forceUploadIfPending bool
}

func NewSimpleRelayHandler(fileStash *CessStash, fsm *segment.FileSegmentMeta, uploadReq UploadReq) SimpleRelayHandler {
	base := SimpleRelayHandlerBase{
		id:         *fsm.RootHash,
		state:      RelayState{FileHash: fsm.RootHash.Hex()},
		log:        logger.WithValues("cessFileId", fsm.RootHash),
		fileStash:  fileStash,
		fsm:        fsm,
		accountId:  uploadReq.AccountId,
		bucketName: uploadReq.BucketName,
	}
	drh := SimpleRelayHandler{
		forceUploadIfPending: uploadReq.ForceUploadIfPending,
	}
	drh.SimpleRelayHandlerBase = &base
	return drh
}

func (t *SimpleRelayHandler) Relay() (retErr error) {
	defer func() {
		if retErr != nil {
			EmitStep(t, _ABORT_STEP, retErr)
		} else {
			EmitStep(t, _FINISH_STEP)
			cleanChunks(t.fsm.OutputDir)
		}
	}()

	EmitStep(t, "bucketing")
	if _, err := t.createBucketIfAbsent(); err != nil {
		return errors.Wrap(err, "create bucket error")
	}

	EmitStep(t, "uploading")
	t.log.V(1).Info("the fsm", "fsm", t.fsm)
	if err := t.upload(t.fsm); err != nil {
		return err
	}

	return nil
}

func (t *SimpleRelayHandler) ReRelayIfAbort() bool {
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

func (t *SimpleRelayHandler) createBucketIfAbsent() (string, error) {
	_, err := t.fileStash.cessc.QueryBucketInfo(t.accountId[:], t.bucketName)
	if err != nil {
		t.log.Error(err, "get bucket info error")
		txHash, err := t.fileStash.cessc.CreateBucket(t.accountId[:], t.bucketName)
		if err != nil {
			t.log.Error(err, "create bucket failed", "bucketName", t.bucketName)
		}
		t.log.Info("create bucket", "txn", txHash)
		return txHash, err
	}
	return "", nil
}

func cleanChunks(chunkDir string) {
	err := os.RemoveAll(chunkDir)
	if err != nil {
		logger.Error(err, "remove chunk dir error")
	}
}

type LoopCtrlAction uint8

const (
	NONE LoopCtrlAction = iota
	RESTART
	FINISH
)

func (t *SimpleRelayHandler) upload(fsm *segment.FileSegmentMeta) error {
	if len(fsm.Segments) == 0 {
		return errors.New("the fsm fragments empty")
	}

	cessFileId := fsm.RootHash.Hex()
	cessc := t.fileStash.cessc
	cessfsc := t.fileStash.cessfsc
	log := t.log

	for {
		fmd, err := cessc.QueryFileMetadata(cessFileId)
		log.V(1).Info("got FileMetaData", "fmd", fmd, "err", err)
		if err == nil {
			// the same cessFileId file is already on chain
			for _, v := range fmd.Owner {
				if t.accountId == v.User {
					// the user has already upload the file before
					EmitStep(t, "thunder", map[string]any{"alreadyUploaded": true})
					return nil
				}
			}
			// only record the relationship
			txn, err := t.Declaration(fsm, false)
			if err != nil {
				return errors.Wrapf(err, "cessc.UploadDeclaration()")
			}
			EmitStep(t, "thunder", map[string]any{"txn": txn})
			return nil
		}

		storageOrder, err := cessc.QueryStorageOrder(cessFileId)
		if err != nil {
			if err.Error() == cesspat.ERR_Empty {
				txn, err := t.Declaration(fsm, true)
				if err != nil {
					return errors.Wrapf(err, "cessc.UploadDeclaration()")
				}
				EmitStep(t, "upload declared", map[string]any{"txn": txn})
			}
			// } else if storageOrder.User.User == t.account {
			// 	EmitStep(t.se, "thunder")
			// 	return nil
		}

		t.log.V(1).Info("storage order detail", "storageOrder", storageOrder)

		usedMiners := make(map[peer.ID]struct{}, segment.FRAG_SHARD_SIZE)
		minerPeers := make([]peer.ID, 0)

		var pickMiner = func() peer.ID {
			noAvailableMiner := false
			for {
				if len(minerPeers) == 0 || noAvailableMiner {
					minerPeers = cessfsc.GetConnectedPeers()
					if len(minerPeers) > 0 {
						rand.Shuffle(len(minerPeers), func(i, j int) { minerPeers[i], minerPeers[j] = minerPeers[j], minerPeers[i] })
					}
					log.V(1).Info("no available miner, 5 second later fetch again")
					time.Sleep(5 * time.Second)
					continue
				}
				for i, peerId := range minerPeers {
					if _, ok := usedMiners[peerId]; ok {
						continue
					}
					usedMiners[peerId] = struct{}{}
					minerPeers = slices.Delete(minerPeers, i, i)
					return peerId
				}
				noAvailableMiner = true
			}
		}

		for _, fragStrip := range fsm.ToFragSeg2dArray() {
		A_FRAG_STRIP:
			minerPeerId := pickMiner()
			for _, frag := range fragStrip {
				if err := cessfsc.WriteFileAction(minerPeerId, cessFileId, frag.FilePath); err != nil {
					if err.Error() != "no addresses" {
						log.Error(err, "WriteFileAction() error, try again later", "frag", frag, "minerPeerId", minerPeerId)
					} // only log none "no addresses" error
					//TODO: retry again
					goto A_FRAG_STRIP // one strip one miner
				}

			}
		}
		break
	}
	return nil
}
