package cesstash

import (
	"math"
	"math/rand"
	"os"
	"time"
	"vdo-cmps/pkg/cesstash/shim"
	"vdo-cmps/pkg/cesstash/shim/segment"

	cesspat "github.com/CESSProject/cess-go-sdk/core/pattern"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/pkg/errors"
	"github.com/vedhavyas/go-subkey/v2"
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
			cleanChunks(t.fsm.HomeDir())
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

func (t *SimpleRelayHandler) upload(fsm *segment.FileSegmentMeta) error {
	if len(fsm.Segments) == 0 {
		return errors.New("the fsm fragments empty")
	}

	cessc := t.fileStash.cessc
	cessfsc := t.fileStash.cessfsc
	log := t.log
	cessFileId := fsm.RootHash.Hex()
	frag2dArray := fsm.ToFragSeg2dArray()

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

	hasDeclared := false
	for {
		for i := 0; i < math.MaxInt; i++ {
			fmd, err := cessc.QueryFileMetadata(cessFileId)
			if err != nil {
				if err.Error() != cesspat.ERR_Empty {
					log.Error(err, "[QueryFileMetadata] failed, try again later")
					time.Sleep(1500 * time.Millisecond)
					continue
				}
				// new file to store
				log.Info("new file to store", "uploador", subkey.SS58Encode(t.accountId[:], 11330)) //TODO: use configed value
				break
			} else {
				log.Info("the file has stored on chain")
				for _, v := range fmd.Owner {
					if t.accountId == v.User {
						// the user has already stored the file before
						EmitStep(t, "thunder", map[string]any{"alreadyStored": true})
						return nil
					}
				}
				if !hasDeclared {
					// only record the relationship
					blockHash, err := t.Declaration(fsm, false)
					if err != nil {
						return errors.Wrapf(err, "[Declaration]")
					}
					EmitStep(t, "thunder", map[string]any{"blockHash": blockHash})
				}
				return nil
			}
		}

		fragStripOnChainMap := t.createFileStorageOrderIfAbsent(fsm, usedMiners)
		hasDeclared = true

		//TODO: Parallel uploading of data
		for i, fragStrip := range frag2dArray {
			if _, ok := fragStripOnChainMap[i]; ok {
				// this index fragStrip has been on chain, skip it
				continue
			}
		A_FRAG_STRIP:
			log.V(1).Info("connected miners count", "minerSize", len(minerPeers))
			minerPeerId := pickMiner()
			nestLog := log.WithValues("minerPeer", minerPeerId, "fragIndex", i)
			for j, frag := range fragStrip {
				nestLog := nestLog.WithValues("frag", frag, "segIndex", j)
				retry := 0
				for {
					nestLog.V(1).Info("frag uploading", "retry", retry)
					if err := cessfsc.WriteFileAction(minerPeerId, cessFileId, frag.FilePath()); err != nil {
						if err.Error() != "no addresses" {
							nestLog.Error(err, "[WriteFileAction] error, try again later", "retry", retry)
							if retry < 3 {
								time.Sleep(700 * time.Millisecond)
								retry++
								continue
							}
						}
						nestLog.Error(err, "[WriteFileAction] error, change to next miner")
						cessfsc.ReportNotAvailable(minerPeerId)
						goto A_FRAG_STRIP // one strip one miner
					}
					nestLog.V(1).Info("frag uploaded")
					break
				}

			}
		}
	}
}

func (t *SimpleRelayHandler) createFileStorageOrderIfAbsent(fsm *segment.FileSegmentMeta, usedMiners map[peer.ID]struct{}) (fragStripOnChainMap map[int]peer.ID) {
	cessFileId := fsm.RootHash.Hex()
	cessc := t.fileStash.cessc
	log := t.log
	fragStripOnChainMap = make(map[int]peer.ID)
	for i := 0; i < math.MaxInt; i++ {
		storageOrder, err := cessc.QueryStorageOrder(cessFileId)
		if err != nil {
			if err.Error() == cesspat.ERR_Empty {
				// new file storage order
				for j := 0; j < math.MaxInt; j++ {
					blockHash, err := t.Declaration(fsm, true)
					if err != nil {
						log.Error(err, "[Declaration] failed, try again later")
						time.Sleep(1500 * time.Millisecond)
						continue
					}
					t.log.Info("new file storage order", "blockHash", blockHash)
					EmitStep(t, "upload declared", map[string]any{"blockHash": blockHash})
					return
				}
			} else {
				log.Error(err, "[QueryStorageOrder] failed, try again later")
				time.Sleep(1500 * time.Millisecond)
				continue
			}
		} else {
			t.log.V(1).Info("storage completes", "storageCompletes", storageOrder.CompleteList)
			for _, e := range storageOrder.CompleteList {
				for j := 0; j < math.MaxInt; j++ {
					minerInfo, err := cessc.QueryStorageMiner(e.Miner[:])
					if err != nil {
						log.Error(err, "[QueryStorageMiner] failed, try again later")
						time.Sleep(1500 * time.Millisecond)
						continue
					}
					peerId, err := shim.ToPeerId(&minerInfo.PeerId)
					if err != nil {
						log.Error(err, "convert peerId error, skip it", "minerPeerId", minerInfo.PeerId)
						break
					}
					// the Index that define on chain is started from 1
					fragStripOnChainMap[int(e.Index)-1] = peerId
					usedMiners[peerId] = struct{}{}
					break
				}
			}
			break
		}
	}
	return
}
