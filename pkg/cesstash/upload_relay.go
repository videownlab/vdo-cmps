package cesstash

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"os"
	"strings"
	"sync"
	"time"
	"vdo-cmps/pkg/cesstash/shim"
	"vdo-cmps/pkg/cesstash/shim/segment"

	cesspat "github.com/CESSProject/cess-go-sdk/core/pattern"
	"github.com/go-logr/logr"
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

	cessfsc := t.fileStash.cessfsc
	log := t.log
	cessFileId := fsm.RootHash.Hex()
	frag2dArray := fsm.ToFragSeg2dArray()

	usedMiners := make(map[peer.ID]struct{}, segment.FRAG_SHARD_SIZE)
	minerPeers := make([]peer.ID, 0)
	mu := sync.Mutex{}
	var pickMiner = func() peer.ID {
		mu.Lock()
		defer mu.Unlock()
		var excludeUsedMiners = func(miners []peer.ID) []peer.ID {
			r := make([]peer.ID, 0, len(miners))
			for _, peerId := range miners {
				if _, ok := usedMiners[peerId]; !ok {
					r = append(r, peerId)
				}
			}
			return r
		}
		for {
			needExcludeUsed := true
			if len(minerPeers) <= 0 {
				minerPeers = excludeUsedMiners(cessfsc.GetConnectedPeers())
				needExcludeUsed = false
				if len(minerPeers) > 0 {
					rand.Shuffle(len(minerPeers), func(i, j int) { minerPeers[i], minerPeers[j] = minerPeers[j], minerPeers[i] })
				} else {
					log.V(1).Info("no available miner, 5 second later fetch again")
					time.Sleep(5 * time.Second)
					continue
				}
			}
			if needExcludeUsed {
				minerPeers = excludeUsedMiners(minerPeers)
			}
			if len(minerPeers) <= 0 {
				continue
			}
			log.V(1).Info("available miners", "miners", len(minerPeers), "usedMiners", len(usedMiners))
			lastIndex := len(minerPeers) - 1
			selectedPeerId := minerPeers[lastIndex]
			minerPeers = slices.Delete(minerPeers, lastIndex, lastIndex)
			return selectedPeerId
		}
	}

	if t.onlyRecordFileOwnershipIfOnChain(fsm) {
		return nil
	}

	formerUnfinishOrder, _ := t.createFileStorageOrderIfAbsent(fsm)
	var hasAlreadyFinished = func(fragStripIndex int) *cesspat.MinerInfo {
		if formerUnfinishOrder != nil {
			for _, e := range formerUnfinishOrder.CompleteList {
				// the Index that define on chain is started from 1
				index := int(e.Index) - 1
				if index == fragStripIndex {
					return t.queryMinerInfoUntilSuccess(e.Miner[:])
				}
			}
		}
		return nil
	}

	ctx, cancelFunc := context.WithCancel(context.Background())
	var uploadFragStrip = func(fragStripIndex int, fragStrip []segment.Fragment, completeChan chan peer.ID, wg *sync.WaitGroup, logger logr.Logger) {
		defer wg.Done()
		const maxStripCompleteWaitTimeout = 18 * time.Second
		var lastUploadedTime time.Time
		nestLog := logger.WithValues("fragStripIndex", fragStripIndex, "segLength", len(fragStrip))
		nestLog.V(1).Info("begin upload frag strip")
		for {
		LOOP_0:
			select {
			case <-ctx.Done():
				nestLog.Info("storage order completed")
				return
			case peerId := <-completeChan:
				// this index fragStrip has been on chain, skip it
				nestLog.Info("frag strip completed", "peerId", peerId)
				return
			default:
				if !lastUploadedTime.IsZero() {
					if time.Since(lastUploadedTime) < maxStripCompleteWaitTimeout {
						time.Sleep(3 * time.Second)
						continue
					} else {
						nestLog.V(1).Info("frag strip complete wait timeout", "completeWaitTimeout", maxStripCompleteWaitTimeout)
					}
				}

				minerPeerId := pickMiner()
				usedMiners[minerPeerId] = struct{}{}
				nestLog = nestLog.WithValues("minerPeer", minerPeerId)
				nestLog.V(1).Info("pick a new miner")
				for j, frag := range fragStrip {
					nestLog := nestLog.WithValues("frag", frag, "segIndex", j)
					retry := 0
					for {
						nestLog.V(1).Info("frag uploading", "retry", retry)
						if err := cessfsc.WriteFileAction(minerPeerId, cessFileId, frag.FilePath()); err != nil {
							nestLog.Error(err, "[WriteFileAction] error, change to next miner")
							cessfsc.ReportNotAvailable(minerPeerId)
						} else {
							nestLog.V(1).Info("frag uploaded, wait for complete")
							lastUploadedTime = time.Now()
						}
						goto LOOP_0
					}
				}
			}
		}
	}

	var wg sync.WaitGroup
	var fragStripCompleteChanMap = make(map[int]chan peer.ID)
	startTime := time.Now()
	n := 0
	// Parallel uploading for each frag strip
	for i := range frag2dArray {
		if m := hasAlreadyFinished(i); m != nil {
			peerId, err := shim.ToPeerId(&m.PeerId)
			if err == nil {
				log.V(1).Info("frag strip has already completed, ignore upload", "fragStripIndex", i, "minerPeer", peerId)
				usedMiners[peerId] = struct{}{}
				continue
			}
			log.Error(err, "convert peerId error", "minerPeer", m.PeerId)
		}
		wg.Add(1)
		ch := make(chan peer.ID, 1)
		fragStripCompleteChanMap[i] = ch
		go uploadFragStrip(i, frag2dArray[i], ch, &wg, log)
		n++
	}
	go t.storageOrderCompleteQueryLoop(fsm, fragStripCompleteChanMap, cancelFunc)
	log.V(1).Info(fmt.Sprintf("%d frag strip upload task started", n))
	wg.Wait()
	log.Info("file storage complete", "cost", time.Since(startTime))
	return nil
}

func (t *SimpleRelayHandler) queryStorageOrderUntilSuccess(cessFileId string) *cesspat.StorageOrder {
	cessc := t.fileStash.cessc
	log := t.log
	for i := 0; i < math.MaxInt; i++ {
		storageOrder, err := cessc.QueryStorageOrder(cessFileId)
		if err != nil {
			if err.Error() == cesspat.ERR_Empty {
				//maybe complete
				return nil
			}
			log.Error(err, "[QueryStorageOrder] failed, try again later")
			time.Sleep(1500 * time.Millisecond)
			continue
		}
		return &storageOrder
	}
	panic("may not be happen here")
}

func (t *SimpleRelayHandler) queryMinerInfoUntilSuccess(minerAccountId []byte) *cesspat.MinerInfo {
	cessc := t.fileStash.cessc
	log := t.log
	for j := 0; j < math.MaxInt; j++ {
		minerInfo, err := cessc.QueryStorageMiner(minerAccountId)
		if err != nil {
			if err.Error() == cesspat.ERR_Empty {
				//maybe not exist
				return nil
			}
			log.Error(err, "[QueryStorageMiner] failed, try again later")
			time.Sleep(1500 * time.Millisecond)
			continue
		}
		return &minerInfo
	}
	panic("may not be happen here")
}

func (t *SimpleRelayHandler) storageOrderCompleteQueryLoop(fsm *segment.FileSegmentMeta, fragStripCompleteChanMap map[int]chan peer.ID, cancelFunc context.CancelFunc) {
	log := t.log
	defer func() {
		log.V(1).Info("storage order complete query loop exit")
	}()
	for {
		storageOrder := t.queryStorageOrderUntilSuccess(fsm.RootHash.Hex())
		if storageOrder == nil {
			//the storage order complete
			log.V(1).Info("query the storage order return empty, storage complete")
			cancelFunc()
			return
		}
		// log.V(1).Info("xxx", "completeList", storageOrder.CompleteList)
		for _, e := range storageOrder.CompleteList {
			minerInfo := t.queryMinerInfoUntilSuccess(e.Miner[:])
			if minerInfo == nil {
				continue
			}
			peerId, err := shim.ToPeerId(&minerInfo.PeerId)
			if err != nil {
				log.Error(err, "convert peerId error, skip it", "minerPeerId", minerInfo.PeerId)
				continue
			}
			// the Index that define on chain is started from 1
			index := int(e.Index) - 1
			if ch, ok := fragStripCompleteChanMap[index]; ok {
				ch <- peerId
				delete(fragStripCompleteChanMap, index)
			}
		}
		time.Sleep(6 * time.Second)
	}
}

func (t *SimpleRelayHandler) onlyRecordFileOwnershipIfOnChain(fsm *segment.FileSegmentMeta) bool {
	cessFileId := fsm.RootHash.Hex()
	cessc := t.fileStash.cessc
	log := t.log
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
			return false
		} else {
			log.Info("the file has stored on chain")
			for _, v := range fmd.Owner {
				if t.accountId == v.User {
					// the user has already stored the file before
					EmitStep(t, "thunder", map[string]any{"alreadyStored": true})
					return true
				}
			}
			// only record the relationship
			blockHash, err := t.Declaration(fsm, false)
			if err != nil {
				log.Error(err, "[Declaration] on onlyRecordFileOwnershipIfOnChain(), try again later")
				time.Sleep(1500 * time.Millisecond)
				continue
			}
			EmitStep(t, "thunder", map[string]any{"blockHash": blockHash})
			return true
		}
	}
	return false
}

func (t *SimpleRelayHandler) createFileStorageOrderIfAbsent(fsm *segment.FileSegmentMeta) (*cesspat.StorageOrder, *string) {
	cessFileId := fsm.RootHash.Hex()
	cessc := t.fileStash.cessc
	log := t.log
	for i := 0; i < math.MaxInt; i++ {
		storageOrder, err := cessc.QueryStorageOrder(cessFileId)
		if err != nil {
			if err.Error() == cesspat.ERR_Empty {
				// new file storage order
				for j := 0; j < math.MaxInt; j++ {
					blockHash, err := t.Declaration(fsm, true)
					if err != nil {
						log.Error(err, "[Declaration] failed, try again later")
						if strings.Contains(err.Error(), "rpc err: connection failed") {
							log.V(1).Info("reconnect chain")
							err = cessc.ReconnectRPC() //FIXME: the cess sdk make the state false once catch rpc error
							if err != nil {
								log.Error(err, "[ReconnectRPC] failed, try again later")
								time.Sleep(1500 * time.Millisecond)
							}
						} else {
							time.Sleep(1500 * time.Millisecond)
						}
						continue
					}
					t.log.Info("new file storage order", "blockHash", blockHash)
					EmitStep(t, "upload declared", map[string]any{"blockHash": blockHash})
					return nil, &blockHash
				}
			} else {
				log.Error(err, "[QueryStorageOrder] failed, try again later")
				time.Sleep(1500 * time.Millisecond)
				continue
			}
		}
		return &storageOrder, nil
	}
	panic("may not be happen here")
}
