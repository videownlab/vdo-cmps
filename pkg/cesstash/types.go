package cesstash

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"
	"vdo-cmps/pkg/cesstash/shim/segment"
	"vdo-cmps/pkg/utils/hash"

	cesspat "github.com/CESSProject/cess-go-sdk/core/pattern"
	"github.com/centrifuge/go-substrate-rpc-client/v4/types"
	"github.com/go-logr/logr"
	"github.com/libp2p/go-libp2p/core/peer"
)

type UploadReq struct {
	FileHeader           FileHeader
	AccountId            types.AccountID
	BucketName           string
	ForceUploadIfPending bool
}

type PeerAccountPair struct {
	PeerId  peer.ID
	Account types.AccountID
}

type PeerSet = map[peer.ID]struct{}

type ShardedStep struct {
	CessFileId string `json:"cessFileId"`
}

const (
	_FINISH_STEP = "finish"
	_ABORT_STEP  = "abort"
)

type HandleStep struct {
	Step  string
	Msg   string
	Data  any
	Error error
}

func (t HandleStep) IsComplete() bool { return t.Step == _FINISH_STEP }
func (t HandleStep) IsAbort() bool    { return t.Error != nil }
func (t HandleStep) String() string {
	b, err := json.Marshal(t)
	if err != nil {
		return fmt.Sprintf("step:%s <json marshal error:%v>", t.Step, err)
	}
	return fmt.Sprint("progress", string(b))
}
func (t HandleStep) MarshalJSON() ([]byte, error) {
	type tmpType struct {
		Step  string `json:"step,omitempty"`
		Msg   string `json:"msg,omitempty"`
		Data  any    `json:"data,omitempty"`
		Error string `json:"error,omitempty"`
	}
	tt := tmpType{
		Step: t.Step,
		Msg:  t.Msg,
		Data: t.Data,
	}
	if t.Error != nil {
		tt.Error = t.Error.Error()
	}
	return json.Marshal(&tt)
}

type RelayState struct {
	FileHash          string       `json:"fileHash,omitempty"`
	Miners            []string     `json:"miners,omitempty"`
	Steps             []HandleStep `json:"steps,omitempty"`
	CompleteTime      time.Time    `json:"completeTime,omitempty"`
	Aborted           bool         `json:"aborted,omitempty"`
	retryRounds       int
	storedButTxFailed bool
}

func (t *RelayState) pushStep(step HandleStep) {
	t.Steps = append(t.Steps, step)
	if step.Step == _FINISH_STEP || step.Step == _ABORT_STEP {
		t.CompleteTime = time.Now()
		if step.Step == _ABORT_STEP {
			t.Aborted = true
		} else {
			t.Aborted = false
		}
	}
}

func (t *RelayState) IsProcessing() bool {
	return t.CompleteTime.IsZero()
}

func (t *RelayState) IsComplete() bool {
	return !t.IsProcessing()
}

func (t *RelayState) IsCompleteBefore(ref time.Time, d time.Duration) bool {
	return t.IsComplete() && ref.After(t.CompleteTime.Add(d))
}

func (t *RelayState) IsAbort() bool {
	return t.IsComplete() && t.Aborted
}

type StepEmiter interface {
	EmitStep(step HandleStep)
}

type RelayHandler interface {
	StepEmiter
	Relay() error
	Id() FileHash
	ListenerProgress() <-chan HandleStep
	State() *RelayState
	IsProcessing() bool
	CanClean() bool
	Close()
}

func EmitStep(emiter StepEmiter, step string, args ...any) {
	p := HandleStep{Step: step}
	if len(args) > 0 {
		if err, ok := args[0].(error); ok {
			p.Error = err
		} else if msg, ok := args[0].(string); ok {
			p.Msg = msg
		} else {
			p.Data = args[0]
		}
	}
	emiter.EmitStep(p)
}

type SimpleRelayHandlerBase struct {
	id         FileHash
	state      RelayState
	log        logr.Logger
	stateMutex sync.RWMutex
	stepChan   chan HandleStep
	fileStash  *CessStash
	fsm        *segment.FileSegmentMeta
	accountId  types.AccountID
	bucketName string
}

func (t *SimpleRelayHandlerBase) Id() FileHash { return t.id }

func (t *SimpleRelayHandlerBase) Fsm() *segment.FileSegmentMeta { return t.fsm }

func (t *SimpleRelayHandlerBase) ListenerProgress() <-chan HandleStep {
	if t.stepChan == nil {
		t.stepChan = make(chan HandleStep)
	}
	return t.stepChan
}

func (t *SimpleRelayHandlerBase) State() *RelayState {
	t.stateMutex.RLock()
	defer t.stateMutex.RUnlock()
	return &t.state
}

func (t *SimpleRelayHandlerBase) IsProcessing() bool {
	return t.State().IsProcessing()
}

func (t *SimpleRelayHandlerBase) CanClean() bool {
	t.stateMutex.RLock()
	defer t.stateMutex.RUnlock()
	s := t.state
	if s.IsProcessing() {
		return false
	}
	if s.IsAbort() {
		return s.IsCompleteBefore(time.Now(), 5*time.Second)
	} else {
		return s.IsCompleteBefore(time.Now(), 300*time.Second)
	}
}

func (t *SimpleRelayHandlerBase) EmitStep(step HandleStep) {
	t.stateMutex.Lock()
	defer t.stateMutex.Unlock()
	t.state.pushStep(step)
	if t.log.V(1).Enabled() {
		log := t.log.V(1).WithCallDepth(2)
		kvs := make([]any, 0, 6)
		kvs = append(kvs, "step", step.Step)
		if step.Data != nil {
			kvs = append(kvs, "data", step.Data)
		}
		if len(step.Msg) > 0 {
			kvs = append(kvs, "msg", step.Msg)
		}
		if step.IsAbort() {
			log.Error(step.Error, "relay abort", kvs...)
		} else {
			log.Info("relay progress", kvs...)
		}
	}
	if t.stepChan != nil {
		t.stepChan <- step
	}
}

func (t *SimpleRelayHandlerBase) Close() {
	if t.stepChan != nil {
		close(t.stepChan)
	}
}

func (t *SimpleRelayHandlerBase) Declaration(fsm *segment.FileSegmentMeta, actualDecl bool) (string, error) {
	user := cesspat.UserBrief{
		User:       t.accountId,
		BucketName: types.NewBytes([]byte(t.bucketName)),
		FileName:   types.NewBytes([]byte(fsm.Name)),
	}
	var dstSegs []cesspat.SegmentList
	if actualDecl {
		dstSegs = make([]cesspat.SegmentList, len(fsm.Segments))
		for i := 0; i < len(dstSegs); i++ {
			srcSeg := &fsm.Segments[i]
			dstSeg := &dstSegs[i]
			dstSeg.SegmentHash = toCessFileHashType(srcSeg.Hash)
			for j := 0; j < len(srcSeg.Frags); j++ {
				dstSeg.FragmentHash = append(dstSeg.FragmentHash, toCessFileHashType(srcSeg.Frags[j].Hash))
			}
		}
		EmitStep(t, "upload declaring")
	} else {
		EmitStep(t, "upload declaring: only record the relationship")
	}
	return t.fileStash.cessc.UploadDeclaration(fsm.RootHash.Hex(), dstSegs, user, uint64(fsm.InputSize))
}

func toCessFileHashType(hash *hash.H256) cesspat.FileHash {
	//FIXME: the chain define FileHash type use 64 bytes hash hex
	cessFileHash := cesspat.FileHash{}
	n := len(cessFileHash)
	hex := hash.Hex()
	for i := 0; i < n; i++ {
		cessFileHash[i] = types.U8(hex[i])
	}
	return cessFileHash
}
