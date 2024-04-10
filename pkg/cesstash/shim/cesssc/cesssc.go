package cesssc

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	cp2p "github.com/CESSProject/p2p-go"
	cp2p_conf "github.com/CESSProject/p2p-go/config"
	cp2p_core "github.com/CESSProject/p2p-go/core"
	"github.com/go-logr/logr"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/discovery/routing"
	"github.com/libp2p/go-libp2p/p2p/discovery/util"
	"github.com/pkg/errors"
	"golang.org/x/time/rate"

	logging "github.com/ipfs/go-log"
)

func init() {
	logging.SetAllLoggers(logging.LevelError)
}

type PeerIdFilter interface {
	IsAllowed(id peer.ID) bool
}

type PeerState uint8

const (
	NotConnected PeerState = iota
	Connecting
	Connected
	FailedConnecting
)

var peerStateNames = []string{
	NotConnected:     "NotConnected",
	Connecting:       "Connecting",
	Connected:        "Connected",
	FailedConnecting: "FailedConnecting",
}

func (p PeerState) String() string {
	if uint(p) < uint(len(peerStateNames)) {
		return peerStateNames[uint8(p)]
	}
	return "PeerState:" + strconv.Itoa(int(p))
}

type PeerStatesCount map[PeerState]int

func (t PeerStatesCount) String() string {
	i := 0
	n := len(t)
	b := strings.Builder{}
	b.WriteString("{")
	for k, v := range t {
		b.WriteString(k.String())
		b.WriteString(":")
		b.WriteString(fmt.Sprint(v))
		i++
		if i < n {
			b.WriteString(",")
		}
	}
	b.WriteString("}")
	return b.String()
}

type CessStorageClient struct {
	cp2p_core.P2P
	ctx             context.Context
	log             logr.Logger
	peerStates      *sync.Map
	maxPeerKeepSize int
	peerFilter      PeerIdFilter
}

func New(listenPort uint16, workDir string, configedBootAddrs []string, log logr.Logger) (*CessStorageClient, error) {
	if len(configedBootAddrs) == 0 {
		return nil, errors.New("empty boot address")
	}
	ctx := context.Background()
	c, err := cp2p.New(
		ctx,
		cp2p.ListenPort(int(listenPort)),
		cp2p.Workspace(workDir),
		cp2p.BootPeers(configedBootAddrs),
		cp2p.ProtocolPrefix(figureProtocolPrefix(configedBootAddrs)),
		cp2p.BucketSize(100),
	)
	if err != nil {
		return nil, err
	}
	log.Info("local peer info", "peerId", c.ID(), "addrs", c.Addrs(), "rendezvousVersion", c.GetRendezvousVersion(), "dhtProtocolVersion", c.GetDhtProtocolVersion())

	p2pm := CessStorageClient{
		ctx:             ctx,
		log:             log,
		peerStates:      &sync.Map{},
		maxPeerKeepSize: 200,
	}
	p2pm.P2P = c
	go func() {
		p2pm.discoverPeersLoop()
	}()
	return &p2pm, nil
}

func (n *CessStorageClient) WithPeerIdFilter(pf PeerIdFilter) {
	n.peerFilter = pf
}

func (n *CessStorageClient) discoverPeersLoop() {
	logger := n.log
	ctx := n.ctx
	rendezvous := n.GetRendezvousVersion()
	routingDiscovery := routing.NewRoutingDiscovery(n.GetDHTable())
	util.Advertise(n.ctx, routingDiscovery, rendezvous)

	ticker := time.NewTicker(time.Second * 5)
	defer ticker.Stop()

	printLimit := rate.NewLimiter(rate.Every(time.Minute), 1)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if printLimit.Allow() {
				countMap := n.countPeerStates()
				n.log.Info("peer connect state", "counter", countMap.String())
			}

			connecteds := n.connectedPeersCount()
			if connecteds >= n.maxPeerKeepSize {
				continue
			}
			peers, err := routingDiscovery.FindPeers(ctx, rendezvous)
			if err != nil {
				logger.Error(err, "[FindPeers] error")
				continue
			}

			for p := range peers {
				if p.ID == n.ID() {
					continue
				}
				if n.peerFilter != nil {
					if !n.peerFilter.IsAllowed(p.ID) {
						continue
					}
				}
				n.handlePeerConnection(p)
			}
		}
	}
}

func (n *CessStorageClient) FindPeer(id peer.ID) (pi peer.AddrInfo, err error) {
	return n.GetDHTable().FindPeer(n.ctx, id)
}

func (n *CessStorageClient) GetConnectedPeers() []peer.ID {
	peerIds := make([]peer.ID, 0, 32)
	n.peerStates.Range(func(k any, v any) bool {
		peerId, ok := k.(peer.ID)
		if ok {
			state, ok := v.(PeerState)
			if ok && state == Connected {
				peerIds = append(peerIds, peerId)
			}
		}
		return true
	})
	return peerIds
}

func (n *CessStorageClient) ReportNotAvailable(peerId peer.ID) {
	n.log.V(1).Info("peer is reported not available", "peerId", peerId)
	n.peerStates.Delete(peerId)
}

func (n *CessStorageClient) Context() context.Context {
	return n.ctx
}

func (n *CessStorageClient) countPeerStates() PeerStatesCount {
	countMap := make(PeerStatesCount)
	n.peerStates.Range(func(_, v any) bool {
		state, ok := v.(PeerState)
		if ok {
			countMap[state]++
		}
		return true
	})
	return countMap
}

func (n *CessStorageClient) connectedPeersCount() int {
	cnt := 0
	n.peerStates.Range(func(_, v any) bool {
		state, ok := v.(PeerState)
		if ok && state == Connected {
			cnt++
		}
		return true
	})
	return cnt
}

func (n *CessStorageClient) handlePeerConnection(pi peer.AddrInfo) {
	peerState, _ := n.peerStates.LoadOrStore(pi.ID, NotConnected)
	switch peerState.(PeerState) {
	case Connected:
		return
	case NotConnected:
	case Connecting:
		// n.log.V(2).Info("Skipping node as we're already trying to connect", "peer", pi.ID)
		return
	case FailedConnecting:
		// n.log.V(2).Info("We tried to connect previously but couldn't establish a connection, try again", "peer", pi.ID)
	}

	go func() {
		ctx := n.ctx
		// logger := n.log
		n.peerStates.Store(pi.ID, Connecting)
		if n.Network().Connectedness(pi.ID) != network.Connected {
			_, err := n.Network().DialPeer(ctx, pi.ID)
			if err != nil {
				// logger.V(2).Info("[DialPeer] error", "error", err)
				n.peerStates.Store(pi.ID, FailedConnecting)
				return
			}
			n.peerStates.Store(pi.ID, Connected)
			// logger.V(1).Info("peer connected", "peer", pi)
		}
		n.peerStates.Store(pi.ID, Connected)
	}()
}

func figureProtocolPrefix(bootNodeAddrs []string) string {
	for _, v := range bootNodeAddrs {
		if strings.Contains(v, "testnet") {
			return cp2p_conf.TestnetProtocolPrefix
		} else if strings.Contains(v, "mainnet") {
			return cp2p_conf.MainnetProtocolPrefix
		} else if strings.Contains(v, "devnet") {
			return cp2p_conf.DevnetProtocolPrefix
		}
	}
	return "unknown"
}
