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
	ctx                 context.Context
	log                 logr.Logger
	maxPeerKeepSize     int
	peerFilter          PeerIdFilter
	connectedPeersLock  sync.RWMutex
	connectedPeers      map[peer.ID]struct{}
	connectingPeersLock sync.RWMutex
	connectingPeers     map[peer.ID]struct{}
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

	p2pm := CessStorageClient{
		ctx:             ctx,
		log:             log,
		maxPeerKeepSize: 200,
		connectedPeers:  make(map[peer.ID]struct{}),
		connectingPeers: make(map[peer.ID]struct{}),
	}
	p2pm.P2P = c
	go func() {
		p2pm.discoverPeersLoop()
	}()
	log.Info("local peer info", "peerId", c.ID(), "addrs", c.Addrs(), "rendezvousVersion", c.GetRendezvousVersion(),
		"dhtProtocolVersion", c.GetDhtProtocolVersion(), "maxPeerKeepSize", p2pm.maxPeerKeepSize)
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

	peerFindingTicker := time.NewTicker(time.Second * 5)
	defer peerFindingTicker.Stop()
	statePrintTicker := time.NewTicker(time.Minute * 15)
	defer statePrintTicker.Stop()

	logger.Info("miner peers discovery starting")
	for {
		select {
		case <-ctx.Done():
			logger.Info("peer discover loop exit")
			return
		case <-peerFindingTicker.C:
			if n.ConnectedPeersCount() >= n.maxPeerKeepSize {
				continue
			}

			// logger.V(2).Info("begin finding peers")
			peerCh, err := routingDiscovery.FindPeers(ctx, rendezvous)
			if err != nil {
				logger.Error(err, "[FindPeers] error")
				continue
			}
			for p := range peerCh {
				if p.ID == n.ID() {
					// logger.V(2).Info("ignore self peer")
					continue
				}
				if n.peerFilter != nil {
					if !n.peerFilter.IsAllowed(p.ID) {
						logger.V(2).Info("peer is banned, ignored", "peerId", p.ID)
						continue
					}
				}
				n.handlePeerConnection(p)
			}
		case <-statePrintTicker.C:
			logger.Info("peer connect state", "connected", n.ConnectedPeersCount(), "connecting", n.ConnectingPeersCount())
		}
	}
}

func (n *CessStorageClient) FindPeer(id peer.ID) (pi peer.AddrInfo, err error) {
	return n.GetDHTable().FindPeer(n.ctx, id)
}

func (n *CessStorageClient) GetConnectedPeers() []peer.ID {
	n.connectedPeersLock.RLock()
	defer n.connectedPeersLock.RUnlock()
	peerIds := make([]peer.ID, 0, len(n.connectedPeers))
	for pid := range n.connectedPeers {
		peerIds = append(peerIds, pid)
	}
	return peerIds
}

func (n *CessStorageClient) ReportNotAvailable(peerId peer.ID) {
	n.connectedPeersLock.Lock()
	defer n.connectedPeersLock.Unlock()
	delete(n.connectedPeers, peerId)
	n.log.V(1).Info("peer is reported not available", "peerId", peerId)
}

func (n *CessStorageClient) Context() context.Context {
	return n.ctx
}

func (n *CessStorageClient) ConnectedPeersCount() int {
	n.connectedPeersLock.RLock()
	defer n.connectedPeersLock.RUnlock()
	return len(n.connectedPeers)
}

func (n *CessStorageClient) ConnectingPeersCount() int {
	n.connectingPeersLock.RLock()
	defer n.connectingPeersLock.RUnlock()
	return len(n.connectingPeers)
}

func (n *CessStorageClient) handlePeerConnection(pi peer.AddrInfo) {
	// logger := n.log
	if n.isPeerConnecting(pi.ID) {
		// logger.V(2).Info("igonre the connecting peer", "peerId", pi.ID)
		return
	}
	go func() {
		ctx := n.ctx
		n.togglePeerConnecting(pi.ID, true)
		if n.Network().Connectedness(pi.ID) != network.Connected {
			_, err := n.Network().DialPeer(ctx, pi.ID)
			if err != nil {
				// logger.V(2).Info("[DialPeer] error", "error", err)
				n.togglePeerConnecting(pi.ID, false)
				return
			}
			// logger.V(1).Info("peer connected", "peer", pi)
		}
		n.togglePeerConnected(pi.ID, true)
		n.togglePeerConnecting(pi.ID, false)
	}()
}

func (n *CessStorageClient) isPeerConnecting(peerId peer.ID) bool {
	n.connectingPeersLock.RLock()
	defer n.connectingPeersLock.RUnlock()
	_, connecting := n.connectingPeers[peerId]
	return connecting
}

func (n *CessStorageClient) togglePeerConnecting(peerId peer.ID, connecting bool) {
	n.connectingPeersLock.Lock()
	defer n.connectingPeersLock.Unlock()
	if connecting {
		n.connectingPeers[peerId] = struct{}{}
	} else {
		delete(n.connectingPeers, peerId)
	}
}

func (n *CessStorageClient) togglePeerConnected(peerId peer.ID, connected bool) {
	n.connectedPeersLock.Lock()
	defer n.connectedPeersLock.Unlock()
	if connected {
		n.connectedPeers[peerId] = struct{}{}
	} else {
		delete(n.connectedPeers, peerId)
	}
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
