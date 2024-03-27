package cesssc

import (
	"context"
	"fmt"
	"net"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/AstaFrode/go-libp2p/core/peer"
	cp2p "github.com/CESSProject/p2p-go"
	cp2p_conf "github.com/CESSProject/p2p-go/config"
	cp2p_core "github.com/CESSProject/p2p-go/core"
	"github.com/go-logr/logr"
	ma "github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
	"github.com/pkg/errors"
	"golang.org/x/time/rate"

	logging "github.com/ipfs/go-log"
)

func init() {
	// logging.SetDebugLogging()
	// logging.SetAllLoggers(logging.LevelDebug)
	logging.SetAllLoggers(logging.LevelInfo)
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
	log        logr.Logger
	peerStates *sync.Map
}

func New(listenPort uint16, workDir string, configedBootAddrs []string, log logr.Logger) (*CessStorageClient, error) {
	if len(configedBootAddrs) == 0 {
		return nil, errors.New("empty boot address")
	}
	c, err := cp2p.New(
		context.Background(),
		cp2p.ListenPort(int(listenPort)),
		cp2p.Workspace(workDir),
		cp2p.BootPeers(configedBootAddrs),
		cp2p.ProtocolPrefix(figureProtocolPrefix(configedBootAddrs)),
		cp2p.EnableBitswap(),
	)
	if err != nil {
		return nil, err
	}
	log.Info("local peer id", "peerId", c.PeerID())

	p2pm := CessStorageClient{
		log:        log,
		peerStates: &sync.Map{},
	}
	p2pm.P2P = c
	go func() {
		//run(&p2pm)
		for {
			if err := p2pm.connectBootNodes(); err != nil {
				log.Error(err, "try again 5secs later")
				time.Sleep(5 * time.Second)
				continue
			}
			break
		}
		p2pm.discoverPeers()
	}()
	return &p2pm, nil
}

func run(n *CessStorageClient) {
	for {
		if err := n.connectBootNodes(); err != nil {
			n.log.Error(err, "try again 5secs later")
			time.Sleep(5 * time.Second)
			continue
		}
		break
	}

	var (
		ch_findPeers = make(chan bool, 1)
		ch_recvPeers = make(chan bool, 1)
	)
	go n.findPeers(ch_findPeers)
	go n.recvPeers(ch_recvPeers)

	for {
		select {

		case <-ch_findPeers:
			go n.findPeers(ch_findPeers)

		case <-ch_recvPeers:
			go n.recvPeers(ch_recvPeers)
		}
	}
}

func (n *CessStorageClient) connectBootNodes() error {
	log := n.log
	bootAddrs := n.P2P.GetBootstraps()
	connectedN := 0
	for _, v := range bootAddrs {
		bootnodes, err := parseMultiaddr(v)
		if err != nil {
			log.Error(err, "parseMultiaddr()", "addr", v)
			continue
		}
		for _, v := range bootnodes {
			addr, err := ma.NewMultiaddr(v)
			if err != nil {
				log.Error(err, "ma.NewMultiaddr()", "addr", v)
				continue
			}
			addrInfo, err := peer.AddrInfoFromP2pAddr(addr)
			if err != nil {
				log.Error(err, "peer.AddrInfoFromP2pAddr", "addr", addr)
				continue
			}
			log.Info("got bootstrap node", "addr", addrInfo)

			if err := n.Connect(n.GetCtxQueryFromCtxCancel(), *addrInfo); err != nil {
				log.Error(err, "connect bootnode")
			}
			n.GetDht().RoutingTable().TryAddPeer(addrInfo.ID, true, true)
			connectedN++
		}
	}
	if connectedN == 0 {
		return errors.New("can't connect any boot nodes")
	}
	return nil
}

func (n *CessStorageClient) discoverPeers() {
	tickDiscover := time.NewTicker(time.Minute)
	defer tickDiscover.Stop()

	var limit = rate.NewLimiter(rate.Every(time.Second*3), 1)
	var printLimit = rate.NewLimiter(rate.Every(time.Minute), 1)

	peerCh, err := n.RouteTableFindPeers(0)
	if err != nil {
		n.log.Error(err, "")
	}
LOOP:
	for {
		select {
		case p, ok := <-n.GetDiscoveredPeers():
			if !ok {
				break LOOP
			}
			fmt.Printf(" ++++ %v\n", p)
		case p := <-peerCh:
			if p.ID == n.ID() {
				continue
			}
			if limit.Allow() {
				tickDiscover.Reset(time.Minute)
			}
			if len(p.Addrs) == 0 {
				break
			}

			p.Addrs = onlyPublic(p.Addrs, &n.log)
			if isRoutable(&p) {
				n.handlePeer(&p)
			}
			if printLimit.Allow() {
				countMap := n.countPeerStates()
				n.log.V(1).Info("peer connect state counts", "count", countMap.String())
			}
		case <-tickDiscover.C:
			countMap := n.countPeerStates()
			peerCh, err = n.RouteTableFindPeers(countMap[Connected] + 20)
			if err != nil {
				n.log.Error(err, "")
			}
		}
	}
}

func (n *CessStorageClient) discoverPeers2() {
	tickDiscover := time.NewTicker(time.Minute)
	defer tickDiscover.Stop()

	var r1 = rate.Every(time.Second * 3)
	var limit = rate.NewLimiter(r1, 1)

	var r2 = rate.Every(time.Minute)
	var printLimit = rate.NewLimiter(r2, 1)
	n.RouteTableFindPeers(0)

LOOP:
	for {
		select {
		case p, ok := <-n.GetDiscoveredPeers():
			if !ok {
				break LOOP
			}
			if limit.Allow() {
				tickDiscover.Reset(time.Minute)
			}
			if len(p.Responses) == 0 {
				break
			}
			fmt.Printf(" ++++ %T, %v\n", p, p)
			addrInfos := p.Responses // clone.Clone(p.Responses).([]*peer.AddrInfo)
			for _, pai := range addrInfos {
				pai.Addrs = onlyPublic(pai.Addrs, &n.log)
				if isRoutable(pai) {
					n.handlePeer(pai)
				}
			}
			if printLimit.Allow() {
				countMap := n.countPeerStates()
				n.log.V(1).Info("peer connect state counts", "count", countMap.String())
			}
		case <-tickDiscover.C:
			countMap := n.countPeerStates()
			_, err := n.RouteTableFindPeers(countMap[Connected] + 20)
			if err != nil {
				n.log.Error(err, "")
			}
		}
	}
	n.log.V(1).Info("peer-discovery loop exit")
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

func (n *CessStorageClient) handlePeer(pi *peer.AddrInfo) {
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
		// n.log.V(2).Info("Connecting to peer", "peer", pi)
		n.peerStates.Store(pi.ID, Connecting)
		if err := n.Connect(n.GetCtxQueryFromCtxCancel(), *pi); err != nil {
			// n.log.Error(err, "Error connecting to peer", "peer", pi)
			n.peerStates.Store(pi.ID, FailedConnecting)
			return
		}
		n.peerStates.Store(pi.ID, Connected)
		// n.log.V(1).Info("Peer connected", "peer", pi)
	}()
}

// Filter out addresses that are local - only allow public ones.
func onlyPublic(addrs []ma.Multiaddr, log *logr.Logger) []ma.Multiaddr {
	routable := []ma.Multiaddr{}
	for _, addr := range addrs {
		if isNil(addr, log) {
			continue
		}
		if manet.IsPublicAddr(addr) {
			routable = append(routable, addr)
		}
	}
	return routable
}

func isRoutable(pi *peer.AddrInfo) bool {
	return len(pi.Addrs) > 0
}

func isNil(addr ma.Multiaddr, log *logr.Logger) bool {
	defer func() {
		if r := recover(); r != nil {
			log.Error(r.(error), "catch panic on check Multiaddr is nil!")
		}
	}()
	if addr == nil {
		return true
	}
	//FIXME: may crash as the 'addr' in debug view error: (unreadable could not resolve interface type)
	return reflect.ValueOf(addr).IsNil()
}

func parseMultiaddr(addr string) ([]string, error) {
	var result = make([]string, 0)
	var realDns = make([]string, 0)

	multiaddr, err := ma.NewMultiaddr(addr)
	if err == nil {
		_, err = peer.AddrInfoFromP2pAddr(multiaddr)
		if err == nil {
			result = append(result, addr)
			return result, nil
		}
	}

	dnsnames, err := net.LookupTXT(addr)
	if err != nil {
		return result, err
	}

	for _, v := range dnsnames {
		if strings.Contains(v, "ip4") && strings.Contains(v, "tcp") && strings.Count(v, "=") == 1 {
			result = append(result, strings.TrimPrefix(v, "dnsaddr="))
		}
	}

	trims := strings.Split(addr, ".")
	domainname := fmt.Sprintf("%s.%s", trims[len(trims)-2], trims[len(trims)-1])

	for _, v := range dnsnames {
		trims = strings.Split(v, "/")
		for _, vv := range trims {
			if strings.Contains(vv, domainname) {
				realDns = append(realDns, vv)
				break
			}
		}
	}

	for _, v := range realDns {
		dnses, err := net.LookupTXT("_dnsaddr." + v)
		if err != nil {
			continue
		}
		for i := 0; i < len(dnses); i++ {
			if strings.Contains(dnses[i], "ip4") && strings.Contains(dnses[i], "tcp") && strings.Count(dnses[i], "=") == 1 {
				var multiaddr = strings.TrimPrefix(dnses[i], "dnsaddr=")
				result = append(result, multiaddr)
			}
		}
	}

	return result, nil
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
