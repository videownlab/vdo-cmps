package cesssc

import (
	"fmt"
	"runtime/debug"
	"sync/atomic"
	"time"
)

var roundCount atomic.Uint32

func (n *CessStorageClient) findPeers(ch chan<- bool) {
	defer func() {
		if r := recover(); r != nil {
			err := r.(error)
			fmt.Printf("%+v\n", err)
			n.log.Error(err, "catch panic on relay process!", "stack", string(debug.Stack()))
		}
		ch <- true
	}()
	n.log.Info(">>>>> start findPeers <<<<<")
	for {
		if roundCount.Load() > 10 {
			roundCount.Store(0)
			err := n.findpeer()
			if err != nil {
				n.log.Error(err, "")
			}
		}
	}
}

func (n *CessStorageClient) recvPeers(ch chan<- bool) {
	defer func() {
		if r := recover(); r != nil {
			err := r.(error)
			fmt.Printf("%+v\n", err)
			n.log.Error(err, "catch panic on relay process!", "stack", string(debug.Stack()))
		}
		ch <- true
	}()

	n.log.Info("info", ">>>>> start recvPeers <<<<<")

	for {
		select {
		case foundPeer := <-n.GetDiscoveredPeers():
			for _, v := range foundPeer.Responses {
				if v != nil {
					if len(v.Addrs) > 0 {
						n.GetDht().RoutingTable().TryAddPeer(foundPeer.ID, true, true)
						n.log.Info("got peer", "peer", v)
						// n.SavePeer(v.ID.Pretty(), peer.AddrInfo{
						// 	ID:    v.ID,
						// 	Addrs: v.Addrs,
						// })
					}
				}
			}
		default:
			roundCount.Add(1)
			time.Sleep(time.Second)
		}
	}
}

func (n *CessStorageClient) findpeer() error {
	peerChan, err := n.GetRoutingTable().FindPeers(
		n.GetCtxQueryFromCtxCancel(),
		n.GetRendezvousVersion(),
	)
	if err != nil {
		return err
	}

	for onePeer := range peerChan {
		if onePeer.ID == n.ID() {
			continue
		}
		err := n.Connect(n.GetCtxQueryFromCtxCancel(), onePeer)
		if err != nil {
			n.GetDht().RoutingTable().RemovePeer(onePeer.ID)
		} else {
			n.GetDht().RoutingTable().TryAddPeer(onePeer.ID, true, true)
			n.log.Info("got peer", "peer", onePeer)
			// n.SavePeer(onePeer.ID.Pretty(), peer.AddrInfo{
			// 	ID:    onePeer.ID,
			// 	Addrs: onePeer.Addrs,
			// })
		}
	}
	return nil
}
