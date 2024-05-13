package shim

import (
	cesspat "github.com/CESSProject/cess-go-sdk/core/pattern"
	"github.com/libp2p/go-libp2p/core/peer"
)

func FirstFilename(t *cesspat.FileMetadata) string {
	if t.Owner == nil || len(t.Owner) == 0 {
		return ""
	}
	return string(t.Owner[0].FileName)
}

func ToPeerId(origPeerId *cesspat.PeerId) (peer.ID, error) {
	byteArray := make([]byte, len(origPeerId))
	for i, d := range origPeerId {
		byteArray[i] = byte(d)
	}
	return peer.IDFromBytes(byteArray)
}
