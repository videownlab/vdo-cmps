package cesssc

import (
	"testing"
	"time"
	"vdo-cmps/pkg/log"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
)

var logger logr.Logger

func init() {
	logger = log.Logger
}

func TestPeerDiscover(t *testing.T) {
	_, err := New(33350, ".workdata", []string{"_dnsaddr.boot-bucket-testnet.cess.cloud"}, logger)
	assert.NoError(t, err)
	time.Sleep(90 * time.Second)
}
