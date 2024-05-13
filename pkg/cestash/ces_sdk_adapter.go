package cestash

import (
	"math"
	"time"

	"github.com/CESSProject/cess-go-sdk/core/pattern"
	cessdk "github.com/CESSProject/cess-go-sdk/core/sdk"
	"github.com/centrifuge/go-substrate-rpc-client/v4/types"
)

type CesSdkAdapter struct {
	c cessdk.SDK
}

func restoreStateForBadSdkIfError(t *CesSdkAdapter, err error) {
	if err == nil || t.c.GetChainState() {
		return
	}
	//FIXME: the cess sdk make the state false once catch rpc error
	logger.Error(err, "the cess sdk make the state false once catch rpc error")
	logger.V(1).Info("reconnect chain")
	for i := 0; i < math.MaxInt; i++ {
		err = t.c.ReconnectRPC()
		if err != nil {
			logger.Error(err, "[ReconnectRPC] failed, try again later")
			time.Sleep(1500 * time.Millisecond)
			continue
		}
		t.c.SetChainState(true)
		return
	}
}

func (t *CesSdkAdapter) UploadDeclaration(filehash string, dealinfo []pattern.SegmentList, user pattern.UserBrief, filesize uint64) (_ string, err error) {
	defer func() {
		e := err
		restoreStateForBadSdkIfError(t, e)
	}()
	return t.c.UploadDeclaration(filehash, dealinfo, user, filesize)
}

func (t *CesSdkAdapter) QueryStorageOrder(fid string) (_ pattern.StorageOrder, err error) {
	defer func() {
		e := err
		restoreStateForBadSdkIfError(t, e)
	}()
	return t.c.QueryStorageOrder(fid)
}

func (t *CesSdkAdapter) QueryFileMetadata(fid string) (_ pattern.FileMetadata, err error) {
	defer func() {
		e := err
		restoreStateForBadSdkIfError(t, e)
	}()
	return t.c.QueryFileMetadata(fid)
}

func (t *CesSdkAdapter) QueryAuthorizedAccounts(accountID []byte) (_ []string, err error) {
	defer func() {
		e := err
		restoreStateForBadSdkIfError(t, e)
	}()
	return t.c.QueryAuthorizedAccounts(accountID)
}

func (t *CesSdkAdapter) GetSignatureAcc() string {
	return t.c.GetSignatureAcc()
}

func (t *CesSdkAdapter) QueryStorageMiner(accountID []byte) (_ pattern.MinerInfo, err error) {
	defer func() {
		e := err
		restoreStateForBadSdkIfError(t, e)
	}()
	return t.c.QueryStorageMiner(accountID)
}

func (t *CesSdkAdapter) CreateBucket(accountID []byte, bucketName string) (_ string, err error) {
	defer func() {
		e := err
		restoreStateForBadSdkIfError(t, e)
	}()
	return t.c.CreateBucket(accountID, bucketName)
}

func (t *CesSdkAdapter) QueryAllSminerAccount() (_ []types.AccountID, err error) {
	defer func() {
		e := err
		restoreStateForBadSdkIfError(t, e)
	}()
	return t.c.QueryAllSminerAccount()
}

func (t *CesSdkAdapter) QueryBucketInfo(accountID []byte, bucketName string) (_ pattern.BucketInfo, err error) {
	defer func() {
		e := err
		restoreStateForBadSdkIfError(t, e)
	}()
	return t.c.QueryBucketInfo(accountID, bucketName)
}

func (t *CesSdkAdapter) DeleteFile(accountID []byte, fid []string) (_ string, _ []pattern.FileHash, err error) {
	defer func() {
		e := err
		restoreStateForBadSdkIfError(t, e)
	}()
	return t.c.DeleteFile(accountID, fid)
}
