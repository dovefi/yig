package tikvclient

import (
	"context"

	"github.com/journeymidnight/yig/helper"
	"github.com/tikv/client-go/config"
	"github.com/tikv/client-go/rawkv"
	"github.com/tikv/client-go/txnkv"
)

const (
	TableClusterPrefix    = "c"
	TableBucketPrefix     = "b"
	TableUserBucketPrefix = "u"
	TableMultipartPrefix  = "m"
	TableObjectPartPrefix = "p"
	TableLifeCyclePrefix  = "l"
)

var (
	TableMaxKeySuffix = string(0xFF)
	TableSeparator    = string(92) // "\"
)

type TiKVClient struct {
	rawCli *rawkv.Client
	txnCli *txnkv.Client
}

// KV represents a Key-Value pair.
type KV struct {
	K, V []byte
}

func NewClient() TiKVClient {
	rawCli, err := rawkv.NewClient(context.TODO(), []string{helper.CONFIG.PdAddress}, config.Default())
	if err != nil {
		panic(err)
	}

	txnCli, err := txnkv.NewClient(context.TODO(), []string{helper.CONFIG.PdAddress}, config.Default())
	if err != nil {
		panic(err)
	}
	return TiKVClient{rawCli, txnCli}
}

func (c *TiKVClient) Put(k []byte, v interface{}) error {
	b, err := helper.MsgPackMarshal(v)
	if err != nil {
		return err
	}
	return c.rawCli.Put(context.TODO(), k, b)
}

func (c *TiKVClient) Get(k []byte) ([]byte, error) {
	return c.rawCli.Get(context.TODO(), k)
}

func (c *TiKVClient) Scan(startKey []byte, endKey []byte, limit int) ([]KV, error) {
	ks, vs, err := c.rawCli.Scan(context.TODO(), startKey, endKey, limit)
	if err != nil {
		return nil, err
	}
	var ret []KV
	for i, k := range ks {
		ret = append(ret, KV{K: k, V: vs[i]})
	}
	return ret, nil
}

func (c *TiKVClient) TxPut(args ...interface{}) error {
	tx, err := c.txnCli.Begin(context.TODO())
	if err != nil {
		return err
	}
	defer func() {
		if err == nil {
			err = tx.Commit(context.Background())
		}
		if err != nil {
			tx.Rollback()
		}
	}()
	for i := 0; i < len(args); i += 2 {
		key := args[i].([]byte)
		val := args[i+1]
		v, err := helper.MsgPackMarshal(val)
		if err != nil {
			return err
		}
		err = tx.Set(key, v)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *TiKVClient) TxDelete(keys ...[]byte) error {
	tx, err := c.txnCli.Begin(context.TODO())
	if err != nil {
		return err
	}
	defer func() {
		if err == nil {
			err = tx.Commit(context.Background())
		}
		if err != nil {
			tx.Rollback()
		}
	}()

	for _, key := range keys {
		err := tx.Delete(key)
		if err != nil {
			return err
		}
	}
	return nil
}