package types

import (
	"github.com/journeymidnight/yig-restore/meta/common"
	"time"
)

type Freezer struct {
	Rowkey           []byte // Rowkey cache
	Name             string
	BucketName       string
	Location         string // which Ceph cluster this object locates
	Pool             string // which Ceph pool this object locates
	OwnerId          string
	Size             int64     // file size
	ObjectId         string    // object name in Ceph
	LastModifiedTime time.Time // in format "2006-01-02T15:04:05.000Z"
	Etag             string
	Parts            map[int]*Part
	PartsIndex       *SimpleIndex
	VersionId        string // version cache
	Status           common.Status
	LifeTime         int
	Type             ObjectType
	CreateTime       uint64 // Timestamp(nanosecond)
}

func (o *Freezer) GetUpdateSql(status common.Status) (string, []interface{}) {
	lastModifiedTime := o.LastModifiedTime.Format(TIME_LAYOUT_TIDB)
	sql := "update restoreobjects set status=?,lastmodifiedtime=?,location=?,pool=?," +
		"ownerid=?,size=?,objectid=?,etag=? where bucketname=? and objectname=? and version=? and status=?"
	args := []interface{}{status, lastModifiedTime, o.Location, o.Pool, o.OwnerId, o.Size, o.ObjectId, o.Etag, o.BucketName, o.Name, o.VersionId, o.Status}
	return sql, args
}