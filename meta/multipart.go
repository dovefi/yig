package meta

import (
	"database/sql"
	. "github.com/journeymidnight/yig/meta/types"
)

func (m *Meta) GetMultipart(bucketName, objectName, uploadId string) (multipart Multipart, err error) {
	return m.Client.GetMultipart(bucketName, objectName, uploadId)
}

func (m *Meta) DeleteMultipart(multipart Multipart) (err error) {
	tx, err := m.Client.NewTrans()
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			m.Client.AbortTrans(tx)
		}
	}()
	err = m.Client.DeleteMultipart(&multipart, tx)
	if err != nil {
		return
	}
	var removedSize int64 = 0
	for _, p := range multipart.Parts {
		removedSize += p.Size
	}
	err = m.Client.UpdateUsage(multipart.BucketName, -removedSize, tx)
	if err != nil {
		return
	}
	err = m.Client.CommitTrans(tx)
	return
}

func (m *Meta) PutObjectPart(multipart Multipart, part Part) (err error) {
	tx, err := m.Client.NewTrans()
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			m.Client.AbortTrans(tx)
		}
	}()
	// 将part元数据信息写入数据库
	err = m.Client.PutObjectPart(&multipart, &part, tx)
	if err != nil {
		return
	}
	// 检查一下当前的part是不是重复上传的，如果是的话可能数据大小已经发生了变化，那么需要更新一下bucket的使用量，
	// part.Size-removedSize 是个前后差值
	var removedSize int64 = 0
	if part, ok := multipart.Parts[part.PartNumber]; ok {
		removedSize += part.Size
	}
	// part.Size-removedSize 可能是0，0的时候没有任何意义，而且大部分情况下都是0，为啥不加个判断？？？？
	err = m.Client.UpdateUsage(multipart.BucketName, part.Size-removedSize, tx)
	if err != nil {
		return
	}
	err = m.Client.CommitTrans(tx)
	return
}

func (m *Meta) RenameObjectPart(object *Object, sourceObject string) (err error) {
	var tx *sql.Tx
	tx, err = m.Client.NewTrans()
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			m.Client.AbortTrans(tx)
		}
	}()
	err = m.Client.RenameObjectPart(object, sourceObject, tx)
	if err != nil {
		return err
	}
	err = m.Client.RenameObject(object, sourceObject, tx)
	if err != nil {
		return err
	}
	err = m.Client.CommitTrans(tx)
	return err
}
