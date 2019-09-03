package seaweed

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/journeymidnight/seaweedfs/weed/operation"
	"github.com/journeymidnight/seaweedfs/weed/wdclient"
	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/log"
	"google.golang.org/grpc"
	"io"
	"mime/multipart"
	"net/http"
	"strings"
	"time"
)

const ObjectSizeLimit = 30 << 20 // 30M, limit introduced by cannyls

type uploadResult struct {
	Name  string `json:"name,omitempty"`
	Size  uint32 `json:"size,omitempty"`
	Error string `json:"error,omitempty"`
	ETag  string `json:"eTag,omitempty"`
}

// Storage implements yig.storage.backend
type Storage struct {
	logger        *log.Logger
	masters       []string
	seaweedClient *wdclient.MasterClient
	httpClient    *http.Client
}

func NewSeaweedStorage(logger *log.Logger, config helper.Config) Storage {
	clientId := fmt.Sprintf("YIG-%s", config.InstanceId)
	logger.Logger.Println("Initializing Seaweedfs client:", clientId,
		"masters:", config.SeaweedMasters)
	seaweedClient := wdclient.NewMasterClient(context.Background(),
		grpc.WithInsecure(), clientId, config.SeaweedMasters)
	go seaweedClient.KeepConnectedToMaster()
	seaweedClient.WaitUntilConnected() // FIXME some kind of timeout?
	logger.Logger.Println("Seaweedfs client initialized")
	return Storage{
		logger:        logger,
		masters:       config.SeaweedMasters,
		seaweedClient: seaweedClient,
		httpClient: &http.Client{
			Timeout: 3 * time.Minute,
			Transport: &http.Transport{
				MaxIdleConnsPerHost: 8192, // keep long connections with volume server
			},
		},
	}
}

func (s Storage) ClusterID() string {
	return strings.Join(s.masters, ",")
}

func (s Storage) assignObject(poolName string) (result operation.AssignResult, err error) {
	masterAddress := s.seaweedClient.GetMaster()
	assignRequest := &operation.VolumeAssignRequest{
		// TODO read from config
		Count:       1,
		Replication: "002",
		Collection:  poolName,
		Ttl:         "",
		DataCenter:  "",
	}
	assignResult, err := operation.Assign(masterAddress, nil,
		assignRequest)
	if err != nil {
		return operation.AssignResult{}, err
	}
	if assignResult.Error != "" {
		return operation.AssignResult{}, errors.New(assignResult.Error)
	}
	return *assignResult, nil
}

func (s Storage) Put(poolName string, object io.Reader) (objectName string,
	bytesWritten uint64, err error) {

	assigned, err := s.assignObject(poolName)
	if err != nil {
		s.logger.Logger.Println("assignObject error:", err)
		return "", 0, err
	}
	url := fmt.Sprintf("http://%s/%s", assigned.Url, assigned.Fid)
	// limit object size because of cannlys
	object = io.LimitReader(object, ObjectSizeLimit)

	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	part, err := writer.CreateFormFile("file", assigned.Fid)
	if err != nil {
		s.logger.Logger.Println("CreateFormFile error:", err)
		return "", 0, err
	}
	n, err := io.Copy(part, object)
	if err != nil {
		s.logger.Logger.Println("io.Copy error:", err)
		return "", 0, err
	}
	err = writer.Close()
	if err != nil {
		s.logger.Logger.Println("writer.Close error:", err)
		return "", 0, err
	}

	req, err := http.NewRequest("POST", url, body)
	if err != nil {
		s.logger.Logger.Println("http.NewRequest error:", err)
		return "", 0, err
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())

	resp, err := s.httpClient.Do(req)
	if err != nil {
		s.logger.Logger.Println("s.httpClient.Do error:", err)
		return "", 0, err
	}
	var result uploadResult
	err = helper.ReadJsonBody(resp.Body, &result)
	if err != nil {
		s.logger.Logger.Println("ReadJsonBody error:", err)
		return "", 0, err
	}
	if result.Error != "" {
		return "", 0, errors.New(result.Error)
	}
	return assigned.Fid, uint64(n), nil
}

func (s Storage) Append(poolName, existName string, objectChunk io.Reader,
	offset int64) (objectName string, bytesWritten uint64, err error) {
	// TODO
	return "", 0, nil
}

func (s Storage) GetReader(poolName, objectName string,
	offset int64, length uint64) (reader io.ReadCloser, err error) {

	url, err := s.seaweedClient.LookupFileId(objectName)
	if err != nil {
		s.logger.Logger.Println("seaweedClient.LookupFileId error:", err)
		return nil, err
	}
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	if length != 0 {
		req.Header.Set("X-Start-Offset", fmt.Sprint(offset))
		req.Header.Set("X-Length-Required", fmt.Sprint(length))
	}
	resp, err := s.httpClient.Do(req)
	if err != nil {
		s.logger.Logger.Println("httpClient.Get error:", err)
		return nil, err
	}
	return resp.Body, nil
}

// Corresponding to weed/server/volume_server_handlers_write.go,
// function DeleteHandler, writeDeleteResult
func (s Storage) Remove(poolName, objectName string) (err error) {
	url, err := s.seaweedClient.LookupFileId(objectName)
	if err != nil {
		s.logger.Logger.Println("seaweedClient.LookupFileId error:", err)
		return err
	}
	req, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		s.logger.Logger.Println("http.NewRequest error:", err)
		return err
	}
	resp, err := s.httpClient.Do(req)
	if err != nil {
		s.logger.Logger.Println("httpClient.Get error:", err)
		return err
	}
	var result map[string]interface{}
	err = helper.ReadJsonBody(resp.Body, &result)
	if err != nil {
		s.logger.Logger.Println("ReadJsonBody error:", err)
		return err
	}
	if resp.StatusCode == http.StatusAccepted {
		return nil
	}
	return errors.New(fmt.Sprintln(result["error"]))
}