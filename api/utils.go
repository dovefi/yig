/*
 * Minio Cloud Storage, (C) 2015 Minio, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package api

import (
	"encoding/base64"
	"encoding/xml"
	"errors"
	"io"
	"net/http"
	"regexp"
	"strings"

	. "github.com/journeymidnight/yig/brand"
	"github.com/journeymidnight/yig/crypto"
	"github.com/journeymidnight/yig/storage"
)

// xmlDecoder provide decoded value in xml.
func xmlDecoder(body io.Reader, v interface{}) error {
	d := xml.NewDecoder(body)
	return d.Decode(v)
}

// checkValidMD5 - verify if valid md5, returns md5 in bytes.
func checkValidMD5(md5 string) ([]byte, error) {
	return base64.StdEncoding.DecodeString(strings.TrimSpace(md5))
}

// isMaxObjectSize - verify if max object size
func isMaxObjectSize(size int64) bool {
	return size > storage.MAX_PART_SIZE
}

// Check if part size is more than or equal to minimum allowed size.
func isMinAllowedPartSize(size int64) bool {
	return size >= storage.MIN_PART_SIZE
}

// isMaxPartNumber - Check if part ID is greater than the maximum allowed ID.
func isMaxPartID(partID int) bool {
	return partID > storage.MAX_PART_NUMBER
}

func contains(stringList []string, element string) bool {
	for _, e := range stringList {
		if e == element {
			return true
		}
	}
	return false
}

// We support '.' with bucket names but we fallback to using path
// style requests instead for such buckets.
var (
	validBucketName       = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9\.\-\_\:]{1,61}[A-Za-z0-9]$`)
	validBucketNameStrict = regexp.MustCompile(`^[a-z0-9][a-z0-9\.\-]{1,61}[a-z0-9]$`)
	ipAddress             = regexp.MustCompile(`^(\d+\.){3}\d+$`)
)

// Common checker for both stricter and basic validation.
func checkBucketNameCommon(bucketName string, strict bool) (err error) {
	if strings.TrimSpace(bucketName) == "" {
		return errors.New("Bucket name cannot be empty")
	}
	if len(bucketName) < 3 {
		return errors.New("Bucket name cannot be smaller than 3 characters")
	}
	if len(bucketName) > 63 {
		return errors.New("Bucket name cannot be greater than 63 characters")
	}
	if ipAddress.MatchString(bucketName) {
		return errors.New("Bucket name cannot be an ip address")
	}
	if strings.Contains(bucketName, "..") {
		return errors.New("Bucket name contains invalid characters")
	}
	if strict {
		if !validBucketNameStrict.MatchString(bucketName) {
			err = errors.New("Bucket name contains invalid characters")
		}
		return err
	}
	if !validBucketName.MatchString(bucketName) {
		err = errors.New("Bucket name contains invalid characters")
	}
	return err
}

// CheckValidBucketName - checks if we have a valid input bucket name.
func CheckValidBucketName(bucketName string) (err error) {
	return checkBucketNameCommon(bucketName, false)
}

func xmlFormat(data interface{}) ([]byte, error) {
	buffer, err := xml.Marshal(data)
	if err != nil {
		return nil, err
	}
	// add XML header
	headerBytes := []byte(xml.Header)
	output := append(headerBytes, buffer...)
	return output, nil
}

func setXmlHeader(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/xml")
}

// hasServerSideEncryptionHeader returns true if the given HTTP header
// contains server-side-encryption.
func hasServerSideEncryptionHeader(header http.Header, brand Brand) bool {
	return crypto.S3.IsRequested(header, brand) || crypto.SSEC.IsRequested(header, brand)
}
