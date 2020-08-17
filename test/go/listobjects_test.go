package _go

import (
	"testing"

	"github.com/journeymidnight/aws-sdk-go/service/s3"
	. "github.com/journeymidnight/yig/test/go/lib"
)

type TestListObjectsCase struct {
	BucketName   string
	Key          string
	Value        string
	StorageClass string
	Expected     string
}

func Test_ListObjects_With_StorageClass(t *testing.T) {
	testCases := []TestListObjectsCase{
		{TestBucket, TestKey, TestValue, s3.ObjectStorageClassStandard, s3.ObjectStorageClassStandard},
		{TestBucket, TestKey, TestValue, s3.ObjectStorageClassStandardIa, s3.ObjectStorageClassStandardIa},
		{TestBucket, TestKey, TestValue, s3.ObjectStorageClassGlacier, s3.ObjectStorageClassGlacier},
	}
	sc := NewS3()
	defer sc.CleanEnv()
	for _, c := range testCases {
		sc.CleanEnv()
		err := sc.MakeBucket(c.BucketName)
		if err != nil {
			t.Fatal("MakeBucket err:", err)
		}
		err = sc.PutObjectWithStorageClass(c.BucketName, c.Key, c.Value, c.StorageClass)
		if err != nil {
			t.Fatal("PutObjectWithStorageClass err:", err)
		}
		out, err := sc.ListObjects(c.BucketName, "", "", 1000)
		for _, object := range out.Contents {
			if *object.Key == c.Key {
				if *object.StorageClass != c.StorageClass {
					t.Fatal("StorageClass is not correct. out:", *object.StorageClass, "expected:", c.Expected)
				}
			}
		}
	}
}
