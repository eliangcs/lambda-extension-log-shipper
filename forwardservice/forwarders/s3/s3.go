package s3

import (
	"fmt"
	"io"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/david7482/lambda-extension-log-shipper/forwardservice"
	"github.com/david7482/lambda-extension-log-shipper/logservice"
)

type S3 struct {
	cfg    config
	logger zerolog.Logger
}

type config struct {
	Enable *bool
}

func New() *S3 {
	return &S3{
		logger: zerolog.New(os.Stdout).With().Str("forwarder", "s3").Timestamp().Logger(),
	}
}

func (s *S3) SetupConfigs(app *kingpin.Application) {
	s.cfg.Enable = app.
		Flag("s3-enable", "Enable the S3 forwarder").
		Envar("LS_S3_ENABLE").
		Default("true").Bool()
}

func (s *S3) Init(params forwardservice.ForwarderParams) {
}

func (s *S3) IsEnable() bool {
	return *s.cfg.Enable
}

func (s *S3) SendLog(logs []logservice.Log) {
	bucket := "sandbox-558"
	fname := os.Getenv("AWS_LAMBDA_FUNCTION_NAME")
	ts := time.Now().UnixMilli()
	key := fmt.Sprintf("%s-%d-%s.log", fname, ts, uuid.New())

	sess, err := session.NewSession(&aws.Config{
		Region: aws.String("us-east-1"),
	})
	if err != nil {
		fmt.Printf("Unable to create session %v\n", err)
	}

	uploader := s3manager.NewUploader(sess)

	reader := NewLogReader(logs)

	_, err = uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(bucket),

		// Can also use the `filepath` standard library package to modify the
		// filename as need for an S3 object key. Such as turning absolute path
		// to a relative path.
		Key: aws.String(key),

		// The file to be uploaded. io.ReadSeeker is preferred as the Uploader
		// will be able to optimize memory when uploading large content. io.Reader
		// is supported, but will require buffering of the reader's bytes for
		// each part.
		Body: reader,
	})
	if err != nil {
		fmt.Printf("Unable to upload %s\n", key)
	} else {
		fmt.Printf("Successfully upload %s\n", key)
	}
}

func (s *S3) Shutdown() {
}

type LogReader struct {
	logs          []logservice.Log
	nextLogIndex  int
	nextByteIndex int
}

func (reader *LogReader) Read(p []byte) (int, error) {
	numLogs := len(reader.logs)
	if reader.nextLogIndex >= numLogs {
		return 0, io.EOF
	}

	lastIndex := numLogs - 1
	if reader.nextLogIndex == lastIndex {
		numBytes := len(reader.logs[lastIndex].Content)
		if reader.nextByteIndex >= numBytes {
			return 0, io.EOF
		}
	}

	plen := len(p)
	numBytesRead := 0

	for _, log := range reader.logs[reader.nextLogIndex:] {
		for _, b := range log.Content[reader.nextByteIndex:] {
			p[numBytesRead] = b
			numBytesRead++
			reader.nextByteIndex++
			if numBytesRead == plen {
				break
			}
		}
		if numBytesRead == plen {
			break
		}
		reader.nextByteIndex = 0
		reader.nextLogIndex++
	}

	return numBytesRead, nil
}

func NewLogReader(logs []logservice.Log) io.Reader {
	return &LogReader{logs: logs, nextLogIndex: 0, nextByteIndex: 0}
}
