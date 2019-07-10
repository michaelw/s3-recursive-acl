package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"runtime"
	"sync/atomic"

	"github.com/pkg/errors"

	"github.com/hashicorp/go-cleanhttp"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"

	"github.com/michaelw/mars"
)

func main() {
	var (
		region      = flag.String("region", "ap-northeast-1", "AWS region")
		bucket      = flag.String("bucket", "s3-bucket", "Bucket name")
		path        = flag.String("path", "/", "Path to recurse under")
		cannedACL   = flag.String("acl", "public-read", "Canned ACL to assign objects")
		dryRun      = flag.Bool("dryrun", true, "dry run, do not change permissions")
		concurrency = flag.Int("p", runtime.GOMAXPROCS(0), "concurrency level")
	)
	flag.Parse()

	tr := cleanhttp.DefaultPooledTransport()
	tr.MaxConnsPerHost = *concurrency
	svc := s3.New(session.Must(session.NewSession(&aws.Config{
		Region: region,
		// LogLevel: aws.LogLevel(aws.LogDebugWithRequestErrors | aws.LogDebugWithRequestRetries),
		HTTPClient: &http.Client{
			Transport: tr,
		},
	})))

	M := mars.New(context.Background(), mars.Config{
		Concurrency: *concurrency,
	})

	var counter int64
	// nolint:errcheck
	M.SubmitFn(func(ctx context.Context) error {
		defer M.Shutdown()
		return svc.ListObjectsPages(&s3.ListObjectsInput{
			Prefix: path,
			Bucket: bucket,
		}, func(page *s3.ListObjectsOutput, lastPage bool) bool {
			for _, obj := range page.Contents {
				obj := obj
				err := M.SubmitFn(func(ctx context.Context) error {
					atomic.AddInt64(&counter, 1)
					if *dryRun {
						log.Printf("[DRYRUN] Updating %q", *obj.Key)
						_, err := svc.GetObjectAclWithContext(ctx, &s3.GetObjectAclInput{
							Bucket: bucket,
							Key:    obj.Key,
						})
						return errors.Wrapf(err, "Failed to get permissions: %v", *obj.Key)
					} else {
						log.Printf("Updating %q", *obj.Key)
						_, err := svc.PutObjectAclWithContext(ctx, &s3.PutObjectAclInput{
							ACL:    cannedACL,
							Bucket: bucket,
							Key:    obj.Key,
						})
						return errors.Wrapf(err, "Failed to change permissions: %v", *obj.Key)
					}
				})
				if err != nil {
					return false
				}
			}
			log.Printf("Processing %d", atomic.LoadInt64(&counter))
			return true
		})
	})

	if err := M.Wait(); err != nil {
		log.Fatal(err)
	}

	log.Printf("Updated permissions on %d objects", atomic.LoadInt64(&counter))
}
