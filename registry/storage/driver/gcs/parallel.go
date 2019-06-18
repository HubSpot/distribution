package gcs

import (
	"bytes"
	"encoding/json"
	"fmt"
	storagedriver "github.com/docker/distribution/registry/storage/driver"
	"google.golang.org/api/googleapi"
	"google.golang.org/cloud"
	"google.golang.org/cloud/storage"
	"io"
	"net/url"
	"sync"
)

type chunk struct {
	buf   []byte
	start int64
}

type parallelWriter struct {
	driver      *driver
	path        string
	wg          *sync.WaitGroup
	chunkCh     chan<- chunk
	doneCh      chan struct{}
	buf         []byte
	offset      int
	startOffset int64
	totalSize   int64
	closed      bool
	cancelled   bool
}

func (pw *parallelWriter) Write(p []byte) (n int, err error) {
	for n = 0; n < len(p); {
		if pw.closed {
			return n, fmt.Errorf("Wrote to closed writer") // TODO: better err?
		}
		if pw.buf == nil {
			pw.buf = pw.driver.pool.Get().([]byte)
			pw.startOffset = pw.totalSize
			pw.offset = 0
		}
		nn := copy(pw.buf[pw.offset:], p[n:])
		n += nn
		pw.offset += nn
		pw.totalSize += int64(nn)
		if pw.offset == cap(pw.buf) {
			pw.chunkCh <- chunk{
				start: pw.startOffset,
				buf:   pw.buf,
			}
			pw.buf = nil
		}
	}
	return n, nil
}

func (pw *parallelWriter) Close() error {
	if pw.closed {
		return nil
	}
	pw.closed = true

	if pw.offset > 0 {
		pw.chunkCh <- chunk{
			start: pw.startOffset,
			buf:   pw.buf[:pw.offset],
		}
		pw.startOffset = pw.totalSize
		pw.offset = 0
		pw.buf = nil
	}

	close(pw.doneCh)
	pw.wg.Wait()
	return nil
}

func (pw parallelWriter) Size() int64 {
	return pw.totalSize
}

func (pw *parallelWriter) Cancel() error {
	pw.Close()

	if pw.cancelled {
		return nil
	}
	pw.cancelled = true

	gcsContext := cloud.NewContext(dummyProjectID, pw.driver.client)

	objects, err := storageListObjects(gcsContext, pw.driver.bucket, &storage.Query{Prefix: pw.driver.pathToKey(pw.path)})
	if err != nil {
		return err
	}

	var errors []error
	for i := range objects.Results {
		if err := storageDeleteObject(gcsContext, pw.driver.bucket, objects.Results[i].Name); err != nil {
			errors = append(errors, err)
		}
	}

	if len(errors) > 0 {
		return errors[0]
	}

	return nil
}

type sourceObject struct {
	Name       string `json:"name"`
	Generation int64  `json:"generation"`
}

type composeRequest struct {
	Kind          string         `json:"kind"`
	SourceObjects []sourceObject `json:"sourceObjects"`
}

func composeObjects(objects []*storage.Object) *composeRequest {
	cr := &composeRequest{
		Kind:          "storage#composeRequest",
		SourceObjects: make([]sourceObject, 0, len(objects)),
	}

	for i := range objects {
		cr.SourceObjects = append(cr.SourceObjects, sourceObject{objects[i].Name, objects[i].Generation})
	}

	return cr
}

func (pw *parallelWriter) Commit() error {
	pw.Close()

	gcsContext := cloud.NewContext(dummyProjectID, pw.driver.client)

	pathKey := pw.driver.pathToKey(pw.path)

	objects, err := storageListObjects(gcsContext, pw.driver.bucket, &storage.Query{Prefix: pathKey})
	if err != nil {
		return err
	}

	if len(objects.Results) == 0 {
		return fmt.Errorf("No objects found for %v", pw.path)
	} else if len(objects.Results) == 1 {
		return nil // don't need to compose one object
	}

	jsonBytes, err := json.Marshal(composeObjects(objects.Results))
	if err != nil {
		return err
	}
	resp, err := pw.driver.client.Post(fmt.Sprintf("https://www.googleapis.com/storage/v1/b/%v/o/%v/compose", pw.driver.bucket, url.PathEscape(pathKey)), "application/json", bytes.NewReader(jsonBytes))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return googleapi.CheckMediaResponse(resp)
}

type WriterFunc func(int64) (storagedriver.FileWriter, error)

func NewParallelWriter(driver *driver, path string, writerFunc WriterFunc, workers int) *parallelWriter {
	wg := &sync.WaitGroup{}
	chunkCh := make(chan chunk, 1)
	doneCh := make(chan struct{})

	pw := &parallelWriter{
		driver:  driver,
		wg:      wg,
		chunkCh: chunkCh,
		doneCh:  doneCh,
		path:    path,
	}

	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			for {
				select {
				case c := <-chunkCh:
					w, err := writerFunc(c.start)
					if err != nil {
						pw.Cancel()
					}
					if _, err := io.Copy(w, bytes.NewReader(c.buf)); err != nil {
						pw.Cancel()
					}
					if err := w.Commit(); err != nil {
						pw.Cancel()
					}
					driver.pool.Put(c.buf[:cap(c.buf)])
					continue
				case <-doneCh:
					return
				}
			}
		}()
	}

	return pw
}
