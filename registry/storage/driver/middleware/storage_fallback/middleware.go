package storage_fallback

import (
	"context"
	"fmt"
	dcontext "github.com/docker/distribution/context"
	storagedriver "github.com/docker/distribution/registry/storage/driver"
	"github.com/docker/distribution/registry/storage/driver/factory"
	storagemiddleware "github.com/docker/distribution/registry/storage/driver/middleware"
	"io"
)

type fallbackStorageDriver struct {
	storagedriver.StorageDriver
	Fallback storagedriver.StorageDriver
}

func (sd *fallbackStorageDriver) Name() string {
	return fmt.Sprintf("%v -> %v", sd.StorageDriver.Name(), sd.Fallback.Name())
}

func (sd *fallbackStorageDriver) GetContent(ctx context.Context, path string) ([]byte, error) {
	result, err := sd.StorageDriver.GetContent(ctx, path)
	if err != nil {
		dcontext.GetLogger(ctx).WithError(err).Warnf("GetContent(%v): falling back to %v", path, sd.Fallback.Name())
		return sd.Fallback.GetContent(ctx, path)
	}
	return result, err
}

func (sd *fallbackStorageDriver) Reader(ctx context.Context, path string, offset int64) (io.ReadCloser, error) {
	result, err := sd.StorageDriver.Reader(ctx, path, offset)
	if err != nil {
		dcontext.GetLogger(ctx).WithError(err).Warnf("Reader(%v, %v): falling back to %v", path, offset, sd.Fallback.Name())
		return sd.Fallback.Reader(ctx, path, offset)
	}
	return result, err
}

func (sd *fallbackStorageDriver) Stat(ctx context.Context, path string) (storagedriver.FileInfo, error) {
	result, err := sd.StorageDriver.Stat(ctx, path)
	if err != nil {
		dcontext.GetLogger(ctx).WithError(err).Warnf("Stat(%v): falling back to %v", path, sd.Fallback.Name())
		return sd.Fallback.Stat(ctx, path)
	}
	return result, err
}

func (sd *fallbackStorageDriver) URLFor(ctx context.Context, path string, options map[string]interface{}) (string, error) {
	if _, err := sd.StorageDriver.Stat(ctx, path); err != nil {
		dcontext.GetLogger(ctx).WithError(err).Warnf("URLFor(%v): Stat() failed, falling back to %v", path, sd.Fallback.Name())
		return sd.Fallback.URLFor(ctx, path, options)
	}

	return sd.StorageDriver.URLFor(ctx, path, options)
}

func newFallbackStorageDriver(sd storagedriver.StorageDriver, options map[string]interface{}) (storagedriver.StorageDriver, error) {
	driverName, ok := options["driver"].(string)
	if !ok {
		driverName, ok = options["driverName"].(string)  // TODO: fully deprecate driverName
	}
	if !ok {
		return nil, fmt.Errorf("failed to extract driver or driverName from options")
	}

	fallback, err := factory.Create(driverName, options)

	if err != nil {
		return nil, err
	}

	return &fallbackStorageDriver{StorageDriver: sd, Fallback: fallback}, nil
}

func init() {
	storagemiddleware.Register("storage_fallback", storagemiddleware.InitFunc(newFallbackStorageDriver))
}
