package compressx

import (
	"bytes"
	"context"
	"github.com/marcodd23/go-micro-core/pkg/logx"
	"io"

	"github.com/pkg/errors"

	"compress/gzip"
	"net/http"
)

// GzipDecompressIfNeeded decompresses the data if it's gzipped
func GzipDecompressIfNeeded(ctx context.Context, data []byte) ([]byte, error) {
	if isGzipped(ctx, data) {
		reader, err := gzip.NewReader(bytes.NewReader(data))
		if err != nil {
			return nil, errors.WithMessage(err, "failed to create gzip reader")
		}

		defer func(reader *gzip.Reader) {
			err := reader.Close()
			if err != nil {
				logx.GetLogger().LogError(ctx, "Error closing Gzip Reader")
			}
		}(reader)

		decompressedData, err := io.ReadAll(reader)
		if err != nil {
			return nil, errors.WithMessage(err, "failed to read gzipped data")
		}

		return decompressedData, nil
	}

	return data, nil
}

// isGzipped checks if the data is compressed with Gzip
func isGzipped(ctx context.Context, data []byte) bool {
	contentType := http.DetectContentType(data)
	return contentType == "application/x-gzip"
}

// GzipCompressJSON compresses a JSON-encoded byte slice using gzip.
func GzipCompressJSON(ctx context.Context, jsonData []byte) ([]byte, error) {
	var compressedBuffer bytes.Buffer
	gzipWriter := gzip.NewWriter(&compressedBuffer)

	_, err := gzipWriter.Write(jsonData)
	if err != nil {
		return nil, errors.WithMessage(err, "failed to write JSON data to gzip writer")
	}

	err = gzipWriter.Close()
	if err != nil {
		return nil, errors.WithMessage(err, "failed to close gzip writer")
	}

	return compressedBuffer.Bytes(), nil
}
