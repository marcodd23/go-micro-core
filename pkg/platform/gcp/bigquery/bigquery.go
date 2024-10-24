package bigquery

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"

	"google.golang.org/protobuf/reflect/protodesc"

	"cloud.google.com/go/bigquery/storage/managedwriter"
	"github.com/marcodd23/go-micro-core/pkg/logx"
	"github.com/pkg/errors"
	"google.golang.org/api/option"
	"google.golang.org/protobuf/proto"
)

// BqManager manages BigQuery clients and multiple managed streams for different tables.
type BqManager struct {
	client     *managedwriter.Client
	streams    map[string]*managedwriter.ManagedStream // Cache of streams by tablePath
	projectId  string
	isLocalEnv bool
	once       sync.Once
	streamMu   sync.Mutex // Protects the streams map
}

// NewBigQueryManager initializes and returns a new BqManager.
//
// Parameters:
// - projectId: The GCP project ID.
// - isLocalEnv: A boolean indicating if the environment is local (used to determine authentication method).
//
// Returns:
// - A pointer to the initialized BqManager.
func NewBigQueryManager(projectId string, isLocalEnv bool) *BqManager {
	return &BqManager{
		projectId:  projectId,
		isLocalEnv: isLocalEnv,
		streams:    make(map[string]*managedwriter.ManagedStream), // Initialize the map for streams
	}
}

// GetBigQueryWriteStream initializes and returns the BigQuery managed stream for the given table and dataset.
//
// This method manages stream caching, handles refreshing of streams, and initializes new streams if necessary.
//
// Parameters:
// - ctx: The context for managing request deadlines, cancellation signals, and other request-scoped values.
// - datasetId: The ID of the BigQuery dataset.
// - tableId: The ID of the BigQuery table.
// - message: A Protobuf message to extract the schema descriptor.
// - refreshStreamCache: A boolean flag indicating if the stream should be refreshed.
//
// Returns:
// - A pointer to the initialized BigQuery managed stream.
// - An error if the stream initialization fails.
func (bq *BqManager) GetBigQueryWriteStream(ctx context.Context, datasetId, tableId string, message proto.Message, refreshStreamCache bool) (*managedwriter.ManagedStream, error) {
	tablePath := fmt.Sprintf("projects/%s/datasets/%s/tables/%s", bq.projectId, datasetId, tableId)

	// Lock the map while interacting with the streams cache
	bq.streamMu.Lock()
	defer bq.streamMu.Unlock()

	// Handle retrieving or refreshing the cached stream
	if !refreshStreamCache {
		// If we do not need to refresh the stream, try to get it from the cache
		if stream, exists := bq.streams[tablePath]; exists {
			return stream, nil
		}

		logx.GetLogger().LogInfo(ctx, fmt.Sprintf("Creating new write stream for %s", tablePath))
	} else {
		logx.GetLogger().LogInfo(ctx, fmt.Sprintf("Refreshing the stream for %s", tablePath))

		// Refresh the stream by closing the old one and creating a new one
		if err := bq.closeStream(ctx, tablePath); err != nil {
			return nil, errors.Wrap(err, "error refreshing BigQuery stream")
		}
	}

	// Create a new stream if not found or after refreshing
	return bq.initializeWriteStream(ctx, tablePath, message)
}

// initializeWriteStream creates a new managed stream for a given table and caches it.
//
// Parameters:
// - ctx: The context for managing request deadlines, cancellation signals, and other request-scoped values.
// - tablePath: The full path of the table in BigQuery.
// - message: A Protobuf message used to derive the schema descriptor.
//
// Returns:
// - A pointer to the initialized managed stream.
// - An error if the managed stream cannot be created.
func (bq *BqManager) initializeWriteStream(ctx context.Context, tablePath string, message proto.Message) (*managedwriter.ManagedStream, error) {
	bqClient, err := bq.getBigQueryClient(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "error getting BigQuery client")
	}

	// Get the Protobuf schema descriptor from the Protobuf message
	schemaDescriptor := protodesc.ToDescriptorProto(message.ProtoReflect().Descriptor())

	logx.GetLogger().LogInfo(ctx, fmt.Sprintf("Initializing BigQuery managed stream for %s", tablePath))

	stream, err := bqClient.NewManagedStream(ctx, managedwriter.WithDestinationTable(tablePath), managedwriter.WithSchemaDescriptor(schemaDescriptor))
	if err != nil {
		return nil, errors.Wrap(err, "error creating BigQuery managed stream")
	}

	// Cache the new stream for future reuse
	bq.streams[tablePath] = stream

	return stream, nil
}

// getBigQueryClient initializes the BigQuery client if it doesn't exist and returns it.
//
// This method ensures the client is created only once using sync.Once.
//
// Parameters:
// - ctx: The context for managing request deadlines, cancellation signals, and other request-scoped values.
//
// Returns:
// - A pointer to the BigQuery managed writer client.
// - An error if the client initialization fails.
func (bq *BqManager) getBigQueryClient(ctx context.Context) (*managedwriter.Client, error) {
	var err error

	bq.once.Do(func() {
		logx.GetLogger().LogInfo(ctx, "Initializing BigQuery client")

		bq.client, err = bq.newBigQueryClient(ctx)
		if err != nil {
			err = errors.WithMessage(err, "error creating BigQuery Storage client")
		}
	})

	return bq.client, err
}

// newBigQueryClient initializes the BigQuery client with the service configuration.
//
// This method handles authentication based on whether the environment is local or remote.
// For local environments, it uses a credentials file.
//
// Parameters:
// - ctx: The context for managing request deadlines, cancellation signals, and other request-scoped values.
//
// Returns:
// - A pointer to the initialized BigQuery client.
// - An error if the client initialization fails.
func (bq *BqManager) newBigQueryClient(ctx context.Context) (*managedwriter.Client, error) {
	var client *managedwriter.Client

	var err error

	if bq.isLocalEnv {
		client, err = managedwriter.NewClient(ctx, bq.projectId, option.WithCredentialsFile(os.Getenv("GOOGLE_APPLICATION_CREDENTIALS")))
	} else {
		client, err = managedwriter.NewClient(ctx, bq.projectId)
	}

	if err != nil {
		return nil, errors.WithMessage(err, "error initializing BigQuery client")
	}

	return client, nil
}

// closeStream closes a stream for the given tablePath if it exists in the cache.
//
// Parameters:
// - ctx: The context for managing request deadlines, cancellation signals, and other request-scoped values.
// - tablePath: The full path of the table in BigQuery.
//
// Returns:
// - An error if the stream cannot be closed properly.
func (bq *BqManager) closeStream(ctx context.Context, tablePath string) error {
	if oldStream, exists := bq.streams[tablePath]; exists {
		err := oldStream.Close()
		if err != nil && !errors.Is(err, io.EOF) {
			logx.GetLogger().LogError(ctx, fmt.Sprintf("Error closing old BigQuery stream for %s", tablePath), err)

			return errors.Wrapf(err, "Error closing old BigQuery stream for %s", tablePath)
		}

		logx.GetLogger().LogInfo(ctx, fmt.Sprintf("Old BigQuery stream closed successfully for %s", tablePath))

		delete(bq.streams, tablePath) // Remove the old stream from the cache
	}

	return nil
}

// CloseAll closes all the BigQuery managed streams and the client.
//
// This method ensures that both the managed streams and the client are closed gracefully. It handles cases where the
// stream or client might already be closed and suppresses logging for io.EOF errors.
//
// Parameters:
// - ctx: The context for managing request deadlines, cancellation signals, and other request-scoped values.
func (bq *BqManager) CloseAll(ctx context.Context) {
	// Print stack trace at the start of the shutdown process
	logx.GetLogger().LogInfo(ctx, "Starting CloseAll method for BigQuery manager")

	// Lock the map while closing streams to avoid race conditions
	bq.streamMu.Lock()
	defer bq.streamMu.Unlock()

	// Close all cached streams
	for tablePath, stream := range bq.streams {
		logx.GetLogger().LogInfo(ctx, fmt.Sprintf("About to close BigQuery managed stream for %s", tablePath))

		err := stream.Close()
		if err != nil && !errors.Is(err, io.EOF) { // Suppress logging for EOF
			logx.GetLogger().LogError(ctx, fmt.Sprintf("Error closing BigQuery managed stream for %s", tablePath), err)
		} else {
			logx.GetLogger().LogInfo(ctx, fmt.Sprintf("BigQuery stream closed gracefully for %s", tablePath))
		}
	}

	// Clear the streams map after closing
	bq.streams = make(map[string]*managedwriter.ManagedStream)

	// Close the client
	if bq.client != nil {
		logx.GetLogger().LogInfo(ctx, "Closing BigQuery client")

		err := bq.client.Close()
		if err != nil {
			logx.GetLogger().LogError(ctx, "Error closing BigQuery client", err)
		}
	}

	logx.GetLogger().LogInfo(ctx, "CloseAll method for BigQuery manager completed")
}
