package pubsub

import (
	"context"
	"fmt"
	"os"
	"testing"

	"cloud.google.com/go/pubsub"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	pubSubEmulatorImage = "gcr.io/google.com/cloudsdktool/cloud-sdk:latest"
	pubSubEmulatorPort  = "8085/tcp"
	pubSubEmulatorHost  = "PUBSUB_EMULATOR_HOST"
)

// PubsubContainer represents the cloud_pubsub container type used in the module.
type PubsubContainer struct {
	Container testcontainers.Container
	URI       string
	// ConnOptions []option.ClientOption
	client    *pubsub.Client
	projectId string
}

// StartPubSubContainer - startContainer creates an instance of the cloud_pubsub container type.
func StartPubSubContainer(ctx context.Context, projectId string) (*PubsubContainer, error) {
	req := testcontainers.ContainerRequest{
		Image:        pubSubEmulatorImage,
		ExposedPorts: []string{pubSubEmulatorPort},
		WaitingFor:   wait.ForLog("started"),
		Cmd: []string{
			"/bin/sh",
			"-c",
			"gcloud beta emulators pubsub start --host-port 0.0.0.0:8085",
		},
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})

	if err != nil {
		return nil, err
	}

	mappedPort, err := container.MappedPort(ctx, "8085")
	if err != nil {
		return nil, err
	}

	hostIP, err := container.Host(ctx)
	if err != nil {
		return nil, err
	}

	uri := fmt.Sprintf("%s:%s", hostIP, mappedPort.Port())

	os.Setenv(pubSubEmulatorHost, uri)

	return &PubsubContainer{Container: container, URI: uri, projectId: projectId}, nil
}

func (c *PubsubContainer) StopContainer(ctx context.Context) error {
	os.Unsetenv(pubSubEmulatorHost)
	return c.Container.Terminate(ctx)
}

func (c *PubsubContainer) CreateConnectionOptions(t *testing.T) []option.ClientOption {
	// Create connection and Client
	conn, err := grpc.Dial(c.URI, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	options := []option.ClientOption{option.WithGRPCConn(conn)}

	return options
}

func (c *PubsubContainer) getClient(ctx context.Context, t *testing.T, connOpt []option.ClientOption) *pubsub.Client {
	if c.client == nil {
		client, err := pubsub.NewClient(ctx, c.projectId, connOpt...)
		if err != nil {
			t.Fatal(err)
		}

		c.client = client
	}

	return c.client
}

func (c *PubsubContainer) CloseClient(ctx context.Context, t *testing.T) {
	err := c.client.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func (c *PubsubContainer) CreateTopic(ctx context.Context, t *testing.T, topicName string) *pubsub.Topic {
	t.Helper()
	// Get conn options
	options := c.CreateConnectionOptions(t)

	client := c.getClient(ctx, t, options)
	// client, err := cloud_pubsub.NewClient(ctx, c.projectId, options...)
	// if err != nil {
	//     t.Fatal(err)
	// }
	// defer func ()  {
	//     client.Close()
	// }()

	// Create topic
	topic, err := client.CreateTopic(ctx, topicName)
	if err != nil {
		t.Fatal(err)
	}

	return topic
}

func (c *PubsubContainer) CreateTopicAndSubscription(ctx context.Context, t *testing.T, topicName string) *pubsub.Topic {
	t.Helper()
	// Get conn options
	options := c.CreateConnectionOptions(t)

	client := c.getClient(ctx, t, options)
	// client, err := cloud_pubsub.NewClient(ctx, c.projectId, options...)
	// if err != nil {
	//     t.Fatal(err)
	// }
	// defer func ()  {
	//     client.Close()
	// }()

	// Create topic
	topic, err := client.CreateTopic(ctx, topicName)
	if err != nil {
		t.Fatal(err)
	}

	return topic
}

func (c *PubsubContainer) CreateSubscription(ctx context.Context, t *testing.T, topicName string, subscriptionName string) *pubsub.Subscription {
	// Get conn options
	options := c.CreateConnectionOptions(t)

	// client, err := cloud_pubsub.NewClient(ctx, c.projectId, options...)
	// if err != nil {
	//     t.Fatal(err)
	// }
	client := c.getClient(ctx, t, options)

	topic := client.Topic(topicName)

	//Create subscription
	subscription, err := client.CreateSubscription(ctx, "subscription",
		pubsub.SubscriptionConfig{Topic: topic})
	if err != nil {
		t.Fatal(err)
	}

	return subscription
}
