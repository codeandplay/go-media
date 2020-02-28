package store

import (
	"context"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Store interface {
	Ping(context.Context) error
}

type mongoStore struct {
	client     *mongo.Client
	collection *mongo.Collection
}

// NewMongoStore return a pointer to newly create instance of mongoStore
func NewMongoStore(connetionString string, dbName string, collectionName string) (*mongoStore, error) {
	// Set client options
	clientOptions := options.Client().ApplyURI(connetionString)
	// connect to MongoDB
	client, err := mongo.Connect(context.TODO(), clientOptions)

	if err != nil {
		return nil, err
	}

	// Check the connection
	err = client.Ping(context.TODO(), nil)

	if err != nil {
		return nil, err
	}

	collection := client.Database(dbName).Collection(collectionName)
	return &mongoStore{
		client:     client,
		collection: collection,
	}, nil
}

func (m mongoStore) Ping(ctx context.Context) error {
	return m.client.Ping(ctx, nil)
}
