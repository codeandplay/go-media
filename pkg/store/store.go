package store

import (
	"context"
	"errors"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"ray.vhatt/todo-gokit/pkg/models"
)

type Store interface {
	Ping(context.Context) error
	InsertToDo(context.Context, models.ToDoItem) (string, error)
	CompleteToDo(context.Context, string) (string, error)
	UnDoToDo(context.Context, string) (string, error)
	DeleteToDo(context.Context, string) (string, error)
	GetAllToDo(context.Context) ([]models.ToDoItem, error)
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

func (m mongoStore) InsertToDo(ctx context.Context, task models.ToDoItem) (string, error) {
	insertResult, err := m.collection.InsertOne(ctx, task)

	if err != nil {
		return "", err
	}
	objID, ok := insertResult.InsertedID.(primitive.ObjectID)

	if !ok {
		return "", errors.New("Malform InsertID")
	}

	return objID.Hex(), nil
}

func (m mongoStore) CompleteToDo(ctx context.Context, taskId string) (string, error) {
	id, err := primitive.ObjectIDFromHex(taskId)
	if err != nil {
		return "", err
	}

	filter := bson.M{"_id": id}
	update := bson.M{"$set": bson.M{"status": true}}
	_, err = m.collection.UpdateOne(ctx, filter, update)
	if err != nil {
		return "", err
	}
	return taskId, nil
}

func (m mongoStore) UnDoToDo(ctx context.Context, taskId string) (string, error) {
	id, err := primitive.ObjectIDFromHex(taskId)
	if err != nil {
		return "", err
	}
	filter := bson.M{"_id": id}
	update := bson.M{"$set": bson.M{"status": false}}
	_, err = m.collection.UpdateOne(ctx, filter, update)
	if err != nil {
		return "", err
	}
	return taskId, nil
}

func (m mongoStore) DeleteToDo(ctx context.Context, taskId string) (string, error) {
	id, err := primitive.ObjectIDFromHex(taskId)
	if err != nil {
		return "", err
	}

	filter := bson.M{"_id": id}
	_, err = m.collection.DeleteOne(ctx, filter)
	if err != nil {
		return "", err
	}
	return taskId, nil
}

func (m mongoStore) GetAllToDo(ctx context.Context) ([]models.ToDoItem, error) {
	cur, err := m.collection.Find(ctx, bson.D{{}})
	if err != nil {
		return nil, err
	}

	defer cur.Close(ctx)

	var results []models.ToDoItem
	for cur.Next(ctx) {
		var result models.ToDoItem
		err = cur.Decode(&result)
		if err != nil {
			return nil, err
		}
		results = append(results, result)
	}

	if err := cur.Err(); err != nil {
		return nil, err
	}
	return results, nil
}
