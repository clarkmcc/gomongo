package database

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/clarkmcc/gomongo/util"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// The DocStatus type is used to 'delete' documents in MongoDB. Documents are
// never actually deleted but instead are set to 'inactive' where they are
// filtered out in most other queries.
type DocStatus int8

const (
	Active   DocStatus = 1
	Inactive DocStatus = 0
)

type (
	// The BaseRepository is embedded into every other repository struct
	// and contains the shared MongoDB client (allows for sharing a MongoDB
	// connection instance between multiple repositories) as well as the
	// repositories collection name and the client.Database.
	BaseRepository struct {
		Client         *mongo.Client
		Database       *mongo.Database
		CollectionName string
	}

	// Every repository's root struct (DeviceRepository => Device,
	// EntityRepository => Entity) has the BaseDoc embedded into it which
	// provides the standard document fields, as well as every root struct
	// to implement the Document interface.
	BaseDoc struct {
		Id           primitive.ObjectID `json:"_id" bson:"_id"`
		Status       DocStatus          `json:"status"`
		CreatedDate  *time.Time         `json:"createdDate,omitempty" bson:"createdDate,omitempty"`
		ModifiedDate *time.Time         `json:"modifiedDate,omitempty" bson:"modifiedDate,omitempty"`
	}

	// The Document interface allows ever root struct to have common helper
	// functions, for example before inserting a new document into MongoDB
	// we call InitializeBaseDoc which creates a new primitive.ObjectID and
	// sets the CreatedDate and ModifiedDate
	Document interface {
		GetBaseDoc() *BaseDoc
		RemoveObjectId()
		InitializeBaseDoc()
	}

	// DEPRECATED: see BaseRepository.UpdateBase method
	MongoUpdateParams struct {
		Query    interface{}
		Update   interface{}
		Document Document
		Multi    bool
		Upsert   bool
	}
)

// Called by every other NewRepository function to grab the MongoDB client and
// set up a repository struct with it.
func NewBaseRepository(client *mongo.Client, databaseName string, collectionName string) (*BaseRepository, error) {
	br := &BaseRepository{
		Database:       client.Database(databaseName),
		Client:         client,
		CollectionName: collectionName,
	}
	return br, nil
}

// used mainly in testing to avoid capping out the concurrent connections on MongoDB
func (r *BaseRepository) Disconnect(ctx context.Context) {
	err := r.Client.Disconnect(ctx)
	if err != nil {
		log.Fatalf("disconnecting from mongodb: %v", err)
	}
}

// returns all documents that match the provided query
func (r *BaseRepository) Find(ctx context.Context, query interface{}, rtn interface{}, opts ...*options.FindOptions) error {
	c, err := r.Database.Collection(r.CollectionName).Find(ctx, query)
	if err != nil {
		return fmt.Errorf("BaseRepository.Find: %v", err)
	}

	err = c.All(ctx, rtn)
	if err != nil {
		return fmt.Errorf("BaseRepository.Find - parsing cursor: %v", err)
	}
	return nil
}

// returns all documents that match the provided query and return the cursor
func (r *BaseRepository) FindCursor(ctx context.Context, query interface{}, opts ...*options.FindOptions) (*mongo.Cursor, error) {
	c, err := r.Database.Collection(r.CollectionName).Find(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("BaseRepository.Find: %v", err)
	}
	return c, nil
}

// returns the first document that matches the provided query
func (r *BaseRepository) FindOne(ctx context.Context, query interface{}, rtn interface{}) error {
	c := r.Database.Collection(r.CollectionName).FindOne(ctx, query)

	err := c.Decode(rtn)
	if err != nil && err == mongo.ErrNoDocuments {
		return nil
	}
	if err != nil {
		return fmt.Errorf("BaseRepository.FindOne - c.All: %v", err)
	}
	return nil
}

// returns the first document that has an _id that matches the id parameter
func (r *BaseRepository) FindById(ctx context.Context, id string, rtn interface{}) error {
	query := bson.M{"_id": bson.M{"$eq": util.StringToObjectId(id)}}
	err := r.Database.Collection(r.CollectionName).FindOne(ctx, query).Decode(rtn)
	if err != nil {
		return fmt.Errorf("find by id on '%v' collection: %v", r.CollectionName, err)
	}
	return nil
}

// returns all documents where the _id is matches an id specified in the ids []string parameter
func (r *BaseRepository) FindByIdList(ctx context.Context, ids []string, rtn interface{}) error {
	query := bson.M{"_id": bson.M{"$in": util.StringsToObjectId(ids)}}
	c, err := r.Database.Collection(r.CollectionName).Find(ctx, query)
	if err != nil {
		return fmt.Errorf("find by id on '%v' collection: %v", r.CollectionName, err)
	}

	err = c.All(ctx, rtn)
	if err != nil {
		return fmt.Errorf("BaseRepository.Find - parsing cursor: %v", err)
	}
	return nil
}

func (r *BaseRepository) FindDistinct(ctx context.Context, fieldName string, query interface{}) ([]interface{}, error) {
	if fieldName == "" {
		return nil, fmt.Errorf("collection.distinct requires a fieldName")
	}

	data, err := r.Database.Collection(r.CollectionName).Distinct(ctx, fieldName, query)
	if err != nil {
		return nil, fmt.Errorf("BaseRepository.Find - parsing cursor: %v", err)
	}
	return data, nil
}

func (r *BaseRepository) Aggregate(ctx context.Context, pipeline interface{}, allowDiskUse bool) (*mongo.Cursor, error) {
	opts := options.AggregateOptions{
		AllowDiskUse: util.PtrBool(allowDiskUse),
	}

	c, err := r.Database.Collection(r.CollectionName).Aggregate(ctx, pipeline, &opts)
	if err != nil {
		return nil, fmt.Errorf("BaseRepository.Aggregate: %v", err)
	}
	return c, nil
}

func (r *BaseRepository) Create(ctx context.Context, doc Document, allowDiskUse bool) (*mongo.InsertOneResult, error) {
	doc.InitializeBaseDoc()

	op, err := r.Database.Collection(r.CollectionName).InsertOne(ctx, doc)
	if err != nil {
		return nil, fmt.Errorf("inserting document into mongodb: %v", err)
	}
	return op, nil
}

func (r *BaseRepository) CreateMany(ctx context.Context, docs []Document, allowDiskUse bool) (*mongo.InsertManyResult, error) {
	var d []interface{}
	for _, doc := range docs {
		doc.InitializeBaseDoc()
		d = append(d, doc)
	}

	op, err := r.Database.Collection(r.CollectionName).InsertMany(ctx, d)
	if err != nil {
		return nil, fmt.Errorf("inserting document into mongodb: %v", err)
	}
	return op, nil
}

func (r *BaseRepository) Update(ctx context.Context, query interface{}, update interface{}, multi bool) (*mongo.UpdateResult, error) {
	var op *mongo.UpdateResult
	var err error

	if !multi {
		op, err = r.Database.Collection(r.CollectionName).UpdateOne(ctx, query, update)
		if err != nil {
			return nil, fmt.Errorf("BaseRepository.Update.UpdateOne: %v", err)
		}
	} else {
		op, err = r.Database.Collection(r.CollectionName).UpdateMany(ctx, query, update)
		if err != nil {
			return nil, fmt.Errorf("BaseRepository.Update.UpdateMany: %v", err)
		}
	}
	return op, nil
}

// DEPRECATED: We may not need this anymore, will implement in the future if otherwise
func (r *BaseRepository) UpdateBase(ctx context.Context, params MongoUpdateParams) (*mongo.UpdateResult, error) {
	if params.Multi {
		result, err := r.Database.Collection(r.CollectionName).UpdateMany(ctx, params.Query, params.Update, &options.UpdateOptions{Upsert: &params.Upsert})
		if err != nil {
			return nil, fmt.Errorf("BaseRepository.UpdateBase.UpdateMany: %v", err)
		}
		return result, nil
	}
	if !params.Multi {
		result, err := r.Database.Collection(r.CollectionName).UpdateOne(ctx, params.Query, params.Update, &options.UpdateOptions{Upsert: &params.Upsert})
		if err != nil {
			return nil, fmt.Errorf("BaseRepository.UpdateBase.UpdateOne: %v", err)
		}
		return result, nil
	}
	return nil, fmt.Errorf("error parsing param Value 'params.Multi'")
}

// Updates a single document whose _id matches what is provided in the `id` parameter
func (r *BaseRepository) UpdateById(ctx context.Context, id string, update interface{}, autoSet bool, status []DocStatus) (*mongo.UpdateResult, error) {
	q := bson.M{
		"_id": bson.M{
			"$eq": util.StringToObjectId(id),
		},
		"status": bson.M{
			"$in": status,
		},
	}

	if autoSet {
		update = bson.M{
			"$set": update,
		}
	}

	opts := &options.UpdateOptions{
		Upsert: util.PtrBool(false),
	}

	result, err := r.Database.Collection(r.CollectionName).UpdateOne(ctx, q, update, opts)
	if err != nil {
		return nil, fmt.Errorf("BaseRepository.UpdateById: %w", err)
	}

	return result, nil
}

// Updates a set of documents whose _ids match what is provided in the `ids` parameter
func (r *BaseRepository) UpdateByIdList(ctx context.Context, ids []string, update interface{}, status []DocStatus) (*mongo.UpdateResult, error) {
	q := bson.M{
		"_id": bson.M{
			"$in": util.StringsToObjectId(ids),
		},
		"status": bson.M{
			"$in": status,
		},
	}

	opts := &options.UpdateOptions{
		Upsert: util.PtrBool(false),
	}

	u := bson.M{
		"$set": update,
	}

	result, err := r.Database.Collection(r.CollectionName).UpdateMany(ctx, q, u, opts)
	if err != nil {
		return nil, fmt.Errorf("BaseRepository.UpdateByIdList: %w", err)
	}
	return result, nil
}

// Deletes a document with a corresponding _id
func (r *BaseRepository) DeleteById(ctx context.Context, id string) (*mongo.DeleteResult, error) {
	filter := bson.M{
		"_id": bson.M{
			"$eq": util.StringToObjectId(id),
		},
	}

	op, err := r.Database.Collection(r.CollectionName).DeleteOne(ctx, filter)
	if err != nil {
		return nil, fmt.Errorf("BaseRepository.DeleteById: %v", err)
	}
	return op, nil
}

// Works similar to DeleteById but doesn't return an op result or an error
func (r *BaseRepository) QuickDeleteById(ctx context.Context, id string) {
	filter := bson.M{
		"_id": bson.M{
			"$eq": util.StringToObjectId(id),
		},
	}

	_, err := r.Database.Collection(r.CollectionName).DeleteOne(ctx, filter)
	if err != nil {
		log.Printf("BaseRepository.DeleteById: %v", err)
	}
}

func (r *BaseRepository) Watch(ctx context.Context) (*mongo.ChangeStream, error) {
	cs, err := r.Database.Collection(r.CollectionName).Watch(ctx, mongo.Pipeline{})
	if err != nil {
		return nil, fmt.Errorf("starting change stream: %v", err)
	}
	return cs, nil
}

// BASEDOC: Methods for BaseDoc to comply with the Document interface
func (b *BaseDoc) RemoveObjectId() {
	// turns it into a zero Value
	if !b.Id.IsZero() {
		b.Id = primitive.NewObjectID()
	}
}

func (b *BaseDoc) UpdateModifiedDate() {
	*b.ModifiedDate = time.Now()
}

func (b *BaseDoc) GetBaseDoc() *BaseDoc {
	return b
}

func (b *BaseDoc) InitializeBaseDoc() {
	if b.Id.IsZero() {
		b.Id = primitive.NewObjectID()
	}
	b.Status = Active
	b.CreatedDate = util.PtrTimeNow()
	b.ModifiedDate = util.PtrTimeNow()
}
