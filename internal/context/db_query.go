package context

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/free5gc/nwdaf/internal/logger"
	"github.com/free5gc/util/mongoapi"
)

const (
	// dbQueryTimeout is the maximum duration for a single MongoDB query
	dbQueryTimeout   = 10 * time.Second
	dbCleanupTimeout = 2 * time.Second

	// DefaultQueryWindow is the default time window for querying historical data
	DefaultQueryWindow = 5 * time.Minute

	// DefaultMaxDataPoints is the default maximum number of data points to return
	DefaultMaxDataPoints = 30
)

// mongoAvailable is set to true only after a successful Ping to MongoDB.
var mongoAvailable bool

// SetMongoAvailable marks MongoDB as reachable (called from service init after Ping succeeds).
func SetMongoAvailable(v bool) {
	mongoAvailable = v
}

// IsMongoAvailable returns true only after a successful connectivity check at startup.
func IsMongoAvailable() bool {
	return mongoAvailable
}

// getCollection returns the UPF traffic time-series collection.
func getCollection(dbName string) *mongo.Collection {
	return mongoapi.Client.Database(dbName).Collection(UpfTrafficDataColl)
}

// QueryTrafficByCorrelationId queries UPF traffic records for a given correlation ID.
// Returns at most `limit` records, selecting the MOST RECENT within `since`, sorted ASC for ML input.
func QueryTrafficByCorrelationId(
	parentCtx context.Context,
	dbName string,
	correlationId string,
	since time.Time,
	limit int,
) ([]UpfTrafficRecord, error) {
	if !IsMongoAvailable() {
		return nil, nil
	}
	if limit <= 0 {
		limit = DefaultMaxDataPoints
	}

	ctx, cancel, err := dbTimeoutContext(parentCtx)
	if err != nil {
		return nil, err
	}
	defer cancel()

	filter := bson.M{
		"metadata.correlationId": correlationId,
		"timestamp":              bson.M{"$gte": since},
	}
	// Sort DESC to get the most recent `limit` records, then reverse to ASC
	opts := options.Find().
		SetSort(bson.D{{Key: "timestamp", Value: -1}}).
		SetLimit(int64(limit))

	coll := getCollection(dbName)
	cur, err := coll.Find(ctx, filter, opts)
	if err != nil {
		return nil, fmt.Errorf("query traffic by correlationId %s: %w", correlationId, err)
	}
	defer func() {
		closeCursorQuietly(cur)
	}()

	var records []UpfTrafficRecord
	if err = cur.All(ctx, &records); err != nil {
		return nil, fmt.Errorf("decode traffic by correlationId %s: %w", correlationId, err)
	}
	// Reverse to restore chronological (ASC) order for ML input
	reverseRecords(records)
	return records, nil
}

// QueryTrafficByMultipleCorrelationIds queries records for multiple correlation IDs.
// Returns at most `limit` of the MOST RECENT records within window, sorted ASC for ML input.
func QueryTrafficByMultipleCorrelationIds(
	parentCtx context.Context,
	dbName string,
	correlationIds []string,
	since time.Time,
	limit int,
) ([]UpfTrafficRecord, error) {
	if !IsMongoAvailable() || len(correlationIds) == 0 {
		return nil, nil
	}
	if limit <= 0 {
		limit = DefaultMaxDataPoints
	}

	ctx, cancel, err := dbTimeoutContext(parentCtx)
	if err != nil {
		return nil, err
	}
	defer cancel()

	filter := bson.M{
		"metadata.correlationId": bson.M{"$in": correlationIds},
		"timestamp":              bson.M{"$gte": since},
	}
	// Sort DESC to get the most recent `limit` records, then reverse to ASC
	opts := options.Find().
		SetSort(bson.D{{Key: "timestamp", Value: -1}}).
		SetLimit(int64(limit))

	coll := getCollection(dbName)
	cur, err := coll.Find(ctx, filter, opts)
	if err != nil {
		return nil, fmt.Errorf("query traffic by multiple correlationIds: %w", err)
	}
	defer func() {
		closeCursorQuietly(cur)
	}()

	var records []UpfTrafficRecord
	if err = cur.All(ctx, &records); err != nil {
		return nil, fmt.Errorf("decode traffic by multiple correlationIds: %w", err)
	}
	// Reverse to restore chronological (ASC) order for ML input
	reverseRecords(records)
	return records, nil
}

// reverseRecords reverses a slice of UpfTrafficRecord in place.
func reverseRecords(records []UpfTrafficRecord) {
	for i, j := 0, len(records)-1; i < j; i, j = i+1, j-1 {
		records[i], records[j] = records[j], records[i]
	}
}

// QueryTrafficInTimeRange queries UPF traffic records for a correlation ID within
// a precise time range [from, to). Used for accuracy monitor ground truth lookup.
func QueryTrafficInTimeRange(
	parentCtx context.Context,
	dbName string,
	correlationIds []string,
	from, to time.Time,
) ([]UpfTrafficRecord, error) {
	if !IsMongoAvailable() || len(correlationIds) == 0 {
		return nil, nil
	}

	ctx, cancel, err := dbTimeoutContext(parentCtx)
	if err != nil {
		return nil, err
	}
	defer cancel()

	filter := bson.M{
		"metadata.correlationId": bson.M{"$in": correlationIds},
		"timestamp": bson.M{
			"$gte": from,
			"$lt":  to,
		},
	}
	opts := options.Find().
		SetSort(bson.D{{Key: "timestamp", Value: 1}})

	coll := getCollection(dbName)
	cur, err := coll.Find(ctx, filter, opts)
	if err != nil {
		return nil, fmt.Errorf("query traffic in time range: %w", err)
	}
	defer func() {
		closeCursorQuietly(cur)
	}()

	var records []UpfTrafficRecord
	if err = cur.All(ctx, &records); err != nil {
		return nil, fmt.Errorf("decode traffic in time range: %w", err)
	}
	return records, nil
}

func dbTimeoutContext(parentCtx context.Context) (context.Context, context.CancelFunc, error) {
	if parentCtx == nil {
		return nil, nil, errors.New("mongo query requires parent context")
	}

	ctx, cancel := context.WithTimeout(parentCtx, dbQueryTimeout)
	return ctx, cancel, nil
}

func closeCursor(cur *mongo.Cursor) error {
	if cur == nil {
		return nil
	}

	cleanupCtx, cancel := context.WithTimeout(context.Background(), dbCleanupTimeout)
	defer cancel()
	return cur.Close(cleanupCtx)
}

func closeCursorQuietly(cur *mongo.Cursor) {
	if err := closeCursor(cur); err != nil {
		// Cursor cleanup failure is diagnostic only and should not override the
		// main query result.
		logger.CtxLog.Debugf("cursor close error: %v", err)
	}
}
