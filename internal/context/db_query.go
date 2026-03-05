package context

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/free5gc/nwdaf/internal/logger"
	"github.com/free5gc/util/mongoapi"
)

const (
	// dbQueryTimeout is the maximum duration for a single MongoDB query
	dbQueryTimeout = 10 * time.Second

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

	ctx, cancel := context.WithTimeout(context.Background(), dbQueryTimeout)
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
		logger.CtxLog.Errorf("MongoDB query failed (correlationId=%s): %v", correlationId, err)
		return nil, err
	}
	defer func() {
		if closeErr := cur.Close(context.Background()); closeErr != nil {
			logger.CtxLog.Debugf("cursor close error: %v", closeErr)
		}
	}()

	var records []UpfTrafficRecord
	if err = cur.All(ctx, &records); err != nil {
		logger.CtxLog.Errorf("MongoDB decode failed: %v", err)
		return nil, err
	}
	// Reverse to restore chronological (ASC) order for ML input
	reverseRecords(records)
	return records, nil
}

// QueryTrafficByMultipleCorrelationIds queries records for multiple correlation IDs.
// Returns at most `limit` of the MOST RECENT records within window, sorted ASC for ML input.
func QueryTrafficByMultipleCorrelationIds(
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

	ctx, cancel := context.WithTimeout(context.Background(), dbQueryTimeout)
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
		logger.CtxLog.Errorf("MongoDB multi-correlation query failed: %v", err)
		return nil, err
	}
	defer func() {
		if closeErr := cur.Close(context.Background()); closeErr != nil {
			logger.CtxLog.Debugf("cursor close error: %v", closeErr)
		}
	}()

	var records []UpfTrafficRecord
	if err = cur.All(ctx, &records); err != nil {
		logger.CtxLog.Errorf("MongoDB decode failed: %v", err)
		return nil, err
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
	dbName string,
	correlationIds []string,
	from, to time.Time,
) ([]UpfTrafficRecord, error) {
	if !IsMongoAvailable() || len(correlationIds) == 0 {
		return nil, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), dbQueryTimeout)
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
		logger.CtxLog.Errorf("MongoDB time range query failed: %v", err)
		return nil, err
	}
	defer func() {
		if closeErr := cur.Close(context.Background()); closeErr != nil {
			logger.CtxLog.Debugf("cursor close error: %v", closeErr)
		}
	}()

	var records []UpfTrafficRecord
	if err = cur.All(ctx, &records); err != nil {
		logger.CtxLog.Errorf("MongoDB decode failed: %v", err)
		return nil, err
	}
	return records, nil
}
