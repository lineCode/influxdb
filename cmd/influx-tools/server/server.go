package server

import (
	"time"

	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/tsdb"
	"go.uber.org/zap"
)

type Interface interface {
	Open(path string) error
	MetaClient() MetaClient
	TSDBConfig() tsdb.Config
	Logger() *zap.Logger
}

type MetaClient interface {
	Database(name string) *meta.DatabaseInfo
	RetentionPolicy(database, name string) (rpi *meta.RetentionPolicyInfo, err error)
	// TODO(sgc): MUST return shards owned by this node only
	ShardGroupsByTimeRange(database, policy string, min, max time.Time) (a []meta.ShardGroupInfo, err error)
	DropDatabase(name string) error
	CreateRetentionPolicy(database string, spec *meta.RetentionPolicySpec, makeDefault bool) (*meta.RetentionPolicyInfo, error)
	CreateDatabase(name string) (*meta.DatabaseInfo, error)
	CreateDatabaseWithRetentionPolicy(name string, spec *meta.RetentionPolicySpec) (*meta.DatabaseInfo, error)
	CreateShardGroup(database, policy string, timestamp time.Time) (*meta.ShardGroupInfo, error)
}
