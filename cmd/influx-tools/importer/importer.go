package importer

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/influxdata/influxdb/cmd/influx-tools/server"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
	"go.uber.org/zap"
)

type Config struct {
	Database string
	Data     tsdb.Config
}

type Importer struct {
	MetaClient server.MetaClient
	db         string
	dataDir    string

	rpi       *meta.RetentionPolicyInfo
	log       *zap.Logger
	sh        *shardWriter
	sw        *seriesWriter
	buildTsi  bool
	seriesBuf []byte
}

const seriesBatchSize = 1000

func NewImporter(client server.MetaClient, db string, dataDir string, buildTsi bool, log *zap.Logger) *Importer {
	i := &Importer{MetaClient: client, db: db, dataDir: dataDir, buildTsi: buildTsi, log: log}
	if !buildTsi {
		i.seriesBuf = make([]byte, 0, 2048)
	}
	return i
}

func (i *Importer) Close() error {
	defer i.CloseShardGroup()
	err := i.CloseSeriesFile()
	if err != nil {
		return err
	}
	err = i.CloseShardGroup()
	if err != nil {
		return err
	}
	return nil
}

func (i *Importer) DropDatabase() error {
	i.MetaClient.DropDatabase(i.db)
	dbpath := filepath.Join(i.dataDir, i.db)
	if err := os.RemoveAll(dbpath); err != nil {
		return err
	}
	return nil
}

func (i *Importer) CreateDatabase(rp *meta.RetentionPolicySpec, replace bool) error {
	var rpi *meta.RetentionPolicyInfo
	dbInfo := i.MetaClient.Database(i.db)
	if dbInfo == nil {
		return i.createDatabaseWithRetentionPolicy(rp)
	}

	rpi, err := i.MetaClient.RetentionPolicy(i.db, rp.Name)
	if err != nil {
		return err
	}
	if rpi == nil || replace {
		_, err := i.MetaClient.CreateRetentionPolicy(i.db, rp, false)
		if err != nil {
			return err
		}
	}
	return i.createDatabaseWithRetentionPolicy(rp)
}

func (i *Importer) createDatabaseWithRetentionPolicy(rp *meta.RetentionPolicySpec) error {
	var err error
	var dbInfo *meta.DatabaseInfo
	if len(rp.Name) == 0 {
		dbInfo, err = i.MetaClient.CreateDatabase(i.db)
	} else {
		dbInfo, err = i.MetaClient.CreateDatabaseWithRetentionPolicy(i.db, rp)
	}
	if err != nil {
		return err
	}
	i.rpi = dbInfo.RetentionPolicy(rp.Name)
	return nil
}

func (i *Importer) StartShardGroup(start int64, end int64) error {
	if i.rpi.ShardGroupByTimestamp(time.Unix(0, start)) != nil {
		return fmt.Errorf("shard already exists for start time %v", start)
	}

	sgi, err := i.MetaClient.CreateShardGroup(i.db, i.rpi.Name, time.Unix(0, start))
	if err != nil {
		return err
	}

	shardPath := filepath.Join(i.dataDir, i.db, i.rpi.Name)
	if err = os.MkdirAll(filepath.Join(shardPath, strconv.Itoa(int(sgi.ID))), 0777); err != nil {
		return err
	}

	i.sh = newShardWriter(sgi.ID, shardPath)
	return nil
}

func (i *Importer) Write(key []byte, values tsm1.Values) error {
	if i.sh == nil {
		return errors.New("importer not currently writing a shard")
	}
	i.sh.Write(key, values)
	if i.sh.err != nil {
		return i.sh.err
	}
	return nil
}

func (i *Importer) CloseShardGroup() error {
	i.sh.Close()
	if i.sh.err != nil {
		return i.sh.err
	}
	return nil
}

func (i *Importer) StartSeriesFile() error {
	dataPath := filepath.Join(i.dataDir, i.db)
	shardPath := filepath.Join(i.dataDir, i.db, i.rpi.Name)

	var err error
	if i.buildTsi {
		i.sw, err = NewTSI1SeriesWriter(i.db, dataPath, shardPath, int(i.sh.id))
	} else {
		i.sw, err = NewInMemSeriesWriter(i.db, dataPath, shardPath, int(i.sh.id), i.seriesBuf)
	}

	if err != nil {
		return err
	}
	return nil
}

func (i *Importer) AddSeries(seriesKey []byte) error {
	return i.sw.AddSeries(seriesKey)
}

func (i *Importer) CloseSeriesFile() error {
	return i.sw.Close()
}
