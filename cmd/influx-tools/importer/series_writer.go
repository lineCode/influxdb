package importer

import (
	"fmt"
	"path/filepath"
	"strconv"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
	"github.com/influxdata/influxdb/tsdb/index/tsi1"
)

type seriesWriter struct {
	keys            [][]byte
	names           [][]byte
	tags            []models.Tags
	seriesBatchSize int
	sfile           *tsdb.SeriesFile
	idx             seriesIndex
}

func NewInMemSeriesWriter(db string, dataPath string, shardPath string, shardId int, buf []byte) (*seriesWriter, error) {
	sfile := tsdb.NewSeriesFile(filepath.Join(dataPath, tsdb.SeriesFileDirectory))
	if err := sfile.Open(); err != nil {
		return nil, err
	}

	return &seriesWriter{seriesBatchSize: seriesBatchSize, sfile: sfile, idx: &seriesFileAdapter{sf: sfile, buf: buf}}, nil
}

func NewTSI1SeriesWriter(db string, dataPath string, shardPath string, shardId int) (*seriesWriter, error) {
	sfile := tsdb.NewSeriesFile(filepath.Join(dataPath, tsdb.SeriesFileDirectory))
	if err := sfile.Open(); err != nil {
		return nil, err
	}

	ti := tsi1.NewIndex(sfile, db, tsi1.WithPath(filepath.Join(shardPath, strconv.Itoa(shardId), "index")))
	if err := ti.Open(); err != nil {
		return nil, fmt.Errorf("error opening TSI1 index %d: %s", shardId, err.Error())
	}

	return &seriesWriter{seriesBatchSize: seriesBatchSize, sfile: sfile, idx: ti}, nil
}

func (sw *seriesWriter) AddSeries(key []byte) error {
	seriesKey, _ := tsm1.SeriesAndFieldFromCompositeKey(key)
	sw.keys = append(sw.keys, seriesKey)

	name, tag := models.ParseKeyBytes(seriesKey)
	sw.names = append(sw.names, name)
	sw.tags = append(sw.tags, tag)

	if len(sw.keys) == sw.seriesBatchSize {
		if err := sw.idx.CreateSeriesListIfNotExists(sw.keys, sw.names, sw.tags); err != nil {
			return err
		}
		sw.keys = sw.keys[:0]
		sw.names = sw.names[:0]
		sw.tags = sw.tags[:0]
	}

	return nil
}

func (sw *seriesWriter) Close() error {
	defer sw.idx.Close()
	defer sw.sfile.Close()
	if err := sw.idx.CreateSeriesListIfNotExists(sw.keys, sw.names, sw.tags); err != nil {
		return err
	}
	err := sw.idx.Close()
	if err != nil {
		return err
	}
	err = sw.sfile.Close()
	if err != nil {
		return err
	}
	return nil
}

type seriesIndex interface {
	CreateSeriesListIfNotExists(keys [][]byte, names [][]byte, tagsSlice []models.Tags) (err error)
	Close() error
}

type seriesFileAdapter struct {
	sf  *tsdb.SeriesFile
	buf []byte
}

func (s *seriesFileAdapter) CreateSeriesListIfNotExists(keys [][]byte, names [][]byte, tagsSlice []models.Tags) (err error) {
	_, err = s.sf.CreateSeriesListIfNotExists(names, tagsSlice, s.buf[:0])
	return
}

func (s *seriesFileAdapter) Close() error {
	return nil
}
