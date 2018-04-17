package importer

import (
	"errors"
	"flag"
	"io"
	"os"
	"time"

	"github.com/influxdata/influxdb/tsdb/engine/tsm1"

	"github.com/influxdata/influxdb/cmd/influx-tools/internal/format/binary"
	"github.com/influxdata/influxdb/cmd/influx-tools/internal/profile"
	"github.com/influxdata/influxdb/cmd/influx-tools/server"
	"github.com/influxdata/influxdb/services/meta"
	"go.uber.org/zap"
)

// Command represents the program execution for "store query".
type Command struct {
	// Standard input/output, overridden for testing.
	stderr io.Writer
	stdin  io.Reader
	Logger *zap.Logger
	server server.Interface

	configPath      string
	database        string
	retentionPolicy string
	shardDuration   time.Duration
	buildTSI        bool
	replace         bool

	cpuProfile string
	memProfile string
}

// NewCommand returns a new instance of Command.
func NewCommand(server server.Interface) *Command {
	return &Command{
		stderr: os.Stderr,
		stdin:  os.Stdin,
		server: server,
	}
}

func (cmd *Command) Run(args []string) (err error) {
	err = cmd.parseFlags(args)
	if err != nil {
		return err
	}

	err = cmd.server.Open(cmd.configPath)
	if err != nil {
		return err
	}

	p := profile.NewProfiler(cmd.cpuProfile, cmd.memProfile, cmd.stderr)
	p.StartProfile()
	defer p.StopProfile()

	i := NewImporter(cmd.server.MetaClient(), cmd.database, cmd.server.TSDBConfig().Dir, cmd.buildTSI, cmd.Logger)

	reader := binary.NewReader(cmd.stdin)
	_, err = reader.ReadHeader()
	if err != nil {
		return err
	}

	err = i.CreateDatabase(&meta.RetentionPolicySpec{Name: cmd.retentionPolicy, ShardGroupDuration: cmd.shardDuration}, cmd.replace)
	if err != nil {
		return err
	}

	var bh *binary.BucketHeader
	for bh, err = reader.NextBucket(); (bh != nil) && (err == nil); bh, err = reader.NextBucket() {
		err = importShard(reader, i, bh.Start, bh.End)
		if err != nil {
			return err
		}
	}

	if err != nil {
		return err
	}

	return nil
}

func importShard(reader *binary.Reader, i *Importer, start int64, end int64) error {
	err := i.StartShardGroup(start, end)
	if err != nil {
		return err
	}
	defer i.CloseShardGroup()

	var sh *binary.SeriesHeader
	var next bool
	for sh, err = reader.NextSeries(); (sh != nil) && (err == nil); sh, err = reader.NextSeries() {
		i.AddSeries(sh.SeriesKey)
		pr := reader.Points()
		seriesFieldKey := tsm1.SeriesFieldKeyBytes(string(sh.SeriesKey), string(sh.Field))

		for next, err = pr.Next(); next && (err == nil); next, err = pr.Next() {
			err = i.Write(seriesFieldKey, pr.Values())
			if err != nil {
				return err
			}
		}
		if err != nil {
			return err
		}
	}

	return i.CloseShardGroup()
}

func (cmd *Command) parseFlags(args []string) error {
	fs := flag.NewFlagSet("import", flag.ContinueOnError)
	fs.StringVar(&cmd.configPath, "config", "", "Config file")
	fs.StringVar(&cmd.cpuProfile, "cpuprofile", "", "")
	fs.StringVar(&cmd.memProfile, "memprofile", "", "")
	fs.StringVar(&cmd.database, "database", "", "Database name")
	fs.StringVar(&cmd.retentionPolicy, "rp", "", "Retention policy")
	fs.DurationVar(&cmd.shardDuration, "shard-duration", time.Hour*24*7, "Retention policy shard duration")
	fs.BoolVar(&cmd.buildTSI, "build-tsi", false, "Build the on disk TSI")
	fs.BoolVar(&cmd.replace, "replace", false, "Enables replacing an existing retention policy")

	if err := fs.Parse(args); err != nil {
		return err
	}

	if cmd.database == "" {
		return errors.New("database is required")
	}

	if cmd.retentionPolicy == "" {
		return errors.New("retention policy is required")
	}

	return nil
}
