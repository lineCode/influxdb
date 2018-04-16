package export

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/influxdata/influxdb/cmd/influx-tools/internal/format"
	"github.com/influxdata/influxdb/cmd/influx-tools/internal/format/binary"
	"github.com/influxdata/influxdb/cmd/influx-tools/internal/format/line"
	"github.com/influxdata/influxdb/cmd/influx-tools/internal/format/text"
	"github.com/influxdata/influxdb/cmd/influx-tools/internal/profile"
	"github.com/influxdata/influxdb/cmd/influx-tools/server"
	"go.uber.org/zap"
)

var (
	_ line.Writer
	_ binary.Writer
)

// Command represents the program execution for "store query".
type Command struct {
	// Standard input/output, overridden for testing.
	Stderr io.Writer
	Stdout io.Writer
	Logger *zap.Logger
	server server.Interface

	configPath      string
	cpuProfile      string
	memProfile      string
	database        string
	rp              string
	shardDuration   time.Duration
	retentionPolicy string
	startTime       int64
	endTime         int64
	format          string
	r               rangeValue
	print           bool
}

// NewCommand returns a new instance of Command.
func NewCommand(server server.Interface) *Command {
	return &Command{
		Stderr: os.Stderr,
		Stdout: os.Stdout,
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

	e, err := cmd.openExporter()
	if err != nil {
		return err
	}
	defer e.Close()

	if cmd.print {
		e.PrintPlan(os.Stdout)
		return nil
	}

	p := profile.NewProfiler(cmd.cpuProfile, cmd.memProfile, cmd.Stderr)
	p.StartProfile()
	defer p.StopProfile()

	var wr format.Writer
	switch cmd.format {
	case "line":
		wr = line.NewWriter(os.Stdout)
	case "binary":
		wr = binary.NewWriter(os.Stdout, cmd.database, cmd.rp, cmd.shardDuration)
	case "series":
		wr = text.NewWriter(os.Stdout, text.Series)
	case "values":
		wr = text.NewWriter(os.Stdout, text.Values)
	case "discard":
		wr = format.Discard
	}
	defer func() {
		err = wr.Close()
	}()

	return e.WriteTo(wr)
}

func (cmd *Command) openExporter() (*Exporter, error) {
	cfg := &ExporterConfig{Database: cmd.database, RP: cmd.rp, ShardDuration: cmd.shardDuration, Min: cmd.r.Min(), Max: cmd.r.Max()}
	e, err := NewExporter(cmd.server, cfg)
	if err != nil {
		return nil, err
	}

	return e, e.Open()
}

func (cmd *Command) parseFlags(args []string) error {
	fs := flag.NewFlagSet("export", flag.ContinueOnError)
	fs.StringVar(&cmd.configPath, "config", "", "Config file")
	fs.StringVar(&cmd.cpuProfile, "cpuprofile", "", "")
	fs.StringVar(&cmd.memProfile, "memprofile", "", "")
	fs.StringVar(&cmd.database, "database", "", "Database name")
	fs.StringVar(&cmd.rp, "rp", "", "Retention policy name")
	fs.StringVar(&cmd.format, "format", "line", "Output format (line, binary)")
	fs.Var(&cmd.r, "range", "Range of target shards to export (default: all)")
	fs.BoolVar(&cmd.print, "print", false, "Print plan to stdout")
	fs.DurationVar(&cmd.shardDuration, "duration", time.Hour*24*7, "Target shard duration")

	if err := fs.Parse(args); err != nil {
		return err
	}

	if cmd.database == "" {
		return errors.New("database is required")
	}

	if cmd.format != "line" && cmd.format != "binary" && cmd.format != "series" && cmd.format != "values" && cmd.format != "discard" {
		return fmt.Errorf("invalid format '%s'", cmd.format)
	}

	return nil
}

type rangeValue struct {
	min, max uint64
	set      bool
}

func (rv *rangeValue) Min() uint64 { return rv.min }

func (rv *rangeValue) Max() uint64 {
	if !rv.set {
		return math.MaxUint64
	}
	return rv.max
}

func (rv *rangeValue) String() string {
	if rv.Min() == rv.Max() {
		return fmt.Sprint(rv.min)
	}
	return fmt.Sprintf("[%d,%d]", rv.Min(), rv.Max())
}

func (rv *rangeValue) Set(v string) (err error) {
	p := strings.Split(v, "-")
	switch {
	case len(p) == 1:
		rv.min, err = strconv.ParseUint(p[0], 10, 64)
		if err != nil {
			return fmt.Errorf("range error: must be a positive number")
		}
		rv.max = rv.min
	case len(p) == 2:
		rv.min, err = strconv.ParseUint(p[0], 10, 64)
		if err != nil {
			return fmt.Errorf("range error: min must be a positive number")
		}
		if len(p[1]) > 0 {
			rv.max, err = strconv.ParseUint(p[1], 10, 64)
			if err != nil {
				return fmt.Errorf("range error: max must be empty or a positive number")
			}
		} else {
			rv.max = math.MaxUint64
		}
	default:
		return fmt.Errorf("range error: %s is not a valid range", v)
	}

	rv.set = true

	return nil
}
