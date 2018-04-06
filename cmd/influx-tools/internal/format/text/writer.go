package text

import (
	"bufio"
	"fmt"
	"io"
	"strconv"

	"github.com/influxdata/influxdb/cmd/influx-tools/internal/format"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/pkg/escape"
	"github.com/influxdata/influxdb/tsdb"
)

type Writer struct {
	w        *bufio.Writer
	key      []byte
	err      error
	m        Mode
	min, max int64
}

type Mode bool

const (
	Series Mode = false
	Values Mode = true
)

func NewWriter(w io.Writer, mode Mode) *Writer {
	var wr *bufio.Writer
	if wr, _ = w.(*bufio.Writer); wr == nil {
		wr = bufio.NewWriter(w)
	}
	return &Writer{
		w:   wr,
		key: make([]byte, 1024),
		m:   mode,
	}
}

func (w *Writer) NewBucket(start, end int64) (format.BucketWriter, error) {
	w.min, w.max = start, end
	return w, nil
}

func (w *Writer) Close() error { return w.w.Flush() }
func (w *Writer) Err() error   { return w.err }

func (w *Writer) WriteSeries(name, field []byte, tags models.Tags) {
	if w.err != nil {
		return
	}

	if w.m == Series {
		w.key = models.AppendMakeKey(w.key[:0], name, tags)
		w.key = append(w.key, ' ')
		w.key = append(w.key, escape.Bytes(field)...)
		w.w.Write(w.key)
		w.w.WriteByte('\n')
	}
}

func (w *Writer) WriteCursor(cur tsdb.Cursor) {
	if w.err != nil || w.m == Series {
		return
	}

	switch c := cur.(type) {
	case tsdb.IntegerBatchCursor:
		w.writeIntegerPoints(c)
	case tsdb.FloatBatchCursor:
		w.writeFloatPoints(c)
	case tsdb.UnsignedBatchCursor:
		w.writeUnsignedPoints(c)
	case tsdb.BooleanBatchCursor:
		w.writeBooleanPoints(c)
	case tsdb.StringBatchCursor:
		w.writeStringPoints(c)
	default:
		panic(fmt.Sprintf("unreachable: %T", c))
	}
}

func (w *Writer) writeIntegerPoints(cur tsdb.IntegerBatchCursor) {
	buf := w.key
	for {
		ts, vs := cur.Next()
		if len(ts) == 0 {
			break
		}
		for i := range ts {
			buf = buf[:0]

			buf = strconv.AppendInt(buf, vs[i], 10)
			buf = append(buf, 'i')
			buf = append(buf, ' ')
			buf = strconv.AppendInt(buf, ts[i], 10)
			buf = append(buf, '\n')
			if _, w.err = w.w.Write(buf); w.err != nil {
				return
			}
		}
	}
}

func (w *Writer) writeFloatPoints(cur tsdb.FloatBatchCursor) {
	buf := w.key
	for {
		ts, vs := cur.Next()
		if len(ts) == 0 {
			break
		}
		for i := range ts {
			buf = buf[:0]

			buf = strconv.AppendFloat(buf, vs[i], 'g', -1, 64)
			buf = append(buf, ' ')
			buf = strconv.AppendInt(buf, ts[i], 10)
			buf = append(buf, '\n')
			if _, w.err = w.w.Write(buf); w.err != nil {
				return
			}
		}
	}
}

func (w *Writer) writeUnsignedPoints(cur tsdb.UnsignedBatchCursor) {
	buf := w.key
	for {
		ts, vs := cur.Next()
		if len(ts) == 0 {
			break
		}
		for i := range ts {
			buf = buf[:0]

			buf = strconv.AppendUint(buf, vs[i], 10)
			buf = append(buf, 'u')
			buf = append(buf, ' ')
			buf = strconv.AppendInt(buf, ts[i], 10)
			buf = append(buf, '\n')
			if _, w.err = w.w.Write(buf); w.err != nil {
				return
			}
		}
	}
}

func (w *Writer) writeBooleanPoints(cur tsdb.BooleanBatchCursor) {
	buf := w.key
	for {
		ts, vs := cur.Next()
		if len(ts) == 0 {
			break
		}
		for i := range ts {
			buf = buf[:0]

			buf = strconv.AppendBool(buf, vs[i])
			buf = append(buf, ' ')
			buf = strconv.AppendInt(buf, ts[i], 10)
			buf = append(buf, '\n')
			if _, w.err = w.w.Write(buf); w.err != nil {
				return
			}
		}
	}
}

func (w *Writer) writeStringPoints(cur tsdb.StringBatchCursor) {
	buf := w.key
	for {
		ts, vs := cur.Next()
		if len(ts) == 0 {
			break
		}
		for i := range ts {
			buf = buf[:0]

			buf = append(buf, '"')
			buf = append(buf, models.EscapeStringField(vs[i])...)
			buf = append(buf, '"')
			buf = append(buf, ' ')
			buf = strconv.AppendInt(buf, ts[i], 10)
			buf = append(buf, '\n')
			if _, w.err = w.w.Write(buf); w.err != nil {
				return
			}
		}
	}
}
