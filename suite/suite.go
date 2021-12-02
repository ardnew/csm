package suite

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
)

type Suite struct {
	inPath    string
	outPath   string
	Processed int
	Filtered  int
}

type RecordHandler func([]string) (rec []string, skip, stop bool)

func New(in, out string, define, handle RecordHandler) (*Suite, error) {

	s := Suite{inPath: in, outPath: out}

	o := io.Discard
	if s.outPath != "" {
		err := os.RemoveAll(s.outPath)
		if nil != err {
			return nil, err
		}
		f, err := os.Create(s.outPath)
		if nil != err {
			return nil, err
		}
		defer func() { _, _ = f.Sync(), f.Close() }()
		// be sure to substitute the output writer from io.Discard (a bit bucket,
		// or null device) to our physical output file.
		o = f
	}

	i, err := os.Open(s.inPath)
	if nil != err {
		return nil, err
	}
	defer i.Close()

	if err := s.filter(i, o, define, handle); nil != err {
		return nil, fmt.Errorf("%s->%s: %s", s.inPath, s.outPath, err.Error())
	}
	return &s, nil
}

func (s *Suite) filter(r io.Reader, w io.Writer, d, h RecordHandler) (err error) {

	ci := csv.NewReader(r)
	co := csv.NewWriter(w)
	defer func() {
		co.Flush()
		if err == nil {
			err = co.Error()
		}
	}()

	lineNo := 0
	for {

		rec, err := ci.Read()
		if err == io.EOF {
			break
		}
		if nil != err {
			return err
		}
		lineNo += 1

		if nil == d {
			d = func(rec []string) (q []string, skip, stop bool) {
				return rec, true, false
			}
		}

		switch lineNo {
		case 1:
			q, skip, stop := d(rec)
			if stop {
				return nil
			} else if !skip {
				if err := co.Write(q); nil != err {
					return err
				}
				break // out of switch block
			}
			// if we skip on field definitions, it means we want to process the row
			// as ordinary data, so continue to default case.
			fallthrough

		default:
			q, skip, stop := h(rec)
			s.Processed += 1
			if stop {
				return nil
			} else if !skip {
				if err := co.Write(q); nil != err {
					return err
				}
				s.Filtered += 1
			}
		}
	}

	return
}
