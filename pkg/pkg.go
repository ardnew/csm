package pkg

import (
	"os"
	"path/filepath"

	"github.com/ardnew/csm/log"
	"github.com/ardnew/csm/pkg/suite"
	"github.com/ardnew/csm/pkg/suite/cache"
	"github.com/ardnew/csm/pkg/suite/field"
	"github.com/ardnew/csm/pkg/suite/filter"

	"github.com/mholt/archiver/v3"
)

const (
	csvBase     = ".csv"
	takeoffName = "takeoff.testcase.csv"
	landingName = "landing.testcase.csv"
	outPrefix   = "[out]"
	extPrefix   = "[outext]"
)

type Pkg struct {
	zipPath string
	outBase string
	outPath string
	csvPath string
	cache   *cache.Cache
}

func New(zipPath string, outPath string) (*Pkg, error) {
	outBase := filepath.Dir(outPath)
	csvPath := filepath.Join(outBase, csvBase)
	return &Pkg{
		zipPath: zipPath,
		outBase: outBase,
		outPath: outPath,
		csvPath: csvPath,
		cache:   cache.New(zipPath, csvPath),
	}, nil
}

func (p *Pkg) Stale() bool {
	if err := p.cache.Read(); nil != err {
		log.Msg(log.Warn, "cache", "%v", err)
		return true
	}
	if err := p.cache.Update(); nil != err {
		log.Msg(log.Warn, "cache", "%v", err)
		return true
	}
	if stale, _ := p.cache.Stale(); stale {
		return true
	}
	return false
}

func (p *Pkg) Extract() error {
	log.Msg(log.Info, "extract", "%q -> %q", p.zipPath, p.csvPath)
	if err := os.RemoveAll(p.csvPath); nil != err {
		return err
	}
	if err := os.MkdirAll(p.csvPath, os.ModePerm); nil != err {
		return err
	}
	if err := archiver.Unarchive(p.zipPath, p.csvPath); nil != err {
		return err
	}
	if err := p.cache.Update(); nil != err {
		return err
	}
	_, file := p.cache.Stale()
	for _, f := range file {
		log.Msg(log.Warn, "cache", "updated %q", f)
	}
	if err := p.cache.Write(); nil != err {
		return err
	}
	return nil
}

func (p *Pkg) Compress() error {
	log.Msg(log.Info, "compress", "%q -> %q", p.csvPath, p.outPath)
	if err := os.Remove(p.outPath); nil != err && !os.IsNotExist(err) {
		return err
	}
	takeoffPath := filepath.Join(p.outBase, takeoffName)
	landingPath := filepath.Join(p.outBase, landingName)
	err := archiver.Archive([]string{takeoffPath, landingPath}, p.outPath)
	if nil != err {
		return err
	}
	if err := os.Remove(takeoffPath); nil != err && !os.IsNotExist(err) {
		return err
	}
	if err := os.Remove(landingPath); nil != err && !os.IsNotExist(err) {
		return err
	}
	return nil
}

type FilterOpts struct {
	LogFieldDefs bool
	Filters      filter.Filters
	InvertFilter bool
}

func (p *Pkg) Filter(opts FilterOpts) error {

	log.Msg(log.Info, "filter", "%q", p.csvPath)

	var takeoffDef, landingDef *field.FieldDef

	ts, err := suite.New(
		filepath.Join(p.csvPath, takeoffName),
		filepath.Join(p.outBase, takeoffName),
		func(r []string) (rec []string, skip, stop bool) {
			takeoffDef = field.NewDef(r, outPrefix, extPrefix)
			if opts.LogFieldDefs {
				takeoffDef.Log(os.Stdout, takeoffName)
			}
			// validate each of the filter fields
			for i := range opts.Filters {
				_, ok := takeoffDef.ColForCsv(opts.Filters[i].Field())
				opts.Filters[i].SetValid(ok)
				if !ok {
					log.Msg(log.Warn, "filter", "ignoring filter on unknown takeoff field: %q",
						opts.Filters[i].Field())
				}
			}
			return r, false, false
		},
		func(r []string) (rec []string, skip, stop bool) {
			match := 0
			for _, f := range opts.Filters {
				if f.Valid() {
					value, _ := takeoffDef.ValueForCsv(f.Field(), r)
					if f.Eval(value) {
						match += 1
					}
				}
			}
			// skip this case if no criteria matched
			skip = match == 0
			if opts.InvertFilter {
				// skip this case if any criteria matched
				skip = !skip
			}
			return r, skip, stop
		})
	if nil != err {
		return err
	}

	ls, err := suite.New(
		filepath.Join(p.csvPath, landingName),
		filepath.Join(p.outBase, landingName),
		func(r []string) (rec []string, skip, stop bool) {
			landingDef = field.NewDef(r, outPrefix, extPrefix)
			if opts.LogFieldDefs {
				landingDef.Log(os.Stdout, landingName)
			}
			// validate each of the filter fields
			for i := range opts.Filters {
				_, ok := landingDef.ColForCsv(opts.Filters[i].Field())
				opts.Filters[i].SetValid(ok)
				if !ok {
					log.Msg(log.Warn, "filter", "ignoring filter on unknown landing field: %q",
						opts.Filters[i].Field())
				}
			}
			return r, false, false
		},
		func(r []string) (rec []string, skip, stop bool) {
			match := 0
			for _, f := range opts.Filters {
				if f.Valid() {
					value, _ := landingDef.ValueForCsv(f.Field(), r)
					if f.Eval(value) {
						match += 1
					}
				}
			}
			// skip this case if no criteria matched
			skip = match == 0
			if opts.InvertFilter {
				// skip this case if any criteria matched
				skip = !skip
			}
			return r, skip, stop
		})
	if nil != err {
		return err
	}

	log.Msg(
		log.Info, "filter", "retained %d of %d records (%d of %d takeoff, %d of %d landing)",
		ts.Filtered+ls.Filtered, ts.Processed+ls.Processed,
		ts.Filtered, ts.Processed,
		ls.Filtered, ls.Processed,
	)

	return nil
}
