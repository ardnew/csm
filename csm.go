package csm

import (
	"os"
	"path/filepath"

	"github.com/ardnew/csm/log"
	"github.com/ardnew/csm/suite"
	"github.com/ardnew/csm/suite/cache"
	"github.com/ardnew/csm/suite/field"
	"github.com/ardnew/csm/suite/filter"

	"github.com/mholt/archiver/v3"
)

const (
	ArchiveExt  = ".zip"
	CsvBase     = ".csv"
	TakeoffName = "takeoff.testcase.csv"
	LandingName = "landing.testcase.csv"
	OutPrefix   = "[out]"
	ExtPrefix   = "[outext]"
)

type CSM struct {
	zipPath string // input zip file
	csvPath string // path to files extracted from input zip
	outPath string // output zip file
	xtcPath string // path to files compressed into output zip
	cache   *cache.Cache
}

type Options struct {
	LogFieldDefs bool
	InvertFilter bool
	KeepContent  bool
	Filters      filter.Filters
}

func New(zipPath, xtcPath, outPath string) (*CSM, error) {
	csvPath := filepath.Join(xtcPath, CsvBase)
	return &CSM{
		zipPath: zipPath,
		csvPath: csvPath,
		outPath: outPath,
		xtcPath: xtcPath,
		cache:   cache.New(zipPath, csvPath),
	}, nil
}

func (c *CSM) Stale() bool {
	if err := c.cache.Read(); nil != err {
		log.Msg(log.Warn, "cache", "%v", err)
		return true
	}
	if err := c.cache.Update(); nil != err {
		log.Msg(log.Warn, "cache", "%v", err)
		return true
	}
	if stale, _ := c.cache.Stale(); stale {
		return true
	}
	return false
}

func (c *CSM) Extract() error {
	log.Msg(log.Info, "extract", "%q -> %q", c.zipPath, c.csvPath)
	if err := os.RemoveAll(c.csvPath); nil != err {
		return err
	}
	if err := os.MkdirAll(c.csvPath, os.ModePerm); nil != err {
		return err
	}
	if err := archiver.Unarchive(c.zipPath, c.csvPath); nil != err {
		return err
	}
	if err := c.cache.Update(); nil != err {
		return err
	}
	_, file := c.cache.Stale()
	for _, f := range file {
		log.Msg(log.Warn, "cache", "updated %q", f)
	}
	if err := c.cache.Write(); nil != err {
		return err
	}
	return nil
}

func (c *CSM) Compress(opts Options) error {
	log.Msg(log.Info, "compress", "%q -> %q", c.xtcPath, c.outPath)
	if err := os.Remove(c.outPath); nil != err && !os.IsNotExist(err) {
		return err
	}
	takeoffPath := filepath.Join(c.xtcPath, TakeoffName)
	landingPath := filepath.Join(c.xtcPath, LandingName)
	err := archiver.Archive([]string{takeoffPath, landingPath}, c.outPath)
	if nil != err {
		return err
	}
	if !opts.KeepContent {
		if err := os.Remove(takeoffPath); nil != err && !os.IsNotExist(err) {
			return err
		}
		if err := os.Remove(landingPath); nil != err && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

func (c *CSM) Filter(opts Options) error {

	log.Msg(log.Info, "filter", "%q -> %q", c.csvPath, c.xtcPath)

	var takeoffDef, landingDef *field.FieldDef

	ts, err := suite.New(
		filepath.Join(c.csvPath, TakeoffName),              // source file
		filepath.Join(c.xtcPath, TakeoffName),              // output file
		c.fieldDefHandler(TakeoffName, &opts, &takeoffDef), // header row handler
		c.recordHandler(TakeoffName, &opts, &takeoffDef))   // data row handler
	if nil != err {
		return err
	}

	ls, err := suite.New(
		filepath.Join(c.csvPath, LandingName),              // source file
		filepath.Join(c.xtcPath, LandingName),              // output file
		c.fieldDefHandler(LandingName, &opts, &landingDef), // header row handler
		c.recordHandler(LandingName, &opts, &landingDef))   // data row handler
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

func (c *CSM) fieldDefHandler(
	name string, opts *Options, def **field.FieldDef) suite.RecordHandler {

	return func(r []string) (rec []string, skip, stop bool) {
		*def = field.NewDef(r, OutPrefix, ExtPrefix)
		if opts.LogFieldDefs {
			(*def).Log(os.Stdout, name)
		}
		for i := range opts.Filters {
			_, ok := (*def).ColForCsv(opts.Filters[i].Field())
			opts.Filters[i].SetValid(ok)
			if !ok {
				log.Msg(log.Warn, "filter", "ignoring %s filter on unknown field: %q",
					name, opts.Filters[i].Field())
			}
		}
		return r, false, false
	}
}

func (c *CSM) recordHandler(
	name string, opts *Options, def **field.FieldDef) suite.RecordHandler {

	return func(r []string) (rec []string, skip, stop bool) {
		match := 0
		for _, f := range opts.Filters {
			if f.Valid() {
				value, _ := (*def).ValueForCsv(f.Field(), r)
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
	}
}
