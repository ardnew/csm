package csm

import (
	"io"
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
	arcPath string // input zip file or directory
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

func New(arcPath, xtcPath, outPath string) (*CSM, error) {
	csvPath := filepath.Join(xtcPath, CsvBase)
	return &CSM{
		arcPath: arcPath,
		csvPath: csvPath,
		outPath: outPath,
		xtcPath: xtcPath,
		cache:   cache.New(arcPath, csvPath),
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
	stale, _ := c.cache.Stale()
	return stale
}

func (c *CSM) Extract() error {
	log.Msg(log.Info, "extract", "%q -> %q", c.arcPath, c.csvPath)
	if err := os.RemoveAll(c.csvPath); nil != err {
		return err
	}
	if err := os.MkdirAll(c.csvPath, os.ModePerm); nil != err {
		return err
	}
	if err := archiver.Unarchive(c.arcPath, c.csvPath); nil != err {
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

func (c *CSM) Replicate() error {
	log.Msg(log.Info, "replicate", "%q -> %q", c.arcPath, c.csvPath)
	if err := os.RemoveAll(c.csvPath); nil != err {
		return err
	}
	if err := os.MkdirAll(c.csvPath, os.ModePerm); nil != err {
		return err
	}
	cp := func(src, dst string) error {
		s, err := os.Open(src)
		if nil != err {
			return err
		}
		defer s.Close()
		d, err := os.Create(dst)
		if nil != err {
			return err
		}
		defer d.Close()
		_, err = io.Copy(d, s)
		return err
	}
	if err := cp(filepath.Join(c.arcPath, LandingName),
		filepath.Join(c.csvPath, LandingName)); nil != err {
		return err
	}
	if err := cp(filepath.Join(c.arcPath, TakeoffName),
		filepath.Join(c.csvPath, TakeoffName)); nil != err {
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

	var takeoffDef, landingDef *field.FieldDef

	var takeoffOut, landingOut string
	if !opts.LogFieldDefs {
		log.Msg(log.Info, "filter", "%q -> %q", c.csvPath, c.xtcPath)
		takeoffOut = filepath.Join(c.xtcPath, TakeoffName)
		landingOut = filepath.Join(c.xtcPath, LandingName)
	}

	ts, err := suite.New(
		filepath.Join(c.csvPath, TakeoffName), // source file
		takeoffOut,                            // output file
		c.fieldDefHandler(TakeoffName, &opts, &takeoffDef), // header row handler
		c.recordHandler(TakeoffName, &opts, &takeoffDef))   // data row handler
	if nil != err {
		return err
	}

	ls, err := suite.New(
		filepath.Join(c.csvPath, LandingName), // source file
		landingOut,                            // output file
		c.fieldDefHandler(LandingName, &opts, &landingDef), // header row handler
		c.recordHandler(LandingName, &opts, &landingDef))   // data row handler
	if nil != err {
		return err
	}

	if !opts.LogFieldDefs {
		log.Msg(
			log.Info, "filter", "retained %d of %d records (%d of %d takeoff, %d of %d landing)",
			ts.Filtered+ls.Filtered, ts.Processed+ls.Processed,
			ts.Filtered, ts.Processed,
			ls.Filtered, ls.Processed,
		)
	}

	return nil
}

func (c *CSM) fieldDefHandler(
	name string, opts *Options, def **field.FieldDef) suite.RecordHandler {

	return func(r []string) (rec []string, skip, stop bool) {
		*def = field.NewDef(r, OutPrefix, ExtPrefix)
		if opts.LogFieldDefs {
			(*def).Log(os.Stdout, name)
			return r, false, true // stop processing after reading field def header
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
