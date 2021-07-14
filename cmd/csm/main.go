package main

import (
	"flag"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"github.com/ardnew/csm/log"
	"github.com/ardnew/csm/pkg"
	"github.com/ardnew/csm/pkg/suite/filter"
)

var (
	PROJECT   string
	IMPORT    string
	VERSION   string
	BUILDTIME string
	PLATFORM  string
	BRANCH    string
	REVISION  string
)

const defaultOutputFileName = "TestSuite.zip"

func main() {

	var (
		quietLogging   bool
		logFieldDefs   bool
		outputFilePath string
		suiteFilter    filter.Filters
	)

	defaultOutputFilePath := filepath.Join(os.Getenv("PWD"), defaultOutputFileName)

	flag.BoolVar(&quietLogging, "q", false, "Suppress printing non-error log messages (quiet)")
	flag.BoolVar(&logFieldDefs, "d", false, "List the field definitions parsed from headers")
	flag.StringVar(&outputFilePath, "o", defaultOutputFilePath, "Output test suite (.zip) file path")
	flag.Var(&suiteFilter, "f", "Filter expressions, may be specified multiple times")
	flag.Parse()

	if quietLogging {
		log.Output = ioutil.Discard
	}

	if len(flag.Args()) == 0 {
		log.Msg(log.Error, "error", "no input test suite (.zip) file(s) provided")
		os.Exit(1)
	}

	if !strings.HasSuffix(outputFilePath, filepath.Ext(defaultOutputFileName)) {
		outputFilePath = filepath.Join(outputFilePath, defaultOutputFileName)
		log.Msg(log.Warn, "warning", "using default output file name: %q", outputFilePath)
	}

	if err := os.MkdirAll(filepath.Dir(outputFilePath), os.ModePerm); nil != err {
		log.Msg(log.Error, "error", "os.MkdirAll(): %s", err.Error())
		os.Exit(2)
	}

	for _, path := range flag.Args() {
		p, err := pkg.New(path, outputFilePath)
		if nil != err {
			log.Msg(log.Error, "error", "pkg.New(): %s", err.Error())
			os.Exit(3)
		}
		if p.Stale() {
			if err := p.Extract(); nil != err {
				log.Msg(log.Error, "error", "pkg.Extract(): %s", err.Error())
				os.Exit(4)
			}
		}
		opts := pkg.FilterOpts{
			LogFieldDefs: logFieldDefs,
			Filters:      suiteFilter,
		}
		if err := p.Filter(opts); nil != err {
			log.Msg(log.Error, "error", "pkg.Filter(): %s", err.Error())
			os.Exit(5)
		}
		if err := p.Compress(); nil != err {
			log.Msg(log.Error, "error", "pkg.Compress(): %s", err.Error())
			os.Exit(6)
		}
	}

	log.Msg(log.Info, "exit", "ok!")
}
