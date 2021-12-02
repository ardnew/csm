package main

import (
	"flag"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"github.com/ardnew/csm"
	"github.com/ardnew/csm/log"
	"github.com/ardnew/csm/suite/filter"
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

func Version() string {
	cat := func(s ...string) string { return strings.Join(s, "") }
	sen := func(s ...string) string { return strings.Join(s, " ") }
	return sen(PROJECT, "version", VERSION, cat("(", sen(cat(BRANCH, "@", REVISION), BUILDTIME), ")"), PLATFORM)
}

const (
	printVersionFlag      = "v"
	quietLoggingFlag      = "q"
	logFieldDefsFlag      = "d"
	invertFilterFlag      = "r"
	keepContentFlag       = "k"
	suiteFilterFlag       = "f"
	outputArchivePathFlag = "o"
	extractDirPathFlag    = "x"
)

func main() {

	var (
		printVersion      bool
		quietLogging      bool
		logFieldDefs      bool
		invertFilter      bool
		keepContent       bool
		suiteFilter       filter.Filters
		outputArchivePath string
		extractDirPath    string
	)

	const defaultExtractDirPath = "."

	flag.BoolVar(&printVersion, printVersionFlag, false,
		"Print "+PROJECT+" version information and exit")
	flag.BoolVar(&quietLogging, quietLoggingFlag, false,
		"Suppress printing non-error log messages (quiet)")
	flag.BoolVar(&logFieldDefs, logFieldDefsFlag, false,
		"List the field definitions parsed from headers")
	flag.BoolVar(&invertFilter, invertFilterFlag, false,
		"Invert matching semantics (select non-matching records)")
	flag.BoolVar(&keepContent, keepContentFlag, false,
		"Keep filtered files in extraction directory after suite creation")
	flag.Var(&suiteFilter, suiteFilterFlag,
		"Select records matching `expression` (logical-OR of each flag given)")
	flag.StringVar(&outputArchivePath, outputArchivePathFlag, "",
		"Create output test suite (.zip) at `filepath`")
	flag.StringVar(&extractDirPath, extractDirPathFlag, defaultExtractDirPath,
		"Extract and save filtered test suites to `dirpath`")
	flag.Parse()

	if printVersion {
		log.Raw("%s\n", Version())
		os.Exit(0)
	}

	givenFlag := map[string]bool{}
	flag.Visit(func(f *flag.Flag) {
		givenFlag[f.Name] = true
	})

	if quietLogging {
		log.Output = ioutil.Discard
	}

	if len(flag.Args()) == 0 {
		log.Msg(log.Error, "error",
			"no input test suite (.zip file or directory) provided")
		os.Exit(1)
	}

	if "" == outputArchivePath && "" == extractDirPath {
		extractDirPath = defaultExtractDirPath
	} else if "" != outputArchivePath {
		if extractDirPath != defaultExtractDirPath && extractDirPath != "" {
			log.Msg(log.Warn, "warning",
				"using directory of output suite (-%s) instead of extraction path (-%s): %q",
				outputArchivePathFlag, extractDirPathFlag,
				filepath.Dir(outputArchivePath))
		}
		extractDirPath = filepath.Dir(outputArchivePath)
	}

	if "" != outputArchivePath {
		if !strings.HasSuffix(outputArchivePath, csm.ArchiveExt) {
			outputArchivePath =
				filepath.Join(outputArchivePath, filepath.Base(flag.Arg(0)))
			log.Msg(log.Warn, "warning", "using default output file name: %q",
				outputArchivePath)
		}
		err := os.MkdirAll(filepath.Dir(outputArchivePath), os.ModePerm)
		if nil != err {
			log.Msg(log.Error, "error", "os.MkdirAll(): %s", err.Error())
			os.Exit(2)
		}
	}
	if filepath.Dir(outputArchivePath) != extractDirPath {
		if err := os.MkdirAll(extractDirPath, os.ModePerm); nil != err {
			log.Msg(log.Error, "error", "os.MkdirAll(): %s", err.Error())
			os.Exit(2)
		}
	}

	path := flag.Arg(0)
	{
		p, err := csm.New(path, extractDirPath, outputArchivePath)
		if nil != err {
			log.Msg(log.Error, "error", "csm.New(): %s", err.Error())
			os.Exit(3)
		}
		if info, err := os.Stat(path); nil != err {
			log.Msg(log.Error, "error", "os.Stat(): %s", err.Error())
			os.Exit(4)
		} else if info.IsDir() {
			if err := p.Replicate(); nil != err {
				log.Msg(log.Error, "error", "csm.Replicate(): %s", err.Error())
				os.Exit(5)
			}
		} else {
			if p.Stale() {
				if err := p.Extract(); nil != err {
					log.Msg(log.Error, "error", "csm.Extract(): %s", err.Error())
					os.Exit(6)
				}
			}
		}
		opts := csm.Options{
			LogFieldDefs: logFieldDefs,
			InvertFilter: invertFilter,
			KeepContent:  keepContent,
			Filters:      suiteFilter,
		}
		if err := p.Filter(opts); nil != err {
			log.Msg(log.Error, "error", "csm.Filter(): %s", err.Error())
			os.Exit(7)
		}
		if "" != outputArchivePath {
			if err := p.Compress(opts); nil != err {
				log.Msg(log.Error, "error", "csm.Compress(): %s", err.Error())
				os.Exit(8)
			}
		}
	}

	log.Msg(log.Info, "exit", "ok!")
}
