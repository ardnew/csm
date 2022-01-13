package main

import (
	"flag"
	"fmt"
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
	return fmt.Sprintf("%s version %s (%s@%s %s) %s",
		PROJECT, VERSION, BRANCH, REVISION, BUILDTIME, PLATFORM)
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
	formatStringFlag      = "p"
	procTakeoffFlag       = "t"
	procLandingFlag       = "l"
)

func main() {

	var (
		printVersion      bool
		quietLogging      bool
		logFieldDefs      bool
		invertFilter      bool
		keepContent       bool
		procTakeoff       bool
		procLanding       bool
		suiteFilter       filter.Filters
		outputArchivePath string
		extractDirPath    string
		formatString      string
	)

	const defaultExtractDirPath = "."

	cli := flag.NewFlagSet("command-line", flag.ExitOnError)

	cli.BoolVar(&printVersion, printVersionFlag, false,
		"Print "+PROJECT+" version information and exit")
	cli.BoolVar(&quietLogging, quietLoggingFlag, false,
		"Suppress printing non-error log messages (quiet)")
	cli.BoolVar(&logFieldDefs, logFieldDefsFlag, false,
		"List the field definitions parsed from headers")
	cli.BoolVar(&invertFilter, invertFilterFlag, false,
		"Invert matching semantics (select non-matching records)")
	cli.BoolVar(&keepContent, keepContentFlag, false,
		"Keep filtered files in extraction directory after suite creation")
	cli.BoolVar(&procTakeoff, procTakeoffFlag, true,
		"Process takeoff test cases")
	cli.BoolVar(&procLanding, procLandingFlag, true,
		"Process landing test cases")
	cli.Var(&suiteFilter, suiteFilterFlag,
		"Select records matching `expression` (logical-OR of each flag given)")
	cli.StringVar(&outputArchivePath, outputArchivePathFlag, "",
		"Create output test suite (.zip) at `filepath`")
	cli.StringVar(&extractDirPath, extractDirPathFlag, defaultExtractDirPath,
		"Extract and save filtered test suites to `dirpath`")
	cli.StringVar(&formatString, formatStringFlag, "",
		"Print each column named in trailing arguments per format `string`")

	cliArg := []string{}
	colArg := []string{}

	var cliLen, colPos int
	if len(os.Args) > 1 {
		for i, a := range os.Args[1:] {
			if a == "--" {
				if pos := i + 2; len(os.Args) > pos {
					colPos = pos
				}
				break
			}
			cliLen++
		}
		if cliLen > 0 {
			cliArg = os.Args[1 : cliLen+1]
		}
		if colPos > 0 {
			colArg = os.Args[colPos:]
		}
	}
	cli.Parse(cliArg)

	if printVersion {
		log.Raw("%s\n", Version())
		os.Exit(0)
	}

	givenFlag := map[string]bool{}
	cli.Visit(func(f *flag.Flag) {
		givenFlag[f.Name] = true
	})

	if quietLogging {
		log.Output = ioutil.Discard
	}

	if len(cliArg) == 0 {
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
				filepath.Join(outputArchivePath, filepath.Base(cli.Arg(0)))
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

	path := cli.Arg(0)
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
			FormatString: formatString,
			FormatCols:   colArg,
			ProcTakeoff:  procTakeoff,
			ProcLanding:  procLanding,
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
