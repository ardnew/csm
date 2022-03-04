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

	cli.SetOutput(os.Stderr)
	cli.Usage = func() {
		fmt.Fprintf(os.Stderr, "%s\n", Version())
		fmt.Fprintf(os.Stderr, "\n")
		fmt.Fprintf(os.Stderr, "SYNOPSIS\n")
		fmt.Fprintf(os.Stderr, "\n")
		fmt.Fprintf(os.Stderr, "  The Calculator Suite Manager (csm) is a Swiss-Army Knife for analyzing, modifying, and constructing\n")
		fmt.Fprintf(os.Stderr, "  automated test suites used by the PC-based FMPS/DAPA Calculator. The test suites are zip-compressed\n")
		fmt.Fprintf(os.Stderr, "  archives containing two regular files at the root of the archive:\n")
		fmt.Fprintf(os.Stderr, "\n")
		fmt.Fprintf(os.Stderr, "    Suite.zip\n")
		fmt.Fprintf(os.Stderr, "      |__ takeoff.testcase.csv        - Takeoff test cases\n")
		fmt.Fprintf(os.Stderr, "      |__ landing.testcase.csv        - Landing test cases\n")
		fmt.Fprintf(os.Stderr, "\n")
		fmt.Fprintf(os.Stderr, "  The test cases are formatted as comma-separated values (CSV), with one test case per line. The first\n")
		fmt.Fprintf(os.Stderr, "  line in each file contains a header row, which defines the data item corresponding to each CSV column.\n")
		fmt.Fprintf(os.Stderr, "\n")
		fmt.Fprintf(os.Stderr, "  This tool reconstructs the table schema of each test suite by parsing the header row found in each\n")
		fmt.Fprintf(os.Stderr, "  file. Thus, as the test suites grow and evolve, no change is required to the tool to understand the\n")
		fmt.Fprintf(os.Stderr, "  different schemas. The same tool can be used with both FMPS and DAPA automated test suites.\n")
		fmt.Fprintf(os.Stderr, "\n")
		fmt.Fprintf(os.Stderr, "USAGE\n")
		fmt.Fprintf(os.Stderr, "\n")
		fmt.Fprintf(os.Stderr, "  There are three primary use cases the tool currently supports, with general usage as follows:\n")
		fmt.Fprintf(os.Stderr, "\n")
		fmt.Fprintf(os.Stderr, "    %s [flags] [-o output] input[.zip]                     - Extract test cases into new test suite\n", PROJECT)
		fmt.Fprintf(os.Stderr, "    %s [flags] -d input[.zip]                              - Display test suite table schema\n", PROJECT)
		fmt.Fprintf(os.Stderr, "    %s [flags] [-p format] input[.zip] [-- columns]        - Print formatted values of test cases\n", PROJECT)
		fmt.Fprintf(os.Stderr, "\n")
		fmt.Fprintf(os.Stderr, "FLAGS\n")
		fmt.Fprintf(os.Stderr, "\n")
		cli.PrintDefaults()
	}

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
			"no input test suite (.zip file or directory) provided. see -h for usage.")
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
		if !opts.LogFieldDefs {
			if "" != outputArchivePath {
				if err := p.Compress(opts); nil != err {
					log.Msg(log.Error, "error", "csm.Compress(): %s", err.Error())
					os.Exit(8)
				}
			}
			if err := p.Cleanup(opts); nil != err {
				log.Msg(log.Error, "error", "csm.Cleanup(): %s", err.Error())
				os.Exit(9)
			}
		}
	}

	log.Msg(log.Info, "exit", "ok!")
}
