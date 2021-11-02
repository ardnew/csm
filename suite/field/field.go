package field

import (
	"fmt"
	"io"
	"strings"

	"github.com/ardnew/csm/log"
)

type InField struct {
	csvCol  int
	csvName string
}

type OutField struct {
	csvCol     int
	csvName    string
	csvColExt  int
	csvNameExt string
}

type FieldId int

type FieldDef struct {
	In        []InField
	Out       []OutField
	OutPrefix string
	ExtPrefix string
}

func NewDef(r []string, outPrefix, extPrefix string) *FieldDef {

	var inCount, outCount int
	for i, name := range r {
		if strings.HasPrefix(name, outPrefix) ||
			strings.HasPrefix(name, extPrefix) {
			inCount = i
			break
		}
	}
	outCount = (len(r) - inCount) / 2

	// If there is an even number of inputs, then the extended precision columns
	// are all even-numbered, and vise-versa. Meaning, the number of inputs and
	// the extended precision output columns must have the same parity (LSB).

	in := make([]InField, inCount)
	out := make([]OutField, outCount)

	for col, name := range r {
		if col < inCount {
			in[col].csvCol = col
			in[col].csvName = name
		} else {
			outCol := (col - inCount) / 2
			// see comment above regarding output column number parity.
			if inCount&1 == col&1 {
				out[outCol].csvColExt = col
				out[outCol].csvNameExt = name
			} else {
				out[outCol].csvCol = col
				out[outCol].csvName = name
			}
		}
	}

	return &FieldDef{
		In:        in,
		Out:       out,
		OutPrefix: outPrefix,
		ExtPrefix: extPrefix,
	}
}

func (def *FieldDef) inputID(col int) (int, bool) {
	if col >= 0 && col < len(def.In) {
		return col, true
	} else {
		return -1, false
	}
}

func (def *FieldDef) outputID(col int) (int, bool) {
	if col >= len(def.In) && col < (len(def.In)+(len(def.Out)*2)) {
		return (col - len(def.In)) / 2, true
	} else {
		return -1, false
	}
}

func (def *FieldDef) Input(col int) (csv string, ok bool) {
	if id, valid := def.inputID(col); valid {
		f := def.In[id]
		csv = f.csvName
		ok = true
	}
	return csv, ok
}

func (def *FieldDef) Output(col int) (csv, ext string, ok bool) {
	if id, valid := def.outputID(col); valid {
		f := def.Out[id]
		csv = f.csvName
		ext = f.csvNameExt
		ok = true
	}
	return csv, ext, ok
}

func (def *FieldDef) ColForCsv(csvName string) (col int, ok bool) {
	if !strings.HasPrefix(csvName, def.OutPrefix) &&
		!strings.HasPrefix(csvName, def.ExtPrefix) {
		for _, f := range def.In {
			if csvName == f.csvName {
				return f.csvCol, true
			}
		}
	} else {
		for _, f := range def.Out {
			if csvName == f.csvName {
				return f.csvCol, true
			} else if csvName == f.csvNameExt {
				return f.csvColExt, true
			}
		}
	}
	return -1, false
}

func (def *FieldDef) ValueForCsv(csvName string, record []string) (string, bool) {
	if col, ok := def.ColForCsv(csvName); ok {
		return record[col], true
	}
	return "", false
}

func (def *FieldDef) Log(w io.Writer, name string) {
	n := log.Digits(len(def.In) + len(def.Out))
	fmt.Fprintln(w, "==", name)
	for _, f := range def.In {
		fmt.Fprintf(w, "  I %0*d %q\n", n, f.csvCol, f.csvName)
	}
	for _, f := range def.Out {
		fmt.Fprintf(w, "  E %0*d %q\n", n, f.csvColExt, f.csvNameExt)
		fmt.Fprintf(w, "  O %0*d %q\n", n, f.csvCol, f.csvName)
	}
}

func (def *FieldDef) LogRecord(record []string) {
	var fe string
	for i, f := range record {
		if csv, ok := def.Input(i); ok {
			log.Msg(log.Info, "input", "%d:[%s]->%s", i, csv, f)
		} else if csv, _, ok := def.Output(i); ok {
			if i&1 == len(def.In)&1 {
				fe = f
			} else {
				log.Msg(log.Info, "output", "%d:[%s]->%s(%s)", i, strings.TrimPrefix(csv, def.OutPrefix), f, fe)
			}
		} else {
			log.Msg(log.Error, "handle", "invalid field (%d): %s", i, f)
		}
	}
}

var (
	thrustMap = map[string]string{
		"0": "NONE",
		"1": "TRT",
		"2": "EWO",
		"3": "MIN",
		"4": "MCL",
		"5": "SPLIT",
	}
	mdsMap = map[string]string{
		"0": "RC-135S",
		"1": "RC-135U",
		"2": "RC-135V",
		"3": "RC-135W",
		"4": "TC-135W (4133)",
		"5": "TC-135W",
		"6": "NC-135W",
		"7": "WC-135C",
		"8": "WC-135W",
	}
	brakeMap = map[string]string{
		"0": "MKII STEEL",
		"1": "MKIII STEEL",
		"2": "CARBON",
	}
	climbMap = map[string]string{
		"0": "MAX",
		"1": "ACCL",
	}
	dpobstMap = map[string]string{
		"0": "SDP",
		"1": "OBSTACLE",
		"2": "LROC_CAC",
		"3": "LROC_ODP",
		"4": "LROC_DIR_CLB",
		"5": "ODP",
	}
	flapMap = map[string]string{
		"0": "0",
		"1": "20",
		"2": "30",
		"3": "40",
		"4": "50",
	}
	hwbenMap = map[string]string{
		"0": "0%",
		"1": "50%",
		"2": "100%",
	}
	lnpMap = map[string]string{
		"0": "NUMERIC",
		"1": "STATIC",
		"2": "ROLLING",
		"3": "STATIC/ROLLING",
	}
	lflMap = map[string]string{
		"0": "2.0G",
		"1": "2.5G",
	}
	rcrMap = map[string]string{
		"0": "NUMERIC",
		"1": "DRY",
		"2": "WET",
		"3": "SLUSHY",
	}
	scrhtMap = map[string]string{
		"0": "0ft",
		"1": "16ft",
		"2": "35ft",
	}
	spdbrkMap = map[string]string{
		"0": "INOP",
		"1": "NORM",
		"2": "PART",
	}
	modeMap = map[string]string{
		"0": "NORMAL",
		"1": "EWO",
	}
)
