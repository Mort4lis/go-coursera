package main

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
)

type FileLineFormatter interface {
	FormatLine(fi os.FileInfo) string
	FormatLastLine(fi os.FileInfo) string
	CreateChildFormatter() FileLineFormatter
}

const (
	levelDelim    = '│'
	lineDelim     = '├'
	lastLineDelim = '└'
)

type BeautyFileLineFormatter struct {
	includeSize bool
	wasLastLine bool
	fmtStr      string
}

func NewBeautyFileLineFormatter(includeSize bool) *BeautyFileLineFormatter {
	return &BeautyFileLineFormatter{includeSize: includeSize}
}

func (f *BeautyFileLineFormatter) CreateChildFormatter() FileLineFormatter {
	fmtStr := f.fmtStr
	if !f.wasLastLine {
		fmtStr += string(levelDelim)
	}
	fmtStr += "\t"

	return &BeautyFileLineFormatter{
		fmtStr:      fmtStr,
		includeSize: f.includeSize,
	}
}

func (f *BeautyFileLineFormatter) FormatLine(fi os.FileInfo) string {
	return f.format(fi, lineDelim)
}

func (f *BeautyFileLineFormatter) FormatLastLine(fi os.FileInfo) string {
	f.wasLastLine = true
	return f.format(fi, lastLineDelim)
}

func (f *BeautyFileLineFormatter) format(fi os.FileInfo, delim rune) string {
	res := fmt.Sprintf("%s%c───%s", f.fmtStr, delim, fi.Name())
	if !f.includeSize || fi.IsDir() {
		return res
	}

	if fi.Size() == 0 {
		return res + " (empty)"
	}
	return res + fmt.Sprintf(" (%db)", fi.Size())
}

func filterFilesInfo(fiList []os.FileInfo, filter func(os.FileInfo) bool) []os.FileInfo {
	res := make([]os.FileInfo, 0)
	for _, fi := range fiList {
		if filter(fi) {
			res = append(res, fi)
		}
	}

	return res
}

func dirTreeInner(out io.Writer, path string, flFmt FileLineFormatter, printFiles bool) error {
	fiList, err := ioutil.ReadDir(path)
	if err != nil {
		return fmt.Errorf("failed to read directory: %v", err)
	}

	if !printFiles {
		fiList = filterFilesInfo(fiList, func(fi os.FileInfo) bool {
			return fi.IsDir()
		})
	}

	for i, fi := range fiList {
		var line string

		isLast := i == len(fiList)-1
		if isLast {
			line = flFmt.FormatLastLine(fi)
		} else {
			line = flFmt.FormatLine(fi)
		}

		if _, err = fmt.Fprintln(out, line); err != nil {
			return fmt.Errorf("failed to print line into %T", out)
		}

		if fi.IsDir() {
			dirFmt := flFmt.CreateChildFormatter()
			dirPath := filepath.Join(path, fi.Name())
			if err = dirTreeInner(out, dirPath, dirFmt, printFiles); err != nil {
				return err
			}
		}
	}

	return nil
}

func dirTree(out io.Writer, path string, printFiles bool) error {
	flFmt := NewBeautyFileLineFormatter(printFiles)
	return dirTreeInner(out, path, flFmt, printFiles)
}

func main() {
	out := os.Stdout
	if !(len(os.Args) == 2 || len(os.Args) == 3) {
		panic("usage go run main.go . [-f]")
	}

	path := os.Args[1]
	printFiles := len(os.Args) == 3 && os.Args[2] == "-f"
	err := dirTree(out, path, printFiles)
	if err != nil {
		panic(err.Error())
	}
}
