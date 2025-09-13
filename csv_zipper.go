package csv

import (
	"github.com/klauspost/compress/zip"
	"io"
	"os"
	"time"
)

// CsvZipper 流式ZIP创建器
type CsvZipper struct {
	zipWriter *zip.Writer
	zipFile   *os.File
}

func NewCsvZipper(outputPath string) (*CsvZipper, error) {
	zipFile, err := os.Create(outputPath)
	if err != nil {
		return nil, err
	}

	return &CsvZipper{
		zipWriter: zip.NewWriter(zipFile),
		zipFile:   zipFile,
	}, nil
}

func (s *CsvZipper) AddFile(filenames ...string) (err error) {
	for _, filename := range filenames {
		if err = s.addSingleFile(filename); err != nil {
			return err
		}
	}
	return nil
}

func (s *CsvZipper) addSingleFile(filename string) (err error) {
	header := &zip.FileHeader{
		Name:     filename,
		Method:   zip.Deflate,
		Modified: time.Now(),
	}
	var reader *os.File
	if reader, err = os.Open(filename); err != nil {
		return err
	}
	defer reader.Close()

	writer, err := s.zipWriter.CreateHeader(header)
	if err != nil {
		return err
	}

	_, err = io.Copy(writer, reader)
	if err != nil {
		return err
	}
	return nil
}

func (s *CsvZipper) Close() error {
	if err := s.zipWriter.Close(); err != nil {
		return err
	}
	return s.zipFile.Close()
}
