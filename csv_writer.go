package csv

import (
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
)

type Option func(*CsvWriter)

type CsvWriter struct {
	csvName    string      // CSV文件名
	zipName    string      // zip文件名
	comma      string      // 分隔符(默认英文逗号)
	file       *os.File    // CSV文件句柄
	writer     *csv.Writer // CSV写入器
	zip        bool        // 是否zip压缩(启用压缩会最终文件会变成 myfile.csv.zip)
	flushCount uint32      // 写入多少行数据后flush
	wroteCount uint32      // 已写入行数
	removeCsv  bool        // 调用Close方法时删除csv文件（一般用于导出数据上传对象存储后清理生成的csv文件）
	removeZip  bool        // 调用Close方法时删除zip文件（一般用于导出数据上传对象存储后清理生成的zip文件）
	useCRLF    bool        // 是否使用CRLF换行符
}

func WithZip() Option {
	return func(c *CsvWriter) {
		c.zip = true
	}
}

func WithComma(comma string) Option {
	return func(c *CsvWriter) {
		c.comma = comma
	}
}

func WithUseCRLF() Option {
	return func(c *CsvWriter) {
		c.useCRLF = true
	}
}

func WithFlushCount(flushCount uint32) Option {
	return func(c *CsvWriter) {
		c.flushCount = flushCount
	}
}

func WithRemoveCsv() Option {
	return func(c *CsvWriter) {
		c.removeCsv = true
	}
}

func WithRemoveZip() Option {
	return func(c *CsvWriter) {
		c.removeZip = true
	}
}

func NewWriter(fileName string, opts ...Option) (w *CsvWriter, err error) {
	w = &CsvWriter{
		csvName: fileName,
	}
	parseCsvOptions(w, opts...)
	if err = w.createFile(); err != nil {
		return nil, err
	}
	return w, nil
}

func parseCsvOptions(cw *CsvWriter, opts ...Option) {
	for _, opt := range opts {
		opt(cw)
	}
}
func (m *CsvWriter) GetCsvFilePath() string {
	return m.csvName
}

func (m *CsvWriter) GetZipFilePath() string {
	return m.zipName
}

func (m *CsvWriter) GetCsvName() string {
	_, name := filepath.Split(m.csvName)
	return name
}

func (m *CsvWriter) GetZipName() string {
	_, name := filepath.Split(m.zipName)
	return name
}

func (m *CsvWriter) GetWroteCount() uint32 {
	return m.wroteCount
}

func (m *CsvWriter) createFile() (err error) {

	if m.csvName == "" {
		return fmt.Errorf("csv file name is empty")
	}
	ext := filepath.Ext(m.csvName)
	if ext != ".csv" {
		return fmt.Errorf("csv file name must end with .csv")
	}
	dir := filepath.Dir(m.csvName)
	if dir != "" {
		if err = os.MkdirAll(dir, os.ModePerm); err != nil {
			return fmt.Errorf("make dir error: %s", err.Error())
		}
	}
	m.file, err = os.Create(m.csvName)
	if err != nil {
		return fmt.Errorf("create file: %s error: %s", m.csvName, err)
	}
	if m.zip {
		m.zipName = m.csvName + ".zip"
	}
	if m.comma != "" {
		m.writer.Comma = []rune(m.comma)[0]
	}
	if m.useCRLF {
		m.writer.UseCRLF = true
	}
	m.writer = csv.NewWriter(m.file)
	return nil
}
func (m *CsvWriter) WriteHeader(header []string) (err error) {
	if m.writer == nil {
		return fmt.Errorf("no writer found")
	}
	if err = m.writer.Write(header); err != nil {
		return err
	}
	return nil
}

func (m *CsvWriter) WriteRows(rows [][]any) (err error) {
	for _, row := range rows {
		if err = m.WriteRow(row); err != nil {
			return err
		}
	}
	return nil
}

func (m *CsvWriter) WriteRow(record []any) (err error) {
	if m.writer == nil {
		return fmt.Errorf("no writer found")
	}

	var row []string
	for _, v := range record {
		row = append(row, fmt.Sprintf("%v", v))
	}

	if err = m.writer.Write(row); err != nil {
		return err
	}
	if m.flushCount > 0 && m.wroteCount > 0 && m.wroteCount%m.flushCount == 0 {
		m.writer.Flush()
	}
	m.wroteCount++
	return nil
}

// 手动flush, over设置为true时将文件写入并压缩成zip文件(如果启用了压缩的话)
func (m *CsvWriter) Flush(over bool) (err error) {
	m.writer.Flush()
	if over && m.zip {
		if err = m.zipFile(); err != nil {
			return fmt.Errorf("compress file error: %s", err)
		}
	}
	return nil
}

// 关闭文件并决定是否将生成的文件删除（一般用于导出数据上传对象存储后清理生成的文件）
func (m *CsvWriter) Close() (err error) {
	if m.removeCsv && m.csvName != "" {
		if err = os.Remove(m.csvName); err != nil {
			return fmt.Errorf("remove csv file: %s error: %s", m.csvName, err.Error())
		}
	}
	if m.removeZip {
		if m.zip && m.zipName != "" {
			if err = os.Remove(m.zipName); err != nil {
				return fmt.Errorf("remove zip file: %s error: %s", m.zipName, err.Error())
			}
		}
	}
	return nil
}

func (m *CsvWriter) zipFile() (err error) {
	// 创建流式ZIP写入器
	creator, err := NewCsvZipper(m.zipName)
	if err != nil {
		return err
	}
	defer creator.Close()
	if err = creator.AddFile(m.csvName); err != nil {
		return err
	}
	return nil
}
