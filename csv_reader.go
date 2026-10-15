package csv

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"reflect"
	"strconv"
	"strings"
)

func OpenCSV(filename string) (io.Reader, error) {
	file, err := os.OpenFile(filename, os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}
	return file, nil
}

// ParseCSVRows 解析整个 CSV（第一行表头，之后为数据），
// 返回按 csv 标签填充好的 []T。T 为任意带 csv 标签的结构体。
// trimLeadingSpace=true表示第一行为表头，数据从第2行开始取
func ParseCSVRows[T any](r io.Reader, trimLeadingSpace bool) ([]T, error) {
	cr := csv.NewReader(r)
	cr.TrimLeadingSpace = trimLeadingSpace

	header, err := cr.Read()
	if err != nil {
		return nil, fmt.Errorf("读取表头失败: %w", err)
	}

	var out []T
	for {
		rec, err := cr.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("读取数据行失败: %w", err)
		}
		if isAllEmpty(rec) { // 跳过全空行
			continue
		}
		one, err := unmarshalRow[T](header, rec)
		if err != nil {
			return nil, fmt.Errorf("第%d行解析失败: %w", len(out)+2, err)
		}
		out = append(out, *one)
	}
	return out, nil
}

// UnmarshalRow 通用实现：按 csv 标签把一行记录映射到结构体字段。
func unmarshalRow[T any](header, record []string) (*T, error) {
	var zero T
	rv := reflect.ValueOf(&zero).Elem()
	rt := rv.Type()

	// 以「表头列名 -> 该列值」建立查找表
	vals := make(map[string]string, len(header))
	for i, h := range header {
		if i < len(record) {
			vals[strings.TrimSpace(h)] = record[i]
		}
	}

	// 遍历结构体字段，按 csv 标签取列值并转换
	for i := 0; i < rt.NumField(); i++ {
		sf := rt.Field(i)
		tag := sf.Tag.Get("csv")
		if tag == "" {
			continue // 无 csv 标签的字段不处理
		}
		raw, ok := vals[tag]
		if !ok {
			return nil, fmt.Errorf("表头缺少列: %s", tag)
		}
		if err := setField(rv.Field(i), strings.TrimSpace(raw)); err != nil {
			return nil, fmt.Errorf("列 %s 值 %q 转换失败: %w", tag, raw, err)
		}
	}
	return &zero, nil
}

// setField 根据目标字段类型把字符串转成对应类型。
func setField(fv reflect.Value, raw string) error {
	switch fv.Kind() {
	case reflect.String:
		fv.SetString(raw)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		v, err := strconv.ParseUint(raw, 10, 64)
		if err != nil {
			return err
		}
		fv.SetUint(v)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		v, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return err
		}
		fv.SetInt(v)
	case reflect.Float32, reflect.Float64:
		v, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			return err
		}
		fv.SetFloat(v)
	case reflect.Bool:
		v, err := strconv.ParseBool(raw)
		if err != nil {
			return err
		}
		fv.SetBool(v)
	default:
		return fmt.Errorf("不支持的字段类型: %s", fv.Kind())
	}
	return nil
}

// ParseCSVToMap 读取 CSV：第一行视为表头，数据从第二行开始。
// 返回 map[string][]string，key 为表头列名，value 为该列所有数据行的值。
// trimLeadingSpace=true表示第一行为表头，数据从第2行开始取
func ParseCSVToMap(r io.Reader, trimLeadingSpace bool) (map[string][]string, error) {
	cr := csv.NewReader(r)
	cr.TrimLeadingSpace = trimLeadingSpace

	// 1. 第一行 = 表头
	header, err := cr.Read()
	if err != nil {
		return nil, fmt.Errorf("读取表头失败: %w", err)
	}

	// 2. 空表头列生成占位名
	headers := make([]string, len(header))
	for i, h := range header {
		h = strings.TrimSpace(h)
		if h == "" {
			h = fmt.Sprintf("列%d", i+1)
		}
		headers[i] = h
	}

	// 3. 初始化 map
	result := make(map[string][]string, len(headers))
	for _, h := range headers {
		result[h] = []string{}
	}

	// 4. 从第二行开始读数据
	for {
		record, err := cr.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("读取数据行失败: %w", err)
		}
		if isAllEmpty(record) { // 跳过全空行
			continue
		}
		for i, h := range headers {
			if i < len(record) {
				result[h] = append(result[h], record[i])
			}
		}
	}
	return result, nil
}

func isAllEmpty(record []string) bool {
	for _, v := range record {
		if v != "" {
			return false
		}
	}
	return true
}
