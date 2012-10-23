package handa

import (
  tdh "github.com/reusee/go-tdhsocket"
  "regexp"
  "errors"
  "fmt"
  "strings"
)

func init() {
  fmt.Print("")
}

type Cursor struct {
  isValid bool // conn释放后，isValid为false，不能执行任何操作
  handa *Handa

  isBatch bool
  conn *tdh.Conn
  end chan bool
}

// update and insert

func (self *Cursor) Update(table string, index string, key interface{}, fieldList string, values ...interface{}) (count int, change int, err error) {
  if !self.isValid { panic("Using an invalid cursor") }
  if !self.isBatch { defer func() {
    self.end <- true
  }()}
  dbIndex, _, dbKeys, _, _, dbFields, dbValues := self.handa.checkSchemaAndConvertData(table, index, key, fieldList, values...)
  count, change, err = self.conn.Update(self.handa.dbname, table, dbIndex,
    dbFields,
    [][]string{dbKeys}, tdh.EQ,
    0, 0, nil, dbValues)
  return
}

func (self *Cursor) Insert(table string, index string, keys interface{}, fieldList string, values ...interface{}) (err error) {
  if !self.isValid { panic("Using an invalid cursor") }
  if !self.isBatch { defer func() {
    self.end <- true
  }()}

  dbIndex, dbIndexStrs, dbKeys, indexStrs, keyStrs, dbFields, dbValues := self.handa.checkSchemaAndConvertData(table, index, keys, fieldList, values...)
  err = self.conn.Insert(self.handa.dbname, table, dbIndex,
    append(append(dbFields, indexStrs...), dbIndexStrs...),
    append(append(dbValues, keyStrs...), dbKeys...))
  return
}

func (self *Cursor) UpdateInsert(table string, index string, key interface{}, fieldList string, values ...interface{}) (err error) {
  if !self.isValid { panic("Using an invalid cursor") }
  if self.isBatch { panic("Not permit in batch mode") }
  if !self.isBatch { defer func() {
    self.end <- true
  }()}
  dbIndex, dbIndexStrs, dbKeys, indexStrs, keyStrs, dbFields, dbValues := self.handa.checkSchemaAndConvertData(table, index, key, fieldList, values...)
  var count int
  count, _, err = self.conn.Update(self.handa.dbname, table, dbIndex,
    dbFields,
    [][]string{dbKeys}, tdh.EQ,
    0, 0, nil, dbValues)
  if err != nil {
    return
  }
  if count == 0 { // not exists, then insert
    err = self.conn.Insert(self.handa.dbname, table, dbIndex,
    append(append(dbFields, indexStrs...), dbIndexStrs...),
    append(append(dbValues, keyStrs...), dbKeys...))
    if err != nil {
      e, _ := err.(*tdh.Error)
      if e.ClientStatus == tdh.CLIENT_STATUS_DB_ERROR && e.ErrorCode == 121 {
        err = nil
      }
    }
  }
  return
}

func (self *Cursor) InsertUpdate(table string, index string, key interface{}, fieldList string, values ...interface{}) (err error) {
  if !self.isValid { panic("Using an invalid cursor") }
  if self.isBatch { panic("Not permit in batch mode") }
  if !self.isBatch { defer func() {
    self.end <- true
  }()}
  dbIndex, dbIndexStrs, dbKeys, indexStrs, keyStrs, dbFields, dbValues := self.handa.checkSchemaAndConvertData(table, index, key, fieldList, values...)
  err = self.conn.Insert(self.handa.dbname, table, dbIndex,
    append(append(dbFields, indexStrs...), dbIndexStrs...),
    append(append(dbValues, keyStrs...), dbKeys...))
  if err != nil {
    e, _ := err.(*tdh.Error)
    if e.ClientStatus == tdh.CLIENT_STATUS_DB_ERROR && e.ErrorCode == 121 { // update
      _, _, err = self.conn.Update(self.handa.dbname, table, dbIndex,
        dbFields,
        [][]string{dbKeys}, tdh.EQ,
        0, 0, nil, dbValues)
    }
  }
  return
}

func (self *Cursor) Commit() ([]Result, error) {
  if !self.isValid { panic("Using an invalid cursor") }
  if !self.isBatch { return nil, nil }
  res, err := self.conn.Commit()
  if err != nil {
    return nil, err
  }
  ret := make([]Result, len(res))
  for i, r := range res {
    switch r.T {
    case tdh.INSERT:
      ret[i] = Result{INSERT, r.Change, r.Count, r.Err}
    case tdh.UPDATE:
      ret[i] = Result{UPDATE, r.Change, r.Count, r.Err}
    case tdh.DELETE:
      ret[i] = Result{DELETE, r.Change, r.Count, r.Err}
    }
  }
  self.end <- true
  return ret, nil
}

type Result struct {
  T int
  Change int
  Count int
  Err error
}

const (
  INSERT = iota
  UPDATE
  DELETE
)

// get

func (self *Cursor) getRows(table string, index string, fields []string, filterStrs []string, start int, limit int) (rows [][][]byte, err error) {
  if !self.isValid { panic("Using an invalid cursor") }
  if self.isBatch { panic("Not permit in batch mode") }
  if !self.isBatch { defer func() {
    self.end <- true
  }()}

  var isString []bool
  indexCols := strings.Split(index, "$")
  index, isString = self.handa.ensureIndexExists(table, indexCols...)

  minKey := make([]string, len(isString)) // match all rows
  for i, t := range isString {
    if t {
      minKey[i] = "(null)"
    } else {
      minKey[i] = "-9223372036854775808"
    }
  }
  key := [][]string{minKey}
  op := tdh.GT
  tableScan := true

  var filters, convertedFilters []tdh.Filter
  if filterStrs != nil {
    convertedFilters, err = convertFilterStrings(filterStrs)
    if err != nil {
      return
    }
    filters = make([]tdh.Filter, 0, len(convertedFilters))
    for i, filter := range convertedFilters { // convert text filed to hash field
      if self.handa.schema[table].columnType[filter.Field] == ColTypeLongString {
        filters[i].Field = "hash_" + filters[i].Field
        filters[i].Value = mmh3Hex(filters[i].Value)
      }
      if tableScan && filter.Field == indexCols[0] && isConvertableOp(filter.Op) { // use key/op to filter
        key = [][]string{[]string{filter.Value}}
        op = convertOp(filter.Op)
        tableScan = false
        continue
      }
      filters = append(filters, filter)
    }
  }

  rows, _, err = self.conn.Get(self.handa.dbname, table, index, fields,
    key, op, uint32(start), uint32(limit), filters)
  return
}

func isConvertableOp(op uint8) (t bool) {
  switch op {
  case tdh.FILTER_EQ, tdh.FILTER_LE, tdh.FILTER_LT, tdh.FILTER_GE, tdh.FILTER_GT:
    t = true
  }
  return
}

func convertOp(op uint8) (ret uint8) {
  switch op {
  case tdh.FILTER_EQ:
    ret = tdh.EQ
  case tdh.FILTER_LT:
    ret = tdh.LT
  case tdh.FILTER_LE:
    ret = tdh.LE
  case tdh.FILTER_GT:
    ret = tdh.GT
  case tdh.FILTER_GE:
    ret = tdh.GE
  }
  return
}

func (self *Cursor) TdhGet(table string, index string, fields []string,
key [][]string, op uint8,
start uint32, limit uint32, filters []tdh.Filter) (rows [][][]byte, types []uint8, err error) {
  if !self.isValid { panic("Using an invalid cursor") }
  if self.isBatch { panic("Not permit in batch mode") }
  if !self.isBatch { defer func() {
    self.end <- true
  }()}

  rows, types, err = self.conn.Get(self.handa.dbname, table, index, fields,
  key, op, start, limit, filters)
  return
}

// get col

func (self *Cursor) getCol(table string, index string, filterStrs []string, start int, limit int) ([]string, error) {
  var fields []string
  if indexSplit := strings.Split(index, ","); len(indexSplit) > 1 {
    index = strings.TrimSpace(indexSplit[0])
    fields = []string{strings.TrimSpace(indexSplit[1])}
  } else {
    fields = []string{index}
  }
  rows, err := self.getRows(table, index, fields, filterStrs, start, limit)
  if err != nil {
    return nil, err
  }
  ret := make([]string, len(rows))
  for i, row := range rows {
    ret[i] = string(row[0])
  }
  return ret, nil
}

func (self *Cursor) getMultiCol(table string, fieldsStr string, filterStrs []string, start int, limit int) ([][]string, error) {
  fields := make([]string, 0)
  var index string
  for i, field := range strings.Split(fieldsStr, ",") {
    if i == 0 {
      index = field
    }
    if !strings.Contains(field, "$") {
      fields = append(fields, strings.TrimSpace(field))
    }
  }
  rows, err := self.getRows(table, index, fields, filterStrs, start, limit)
  if err != nil {
    return nil, err
  }
  ret := make([][]string, len(rows))
  for i, row := range rows {
    ret[i] = make([]string, len(row))
    for j, col := range row {
      ret[i][j] = string(col)
    }
  }
  return ret, nil
}

func (self *Cursor) GetCol(table string, index string) ([]string, error) {
  return self.getCol(table, index, nil, 0, 0)
}

func (self *Cursor) GetMultiCol(table string, fields string) ([][]string, error) {
  return self.getMultiCol(table, fields, nil, 0, 0)
}

func (self *Cursor) GetFilteredCol(table string, index string, filters ...string) ([]string, error) {
  return self.getCol(table, index, filters, 0, 0)
}

func (self *Cursor) GetMultiFilteredCol(table string, fields string, filters ...string) ([][]string, error) {
  return self.getMultiCol(table, fields, filters, 0, 0)
}

func (self *Cursor) GetRangedCol(table string, index string, start int, limit int) ([]string, error) {
  return self.getCol(table, index, nil, start, limit)
}

func (self *Cursor) GetMultiRangedCol(table string, fields string, start int, limit int) ([][]string, error) {
  return self.getMultiCol(table, fields, nil, start, limit)
}

func (self *Cursor) GetRangedFilteredCol(table string, index string, start int, limit int, filters ...string) ([]string, error) {
  return self.getCol(table, index, filters, start, limit)
}

func (self *Cursor) GetMultiRangedFilteredCol(table string, index string, start int, limit int, filters ...string) ([][]string, error) {
  return self.getMultiCol(table, index, filters, start, limit)
}

// get map

func (self *Cursor) getMap(table string, index string, field string, filterStrs []string, start int, limit int) (map[string]string, error) {
  var fields []string
  if indexSplit := strings.Split(index, ","); len(indexSplit) > 1 {
    index = strings.TrimSpace(indexSplit[0])
    fields = []string{strings.TrimSpace(indexSplit[1]), field}
  } else {
    fields = []string{index, field}
  }
  rows, err := self.getRows(table, index, fields, filterStrs, start, limit)
  if err != nil {
    return nil, err
  }
  ret := make(map[string]string)
  for _, row := range rows {
    ret[string(row[0])] = string(row[1])
  }
  return ret, nil
}

func (self *Cursor) getMultiMap(table string, index string, fieldsStr string, filterStrs []string, start int, limit int) (map[string][]string, error) {
  var fields []string
  if indexSplit := strings.Split(index, ","); len(indexSplit) > 1 {
    index = strings.TrimSpace(indexSplit[0])
    fields = []string{strings.TrimSpace(indexSplit[1])}
  } else {
    fields = []string{index}
  }
  for _, field := range strings.Split(fieldsStr, ",") {
    fields = append(fields, strings.TrimSpace(field))
  }
  rows, err := self.getRows(table, index, fields, filterStrs, start, limit)
  if err != nil {
    return nil, err
  }
  ret := make(map[string][]string)
  for _, row := range rows {
    values := make([]string, len(row) - 1)
    for i, col := range row[1:] {
      values[i] = string(col)
    }
    ret[string(row[0])] = values
  }
  return ret, nil
}

func (self *Cursor) GetMap(table string, index string, field string) (map[string]string, error) {
  return self.getMap(table, index, field, nil, 0, 0)
}

func (self *Cursor) GetMultiMap(table string, index string, fields string) (map[string][]string, error) {
  return self.getMultiMap(table, index, fields, nil, 0, 0)
}

func (self *Cursor) GetFilteredMap(table string, index string, field string, filters ...string) (map[string]string, error) {
  return self.getMap(table, index, field, filters, 0, 0)
}

func (self *Cursor) GetMultiFilteredMap(table string, index string, fields string, filters ...string) (map[string][]string, error) {
  return self.getMultiMap(table, index, fields, filters, 0, 0)
}

func (self *Cursor) GetRangedMap(table string, index string, field string, start int, limit int) (map[string]string, error) {
  return self.getMap(table, index, field, nil, start, limit)
}

func (self *Cursor) GetMultiRangedMap(table string, index string, fields string, start int, limit int) (map[string][]string, error) {
  return self.getMultiMap(table, index, fields, nil, start, limit)
}

func (self *Cursor) GetRangedFilteredMap(table string, index string, field string, start int, limit int, filters ...string) (map[string]string, error) {
  return self.getMap(table, index, field, filters, start, limit)
}

func (self *Cursor) GetMultiRangedFilteredMap(table string, index string, fields string, start int, limit int, filters ...string) (map[string][]string, error) {
  return self.getMultiMap(table, index, fields, filters, start, limit)
}

// misc

func convertFilterStrings(strs []string) (out []tdh.Filter, err error) {
  out = make([]tdh.Filter, len(strs))
  expPat, _ := regexp.Compile("([^=><!]*)(=|>=|<=|>|<|!=)(.*)")
  for i, s := range strs {
    matches := expPat.FindAllStringSubmatch(s, len(s))
    if len(matches) < 0 || len(matches[0]) < 4 {
      return nil, errors.New("filter invalid: " + s)
    }
    var op uint8
    switch matches[0][2] {
    case "=":
      op = tdh.FILTER_EQ
    case ">=":
      op = tdh.FILTER_GE
    case "<=":
      op = tdh.FILTER_LE
    case ">":
      op = tdh.FILTER_GT
    case "<":
      op = tdh.FILTER_LT
    case "!=":
      op = tdh.FILTER_NOT
    }
    out[i] = tdh.Filter{matches[0][1], op, matches[0][3]}
  }
  return
}
