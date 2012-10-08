package handa

import (
  tdh "github.com/reusee/go-tdhsocket"
  "regexp"
  "errors"
)

type Cursor struct {
  isValid bool // conn释放后，isValid为false，不能执行任何操作
  handa *Handa

  isBatch bool
  conn *tdh.Conn
  end chan bool
}

func (self *Cursor) Update(table string, index string, key interface{}, fieldList string, values ...interface{}) (count int, change int, err error) {
  if !self.isValid { panic("Using an invalid cursor") }
  if !self.isBatch { defer func() {
    self.end <- true
  }()}
  keyStr, fields, valueStrs := self.handa.checkSchema(table, index, key, fieldList, values...)
  count, change, err = self.conn.Update(self.handa.dbname, table, index, fields, [][]string{[]string{keyStr}}, 
  tdh.EQ, 0, 0, nil, valueStrs)
  if err != nil {
    return
  }
  return
}

func (self *Cursor) Insert(table string, index string, key interface{}, fieldList string, values ...interface{}) (err error) {
  if !self.isValid { panic("Using an invalid cursor") }
  if !self.isBatch { defer func() {
    self.end <- true
  }()}
  keyStr, fields, valueStrs := self.handa.checkSchema(table, index, key, fieldList, values...)
  err = self.conn.Insert(self.handa.dbname, table, index, append(fields, index), append(valueStrs, keyStr))
  return
}

func (self *Cursor) UpdateInsert(table string, index string, key interface{}, fieldList string, values ...interface{}) (err error) {
  if !self.isValid { panic("Using an invalid cursor") }
  if self.isBatch { panic("Not permit in batch mode") }
  if !self.isBatch { defer func() {
    self.end <- true
  }()}
  keyStr, fields, valueStrs := self.handa.checkSchema(table, index, key, fieldList, values...)
  var count int
  count, _, err = self.conn.Update(self.handa.dbname, table, index, fields, [][]string{[]string{keyStr}}, 
  tdh.EQ, 0, 0, nil, valueStrs)
  if err != nil {
    return
  }
  if count == 0 { // not exists, then insert
    err = self.conn.Insert(self.handa.dbname, table, index, append(fields, index), append(valueStrs, keyStr))
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
  keyStr, fields, valueStrs := self.handa.checkSchema(table, index, key, fieldList, values...)
  err = self.conn.Insert(self.handa.dbname, table, index, append(fields, index), append(valueStrs, keyStr))
  if err != nil {
    e, _ := err.(*tdh.Error)
    if e.ClientStatus == tdh.CLIENT_STATUS_DB_ERROR && e.ErrorCode == 121 { // update
      _, _, err = self.conn.Update(self.handa.dbname, table, index, fields, [][]string{[]string{keyStr}}, 
      tdh.EQ, 0, 0, nil, valueStrs)
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

func (self *Cursor) getRows(table string, index string, fields []string, filterStrs []string) ([][][]byte, error) {
  if !self.isValid { panic("Using an invalid cursor") }
  if self.isBatch { panic("Not permit in batch mode") }
  if !self.isBatch { defer func() {
    self.end <- true
  }()}
  self.handa.ensureIndexExists(table, index)
  var filters []tdh.Filter
  var err error
  if filterStrs != nil {
    filters, err = convertFilterStrings(filterStrs)
    if err != nil {
      return nil, err
    }
  }
  //TODO keys和op也应为参数
  rows, _, err := self.conn.Get(self.handa.dbname, table, index, fields,
    [][]string{[]string{"(null)"}}, tdh.GT, 0, 0, filters)
  if err != nil {
    return nil, err
  }
  return rows, nil
}

// get col

func (self *Cursor) getCol(table string, index string, filterStrs []string) ([]string, error) {
  rows, err := self.getRows(table, index, []string{index}, filterStrs)
  if err != nil {
    return nil, err
  }
  ret := make([]string, len(rows))
  for i, row := range rows {
    for _, col := range row {
      ret[i] = string(col)
    }
  }
  return ret, nil
}

func (self *Cursor) GetCol(table string, index string) ([]string, error) {
  return self.getCol(table, index, nil)
}

func (self *Cursor) GetFilteredCol(table string, index string, filters ...string) ([]string, error) {
  return self.getCol(table, index, filters)
}

// get map

func (self *Cursor) getMap(table string, index string, field string, filterStrs []string) (map[string]string, error) {
  rows, err := self.getRows(table, index, []string{index, field}, filterStrs)
  if err != nil {
    return nil, err
  }
  ret := make(map[string]string)
  for _, row := range rows {
    ret[string(row[0])] = string(row[1])
  }
  return ret, nil
}

func (self *Cursor) GetMap(table string, index string, field string) (map[string]string, error) {
  return self.getMap(table, index, field, nil)
}

func (self *Cursor) GetFilteredMap(table string, index string, field string, filters ...string) (map[string]string, error) {
  return self.getMap(table, index, field, filters)
}

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
