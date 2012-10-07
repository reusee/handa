package handa

import (
  tdh "github.com/reusee/go-tdhsocket"
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

func (self *Cursor) Commit() {
  if !self.isValid { panic("Using an invalid cursor") }
  if !self.isBatch { return }
  self.conn.Commit() //TODO return result set
  self.end <- true
}
