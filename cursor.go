package handa

import (
  tdh "github.com/reusee/go-tdhsocket"
)

type Cursor struct {
  isValid bool // conn释放后，isValid为false，不能执行任何操作
  handa *Handa

  isBatch bool // 是否为batch cursor
  tdhConn *tdh.Conn // batch cursor所持有的conn
  commit chan bool // 监听commit事件的chan
}

func (self *Cursor) Update(table string, index string, key interface{}, fieldList string, values ...interface{}) (count int, change int, err error) {
  if !self.isValid { panic("Using an invalid cursor") }
  if !self.isBatch { defer func() {
    self.isValid = false
  }()}
  keyStr, fields, valueStrs := self.handa.checkSchema(table, index, key, fieldList, values...)
  if !self.isBatch {
    self.handa.withSocket(func (db *tdh.Conn) {
      count, change, err = db.Update(self.handa.dbname, table, index, fields, [][]string{[]string{keyStr}}, 
      tdh.EQ, 0, 0, nil, valueStrs)
      if err != nil {
        return
      }
    })
  } else {
    //TODO
  }
  return
}

func (self *Cursor) Insert(table string, index string, key interface{}, fieldList string, values ...interface{}) (err error) {
  if !self.isValid { panic("Using an invalid cursor") }
  if !self.isBatch { defer func() {
    self.isValid = false
  }()}
  keyStr, fields, valueStrs := self.handa.checkSchema(table, index, key, fieldList, values...)
  if !self.isBatch {
    self.handa.withSocket(func (db *tdh.Conn) {
      err = db.Insert(self.handa.dbname, table, index, append(fields, index), append(valueStrs, keyStr))
    })
  } else {
    //TODO
  }
  return
}

func (self *Cursor) UpdateInsert(table string, index string, key interface{}, fieldList string, values ...interface{}) (err error) {
  if !self.isValid { panic("Using an invalid cursor") }
  if !self.isBatch { defer func() {
    self.isValid = false
  }()}
  keyStr, fields, valueStrs := self.handa.checkSchema(table, index, key, fieldList, values...)
  if !self.isBatch {
    self.handa.withSocket(func (db *tdh.Conn) {
      var count int
      count, _, err = db.Update(self.handa.dbname, table, index, fields, [][]string{[]string{keyStr}}, 
      tdh.EQ, 0, 0, nil, valueStrs)
      if err != nil {
        return
      }
      if count == 0 { // not exists, then insert
        err = db.Insert(self.handa.dbname, table, index, append(fields, index), append(valueStrs, keyStr))
        if err != nil {
          e, _ := err.(*tdh.Error)
          if e.ClientStatus == tdh.CLIENT_STATUS_DB_ERROR && e.ErrorCode == 121 {
            err = nil
          }
        }
      }
    })
  } else {
    //TODO
  }
  return
}

func (self *Cursor) InsertUpdate(table string, index string, key interface{}, fieldList string, values ...interface{}) (err error) {
  if !self.isValid { panic("Using an invalid cursor") }
  if !self.isBatch { defer func() {
    self.isValid = false
  }()}
  keyStr, fields, valueStrs := self.handa.checkSchema(table, index, key, fieldList, values...)
  if !self.isBatch {
    self.handa.withSocket(func (db *tdh.Conn) {
      err = db.Insert(self.handa.dbname, table, index, append(fields, index), append(valueStrs, keyStr))
      if err != nil {
        e, _ := err.(*tdh.Error)
        if e.ClientStatus == tdh.CLIENT_STATUS_DB_ERROR && e.ErrorCode == 121 { // update
          _, _, err = db.Update(self.handa.dbname, table, index, fields, [][]string{[]string{keyStr}}, 
          tdh.EQ, 0, 0, nil, valueStrs)
        }
      }
    })
  } else {
    //TODO
  }
  return
}

