package handa

import (
  tdh "github.com/reusee/go-tdhsocket"
)

// tdh

func (self *Handa) TdhGet(table string, index string, fields []string,
key [][]string, op uint8,
start uint32, limit uint32, filters []tdh.Filter) (rows [][][]byte, types []uint8, err error) {
  return self.NewCursor(false).TdhGet(table, index, fields, key, op, start, limit, filters)
}

func (self *Handa) TdhDelete(table string, index string, fields []string,
key [][]string, op uint8,
start uint32, limit uint32, filters []tdh.Filter) (int, error) {
  return self.NewCursor(false).TdhDelete(table, index, fields, key, op, start, limit, filters)
}

// insert and update

func (self *Handa) Update(table string, index string, key interface{}, fieldList string, values ...interface{}) (count int, change int, err error) {
  return self.NewCursor(false).Update(table, index, key, fieldList, values...)
}

func (self *Handa) Insert(table string, index string, key interface{}, fieldList string, values ...interface{}) (err error) {
  return self.NewCursor(false).Insert(table, index, key, fieldList, values...)
}

func (self *Handa) InsertUpdate(table string, index string, key interface{}, fieldList string, values ...interface{}) (err error) {
  return self.NewCursor(false).InsertUpdate(table, index, key, fieldList, values...)
}

func (self *Handa) UpdateInsert(table string, index string, key interface{}, fieldList string, values ...interface{}) (err error) {
  return self.NewCursor(false).UpdateInsert(table, index, key, fieldList, values...)
}

// col

func (self *Handa) GetCol(table string, index string) ([]string, error) {
  return self.NewCursor(false).GetCol(table, index)
}

func (self *Handa) GetMultiCol(table string, fields string) ([][]string, error) {
  return self.NewCursor(false).GetMultiCol(table, fields)
}

func (self *Handa) GetFilteredCol(table string, index string, filters ...string) ([]string, error) {
  return self.NewCursor(false).GetFilteredCol(table, index, filters...)
}

func (self *Handa) GetMultiFilteredCol(table string, fields string, filters ...string) ([][]string, error) {
  return self.NewCursor(false).GetMultiFilteredCol(table, fields, filters...)
}

func (self *Handa) GetRangedCol(table string, index string, start int, limit int) ([]string, error) {
  return self.NewCursor(false).GetRangedCol(table, index, start, limit)
}

func (self *Handa) GetMultiRangedCol(table string, fields string, start int, limit int) ([][]string, error) {
  return self.NewCursor(false).GetMultiRangedCol(table, fields, start, limit)
}

func (self *Handa) GetRangedFilteredCol(table string, index string, start int, limit int, filters ...string) ([]string, error) {
  return self.NewCursor(false).GetRangedFilteredCol(table, index, start, limit, filters...)
}

func (self *Handa) GetMultiRangedFilteredCol(table string, index string, start int, limit int, filters ...string) ([][]string, error) {
  return self.NewCursor(false).GetMultiRangedFilteredCol(table, index, start, limit, filters...)
}

// map

func (self *Handa) GetMap(table string, index string, field string) (map[string]string, error) {
  return self.NewCursor(false).GetMap(table, index, field)
}

func (self *Handa) GetMultiMap(table string, index string, fields string) (map[string][]string, error) {
  return self.NewCursor(false).GetMultiMap(table, index, fields)
}

func (self *Handa) GetFilteredMap(table string, index string, field string, filters ...string) (map[string]string, error) {
  return self.NewCursor(false).GetFilteredMap(table, index, field, filters...)
}

func (self *Handa) GetMultiFilteredMap(table string, index string, fields string, filters ...string) (map[string][]string, error) {
  return self.NewCursor(false).GetMultiFilteredMap(table, index, fields, filters...)
}

func (self *Handa) GetRangedMap(table string, index string, field string, start int, limit int) (map[string]string, error) {
  return self.NewCursor(false).GetRangedMap(table, index, field, start, limit)
}

func (self *Handa) GetMultiRangedMap(table string, index string, fields string, start int, limit int) (map[string][]string, error) {
  return self.NewCursor(false).GetMultiRangedMap(table, index, fields, start, limit)
}

func (self *Handa) GetRangedFilteredMap(table string, index string, field string, start int, limit int, filters ...string) (map[string]string, error) {
  return self.NewCursor(false).GetRangedFilteredMap(table, index, field, start, limit, filters...)
}

func (self *Handa) GetMultiRangedFilteredMap(table string, index string, fields string, start int, limit int, filters ...string) (map[string][]string, error) {
  return self.NewCursor(false).GetMultiRangedFilteredMap(table, index, fields, start, limit, filters...)
}
