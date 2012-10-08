package handa

import (
  "log"
  "fmt"
  "strings"

  tdh "github.com/reusee/go-tdhsocket"

  "github.com/ziutek/mymysql/autorc"
  "github.com/ziutek/mymysql/mysql"
  _ "github.com/ziutek/mymysql/native" 
)

var (
  MysqlConnPoolSize = 16
  SocketConnPoolSize = 64
)

type Handa struct {
  mysqlConnPool chan *autorc.Conn
  socketConnPool chan *tdh.Conn

  dbname string
  schema map[string]*TableInfo
}

func New(host string, port string, user string, password string, database string, tdhPort string) *Handa {
  self := new(Handa)

  // init database connection pool
  self.dbname = database
  self.mysqlConnPool = make(chan *autorc.Conn, MysqlConnPoolSize)
  for i := 0; i < MysqlConnPoolSize; i++ {
    conn := autorc.New("tcp", "", host + ":" + port, user, password, database)
    conn.Register("set names utf8")
    self.mysqlConnPool <- conn
  }
  self.socketConnPool = make(chan *tdh.Conn, SocketConnPoolSize)
  for i := 0; i < SocketConnPoolSize; i++ {
    socket, err := tdh.New(host + ":" + tdhPort, "", "")
    if err != nil {
      fatal("Tdhsocket connect error")
    }
    self.socketConnPool <- socket
  }

  // load table schemas
  schema := make(map[string]*TableInfo)
  row, _, _ := self.mysqlQuery("SHOW TABLES")
  for _, row := range row {
    tableName := row.Str(0)
    schema[tableName] = self.loadTableInfo(tableName)
  }
  self.schema = schema

  return self
}

func (self *Handa) loadTableInfo(tableName string) *TableInfo {
  tableInfo := &TableInfo{
    name: tableName,
    columnType: make(map[string]int),
    index: make(map[string]bool),
  }
  r, _, _ := self.mysqlQuery("DESCRIBE %s", tableName)
  for _, c := range r {
    columnName := c.Str(0)
    columnType := c.Str(1)
    columnKeyType := c.Str(3)
    switch columnType {
    case "tinyint(1)":
      tableInfo.columnType[columnName] = ColTypeBool
    case "bigint(255)":
      tableInfo.columnType[columnName] = ColTypeInt
    case "double":
      tableInfo.columnType[columnName] = ColTypeFloat
    case "longblob":
      tableInfo.columnType[columnName] = ColTypeString
    }
    if columnKeyType == "UNI" {
      tableInfo.index[columnName] = true
    }
  }
  return tableInfo
}

const (
  ColTypeBool = iota
  ColTypeInt
  ColTypeFloat
  ColTypeString
)

type TableInfo struct {
  name string
  columnType map[string]int
  index map[string]bool
}

func (self *Handa) mysqlQuery(sql string, args ...interface{}) ([]mysql.Row, mysql.Result, error) {
  conn := <-self.mysqlConnPool
  defer func() {
    self.mysqlConnPool <- conn
  }()
  return conn.Query(sql, args...)
}

func (self *Handa) checkSchema(table string, index string, key interface{}, fieldList string, values ...interface{}) (string, []string, []string) {
  self.ensureTableExists(table)
  keyStr, t, _ := convertToString(key)
  self.ensureColumnExists(table, index, t)
  self.ensureIndexExists(table, index)
  fields := strings.Split(fieldList, ",")
  valueStrs := make([]string, len(values))
  for i, field := range fields {
    fields[i] = strings.TrimSpace(field)
    valueStr, t, _ := convertToString(values[i])
    valueStrs[i] = valueStr
    self.ensureColumnExists(table, fields[i], t)
  }
  return keyStr, fields, valueStrs
}

func (self *Handa) withTableCacheOff(fun func()) {
  _, _, err := self.mysqlQuery("SET GLOBAL tdh_socket_cache_table_on=0")
  defer self.mysqlQuery("SET GLOBAL tdh_socket_cache_table_on=1")
  if err != nil {
    panic("need SUPER privileges to set global variable")
  }
  fun()
}

func (self *Handa) ensureTableExists(table string) {
  _, exists := self.schema[table]
  if !exists {
    self.withTableCacheOff(func() {
      self.mysqlQuery(`CREATE TABLE IF NOT EXISTS %s (
        serial SERIAL
      ) engine=InnoDB`, table)
    })
    self.schema[table] = self.loadTableInfo(table)
  }
}

func (self *Handa) ensureColumnExists(table string, column string, t int) {
  _, exists := self.schema[table].columnType[column]
  if !exists {
    var columnType string
    switch t {
    case ColTypeBool:
      columnType = "BOOLEAN"
    case ColTypeInt:
      columnType = "BIGINT(255)"
    case ColTypeFloat:
      columnType = "DOUBLE"
    case ColTypeString:
      columnType = "LONGBLOB"
    }
    self.withTableCacheOff(func() {
      self.mysqlQuery("ALTER TABLE `%s` ADD (`%s` %s NULL DEFAULT NULL)", table, column, columnType)
    })
    self.schema[table] = self.loadTableInfo(table)
  }
}

func (self *Handa) ensureIndexExists(table string, column string) {
  if !self.schema[table].index[column] {
    self.withTableCacheOff(func() {
      self.mysqlQuery(`CREATE UNIQUE INDEX %s ON %s (%s)`, column, table, column)
    })
    self.schema[table] = self.loadTableInfo(table)
  }
}

func (self *Handa) NewCursor(isBatch bool) *Cursor {
  cursor := &Cursor{
    isValid: true,
    handa: self,
    isBatch: isBatch,
    end: make(chan bool, 1),
  }
  init := make(chan bool, 1)
  go func() {
    conn := <-self.socketConnPool
    defer func() {
      self.socketConnPool <- conn
    }()
    cursor.conn = conn
    if cursor.isBatch {
      cursor.conn.Batch()
    }
    init <- true
    <-cursor.end
    cursor.isValid = false
  }()
  <-init
  return cursor
}

func (self *Handa) Batch() *Cursor {
  return self.NewCursor(true)
}

func fatal(format string, args ...interface{}) {
  log.Fatal(fmt.Sprintf(format, args...))
}
