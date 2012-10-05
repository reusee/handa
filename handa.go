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
  MysqlConnPoolSize = 5
  SocketConnPoolSize = 30
)

type Handa struct {
  mysqlConnPool chan *autorc.Conn
  socketConnPool chan *tdh.Tdh

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
  self.socketConnPool = make(chan *tdh.Tdh, SocketConnPoolSize)
  for i := 0; i < SocketConnPoolSize; i++ {
    socket, err := tdh.New(host + ":" + tdhPort, "", "")
    if err != nil {
      fatal("Tdhsocket connect error")
    }
    self.socketConnPool <- socket
  }

  // load table schemas
  schema := make(map[string]*TableInfo)
  row, _, _ := self.mysqlQuery("show tables")
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
  r, _, _ := self.mysqlQuery("describe %s", tableName)
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

func (self *Handa) Set(table string, index string, key interface{}, fieldList string, values ...interface{}) {
  self.ensureTableExists(table)
  self.ensureColumnExists(table, index, key)
  self.ensureIndexExists(table, index)
  fields := strings.Split(fieldList, ",")
  for i, field := range fields {
    fields[i] = strings.TrimSpace(field)
    self.ensureColumnExists(table, fields[i], values[i])
  }
}

func (self *Handa) ensureTableExists(table string) {
  _, exists := self.schema[table]
  if !exists {
    self.mysqlQuery(`CREATE TABLE IF NOT EXISTS %s (
      serial SERIAL
    ) engine=InnoDB`, table)
    self.schema[table] = self.loadTableInfo(table)
  }
}

func (self *Handa) ensureColumnExists(table string, column string, key interface{}) {
  _, exists := self.schema[table].columnType[column]
  if !exists {
    var columnType string
    switch key.(type) {
    case bool:
      columnType = "BOOLEAN"
    case int, int8, int16, int32, int64, uint, uint8, uint32, uint64:
      columnType = "BIGINT(255)"
    case float32, float64:
      columnType = "DOUBLE"
    case string, []byte:
      columnType = "LONGBLOB"
    default:
      panic("unknow type")
    }
    self.mysqlQuery("ALTER TABLE `%s` ADD (`%s` %s NULL DEFAULT NULL)", table, column, columnType)
    self.schema[table] = self.loadTableInfo(table)
  }
}

func (self *Handa) ensureIndexExists(table string, column string) {
  if !self.schema[table].index[column] {
    self.mysqlQuery(`CREATE UNIQUE INDEX %s ON %s (%s)`, column, table, column)
    self.schema[table] = self.loadTableInfo(table)
  }
}

func fatal(format string, args ...interface{}) {
  log.Fatal(fmt.Sprintf(format, args...))
}
