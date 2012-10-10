package handa

import (
  "fmt"
)

func convertToString(in interface{}) (ret string, t int) {
  switch v := in.(type) {
  case bool:
    ret = "0"
    if v {
      ret = "1"
    }
    t = ColTypeBool
  case int: 
    ret = fmt.Sprintf("%d", v)
    t = ColTypeInt
  case int8:
    ret = fmt.Sprintf("%d", v)
    t = ColTypeInt
  case int16: 
    ret = fmt.Sprintf("%d", v)
    t = ColTypeInt
  case int32:
    ret = fmt.Sprintf("%d", v)
    t = ColTypeInt
  case int64:
    ret = fmt.Sprintf("%d", v)
    t = ColTypeInt
  case uint: 
    ret = fmt.Sprintf("%d", v)
    t = ColTypeInt
  case uint8:
    ret = fmt.Sprintf("%d", v)
    t = ColTypeInt
  case uint32:
    ret = fmt.Sprintf("%d", v)
    t = ColTypeInt
  case uint64:
    ret = fmt.Sprintf("%d", v)
    t = ColTypeInt
  case float32:
    ret = fmt.Sprintf("%f", v)
    t = ColTypeFloat
  case float64:
    ret = fmt.Sprintf("%f", v)
    t = ColTypeFloat
  case string:
    ret = v
    t = ColTypeString
  case []byte:
    ret = string(v)
    t = ColTypeString
  default:
    panic("unknown type")
  }
  return ret, t
}
