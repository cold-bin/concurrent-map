package cmap

import (
	"strconv"
)

type Int int

func (i Int) String() string {
	return strconv.Itoa(int(i))
}

type Int8 int8

func (i Int8) String() string {
	return strconv.Itoa(int(i))
}

type Int16 int16

func (i Int16) String() string {
	return strconv.Itoa(int(i))
}

type Int32 int32

func (i Int32) String() string {
	return strconv.Itoa(int(i))
}

type Int64 int64

func (i Int64) String() string {
	return strconv.FormatInt(int64(i), 10)
}

type Uint uint

func (i Uint) String() string {
	return strconv.Itoa(int(i))
}

type Uint8 uint8

func (i Uint8) String() string {
	return strconv.FormatUint(uint64(i), 10)
}

type Uint16 uint16

func (i Uint16) String() string {
	return strconv.FormatUint(uint64(i), 10)
}

type Uint32 uint32

func (i Uint32) String() string {
	return strconv.FormatUint(uint64(i), 10)
}

type Uint64 uint64

func (i Uint64) String() string {
	return strconv.FormatUint(uint64(i), 10)
}
