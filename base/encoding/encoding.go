// Copyright 2022 gorse Project Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package encoding

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"github.com/chewxy/math32"
	"github.com/juju/errors"
	"io"
	"reflect"
	"strconv"
)

// Hex returns the hex form of a 64-bit integer.
func Hex(v int64) string {
	return fmt.Sprintf("%x", v)
}

// WriteMatrix writes matrix to byte stream.
func WriteMatrix(w io.Writer, m [][]float32) error {
	for i := range m {
		err := binary.Write(w, binary.LittleEndian, m[i])
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// ReadMatrix reads matrix from byte stream.
func ReadMatrix(r io.Reader, m [][]float32) error {
	for i := range m {
		err := binary.Read(r, binary.LittleEndian, m[i])
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// WriteString writes string to byte stream.
func WriteString(w io.Writer, s string) error {
	return WriteBytes(w, []byte(s))
}

// ReadString reads string from byte stream.
func ReadString(r io.Reader) (string, error) {
	data, err := ReadBytes(r)
	return string(data), err
}

// WriteBytes writes bytes to byte stream.
func WriteBytes(w io.Writer, s []byte) error {
	err := binary.Write(w, binary.LittleEndian, int32(len(s)))
	if err != nil {
		return err
	}
	n, err := w.Write(s)
	if err != nil {
		return err
	} else if n != len(s) {
		return errors.New("fail to write string")
	}
	return nil
}

// ReadBytes reads bytes from byte stream.
func ReadBytes(r io.Reader) ([]byte, error) {
	var length int32
	err := binary.Read(r, binary.LittleEndian, &length)
	if err != nil {
		return nil, err
	}
	data := make([]byte, length)
	n, err := r.Read(data)
	if err != nil {
		return nil, err
	} else if n != len(data) {
		return nil, errors.New("fail to read string")
	}
	return data, nil
}

// WriteGob writes object to byte stream.
func WriteGob(w io.Writer, v interface{}) error {
	buffer := bytes.NewBuffer(nil)
	encoder := gob.NewEncoder(buffer)
	err := encoder.Encode(v)
	if err != nil {
		return err
	}
	return WriteBytes(w, buffer.Bytes())
}

// ReadGob read object from byte stream.
func ReadGob(r io.Reader, v interface{}) error {
	data, err := ReadBytes(r)
	if err != nil {
		return err
	}
	buffer := bytes.NewBuffer(data)
	decoder := gob.NewDecoder(buffer)
	return decoder.Decode(v)
}

func FormatFloat32(val float32) string {
	return strconv.FormatFloat(float64(val), 'f', -1, 32)
}

func ParseFloat32(val string) float32 {
	f, err := strconv.ParseFloat(val, 32)
	if err != nil {
		return math32.NaN()
	}
	return float32(f)
}

func ArrayBytes[T any](val []T) uintptr {
	byteCount := reflect.TypeOf(val).Size()
	if len(val) > 0 {
		byteCount += reflect.TypeOf(val).Elem().Size() * uintptr(len(val))
	}
	return byteCount
}

func MatrixBytes[T any](val [][]T) uintptr {
	byteCount := reflect.TypeOf(val).Size()
	if len(val) > 0 {
		rowSize := len(val[0])
		byteCount += reflect.TypeOf(val).Elem().Size() * uintptr(len(val))
		byteCount += reflect.TypeOf(val).Elem().Elem().Size() * uintptr(len(val)*rowSize)
	}
	return byteCount
}

func StringsBytes(val []string) uintptr {
	byteCount := reflect.TypeOf(val).Size()
	byteCount += reflect.TypeOf(val).Elem().Size() * uintptr(len(val))
	for _, s := range val {
		byteCount += 2 * uintptr(len(s))
	}
	return byteCount
}
