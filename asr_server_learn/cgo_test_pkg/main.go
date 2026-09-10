package main

// #cgo windows,amd64 LDFLAGS: -L../lib/ten-vad/lib/Windows/x64 -lten_vad
// #cgo windows,amd64 CFLAGS: -I../lib/ten-vad/include
// #include "ten_vad.h"
import "C"

import "fmt"

func main() {
	fmt.Println(C.GoString(C.ten_vad_get_version()))
}
