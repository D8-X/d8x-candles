package utils

import (
	"fmt"
	"testing"
)

func TestDec2Hex(t *testing.T) {
	tokenId := "21742633143463906290569050155826241533067272736897614950488156847949938836455"
	idHex, err := Dec2Hex(tokenId)
	if err != nil {
		fmt.Println(err.Error())
		t.FailNow()
	}
	fmt.Println("hex id =", idHex)
}
