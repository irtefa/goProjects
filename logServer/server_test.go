package main

import (
	"strings"
	"testing"
)

func Test_execGrep(t *testing.T) { //test function starts with "Test" and takes a pointer to type testing.T
	r := execGrep("^^", "test.log", "test")
	if strings.EqualFold(r, "YOUR GREP COMMAND IS INVALID") {
		t.Log("test passed")
	} else {
		t.Error("test failed") // log some info if you want
	}
}
