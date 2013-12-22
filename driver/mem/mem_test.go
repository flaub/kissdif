package mem

import (
	"github.com/flaub/kissdif/driver/test"
	. "github.com/motain/gocheck"
	"testing"
)

type TestDriver struct {
	*test.TestSuite
}

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) { TestingT(t) }

func init() {
	Suite(&TestDriver{TestSuite: test.NewTestSuite("mem")})
}
