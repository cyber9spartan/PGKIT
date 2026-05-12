package qb

import "fmt"

// MaxQueryParams is the PostgreSQL limit on bound parameters per query.
const MaxQueryParams = 65535

var (
	ErrNoTable   = fmt.Errorf("qb: table name must not be empty")
	ErrEmptyData = fmt.Errorf("qb: data map must not be empty")
	ErrEmptyRows = fmt.Errorf("qb: batch rows must not be empty")
)

func tooManyParamsErr(got int) error {
	return fmt.Errorf("qb: query has %d parameters, exceeding PostgreSQL limit of %d", got, MaxQueryParams)
}
