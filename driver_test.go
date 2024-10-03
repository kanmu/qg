package qg_test

import (
	"context"
	"database/sql/driver"

	"github.com/jackc/pgx/v4/stdlib"
	"github.com/kanmu/qg/v4"
)

var _ driver.Connector = &testConnector{}

type testConnector struct {
	driver.Connector
	Called *int
}

var _ qg.ConnWrapper = &testConnWrapper{}

type testConnWrapper struct {
	*stdlib.Conn
	*testConnector
}

func (cw *testConnWrapper) WrappedConn() driver.Conn {
	*cw.Called++
	return cw.Conn
}

func NewTestConnector(connector driver.Connector, counter *int) *testConnector {
	return &testConnector{connector, counter}
}

func (c *testConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.Connector.Connect(ctx)

	if err != nil {
		return nil, err
	}

	return &testConnWrapper{conn.(*stdlib.Conn), c}, nil
}