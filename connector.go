package qg

import (
	"database/sql/driver"
	"fmt"
	"net/url"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

func GetConnector(host string, port int, username string, password string, database string) (driver.Connector, error) {
	u := &url.URL{
		Scheme: "postgres",
		Host:   fmt.Sprintf("%s:%d", host, port),
		User:   url.UserPassword(username, password),
		Path:   database,
	}

	return GetConnectorFromURL(u)
}

func GetConnectorFromURL(u *url.URL) (driver.Connector, error) {
	return GetConnectorFromConnStr(u.String())
}

func GetConnectorFromConnStr(connStr string) (driver.Connector, error) {
	cfg, err := pgx.ParseConfig(connStr)

	if err != nil {
		return nil, err
	}

	connector := stdlib.GetConnector(*cfg, stdlib.OptionAfterConnect(PrepareStatements))

	return connector, nil
}
