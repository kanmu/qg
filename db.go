package qg

import (
	"database/sql"
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

func GetConnectorFromConnStr(connStr string, opts ...stdlib.OptionOpenDB) (driver.Connector, error) {
	cfg, err := pgx.ParseConfig(connStr)

	if err != nil {
		return nil, err
	}

	opts = append(opts, stdlib.OptionAfterConnect(PrepareStatements))
	connector := stdlib.GetConnector(*cfg, opts...)

	return connector, nil
}

// driver.Conn Wrapper
type ConnWrapper interface {
	WrappedConn() driver.Conn
}

// Get *pgx.Conn from *sql.Conn and pass it to function.
func rawConn(conn *sql.Conn, f func(*pgx.Conn) error) error {
	err := conn.Raw(func(driverConn any) error {
		var stdlibConn *stdlib.Conn

		if tracedConn, ok := driverConn.(ConnWrapper); ok {
			stdlibConn = tracedConn.WrappedConn().(*stdlib.Conn)
		} else {
			stdlibConn = driverConn.(*stdlib.Conn)
		}

		pgxConn := stdlibConn.Conn()
		return f(pgxConn)
	})

	return err
}
