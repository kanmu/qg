package qg

import (
	"database/sql"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/stdlib"
	sqltracer "gopkg.in/DataDog/dd-trace-go.v1/contrib/database/sql"
)

// intPow returns x**y, the base-x exponential of y.
func intPow(x, y int) (r int) {
	if x == r || y < r {
		return
	}
	r = 1
	if x == r {
		return
	}
	if x < 0 {
		x = -x
		if y&1 == 1 {
			r = -1
		}
	}
	for y > 0 {
		if y&1 == 1 {
			r *= x
		}
		x *= x
		y >>= 1
	}
	return
}

// Get *pgx.Conn from *sql.Conn and pass it to function.
func rawConn(conn *sql.Conn, f func(*pgx.Conn) error) error {
	err := conn.Raw(func(driverConn any) error {
		var stdlibConn *stdlib.Conn

		if tracedConn, ok := driverConn.(*sqltracer.TracedConn); ok {
			stdlibConn = tracedConn.WrappedConn().(*stdlib.Conn)
		} else {
			stdlibConn = driverConn.(*stdlib.Conn)
		}

		pgxConn := stdlibConn.Conn()
		return f(pgxConn)
	})

	return err
}
