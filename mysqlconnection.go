package kazsql

import "github.com/farkaz00/kazconfig"

//MySQLConnection encapsulates a MySQLConnection
type MySQLConnection struct {
	address  string
	user     string
	password string
	dbname   string
	protocol string
	params   []string
}

//GetConnString returns a MySQL formatted connection string
func (conn MySQLConnection) GetConnString() string {
	var strConn string
	strConn = conn.user + `:` + conn.password + `@` + conn.protocol + `(` + conn.address + `)` + `/` + conn.dbname + `?parseTime=true`
	return strConn
}

//Close closes the MySQL connection
func (conn MySQLConnection) Close() {

}

//NewMySQLConnection returns a MySQLConnection object
func NewMySQLConnection(s *kazconfig.Settings) *MySQLConnection {
	return &MySQLConnection{
		address:  s.Get("dbhost"),
		user:     s.Get("dbuser"),
		password: s.Get("dbpwd"),
		dbname:   s.Get("dbname"),
		protocol: s.Get("dbprotocol"),
	}
}
