package kazsql

import (
	"database/sql"
	"fmt"
	"reflect"

	_ "github.com/go-sql-driver/mysql" //Justifying underscore
)

//MySQLClient generic sql client based on the database/sql package and the Go-MySql-Driver
type MySQLClient struct {
	dbConn    *MySQLConnection
	dbHandler *sql.DB
	tableName string
}

//SelectOne performs a SELECT query based on the selector and stores the results in the result variable
func (c MySQLClient) SelectOne(collection string, selector interface{}, result interface{}) error {
	var err error
	var db *sql.DB

	if reflect.ValueOf(selector).Kind() != reflect.Ptr {
		return fmt.Errorf("The value for %v must be a poiner. Received %t instead", selector, selector)
	}

	if reflect.ValueOf(result).Kind() != reflect.Ptr {
		return fmt.Errorf("The value for %v must be a poiner. Received %t instead", result, result)
	}

	if db, err = sql.Open("mysql", c.dbConn.GetConnString()); err != nil {
		return err
	}
	defer db.Close()

	if err = db.Ping(); err != nil {
		return err
	}

	query, err := buildSELECTClause(collection, selector, result)
	if err != nil {
		return err
	}

	row, err := c.getRow(db.QueryRow, query, selector)
	if err != nil {
		return err
	}

	obj := reflect.Indirect(reflect.ValueOf(result))
	params := []reflect.Value{}

	for i := 0; i < obj.NumField(); i++ {
		params = append(params, obj.Field(i).Addr())
	}

	method := reflect.ValueOf(row.Scan)
	method.Call(params)

	return err
}

//Returns a single row from the SELECT query
func (c MySQLClient) getRow(method func(query string, args ...interface{}) *sql.Row,
	query string, selector interface{}) (*sql.Row, error) {

	row := new(sql.Row)
	var err error

	meth := reflect.ValueOf(method)

	params := []reflect.Value{}
	params = append(params, reflect.ValueOf(query))

	objSelect := reflect.Indirect(reflect.ValueOf(selector))

	for i := 0; i < objSelect.NumField(); i++ {
		if objSelect.Field(i).Kind() == reflect.String && objSelect.Field(i).String() == "" {
			continue
		}
		if objSelect.Field(i).Kind() != reflect.String && objSelect.Field(i).IsNil() {
			continue
		}
		params = append(params, objSelect.Field(i).Addr())
	}

	res := meth.Call(params)

	//*sql.Row
	if !res[0].IsNil() {
		*row = res[0].Elem().Interface().(sql.Row)
	}

	return row, err
}

//Select performs a SELECT query based on the selector and stores the results in the result variable
func (c MySQLClient) Select(collection string, selector interface{}, result interface{}) error {
	var err error
	var db *sql.DB

	if reflect.ValueOf(result).Kind() != reflect.Ptr && reflect.Indirect(reflect.ValueOf(result)).Kind() != reflect.Slice {
		return fmt.Errorf("The value for result must be a Pointer to a Slice. Received %t instead.", result)
	}

	if db, err = sql.Open("mysql", c.dbConn.GetConnString()); err != nil {
		return err
	}
	defer db.Close()

	if err = db.Ping(); err != nil {
		return err
	}

	query, err := buildSELECTClause(collection, selector, result)
	if err != nil {
		return err
	}

	rows, err := c.getRows(db.Query, query, selector)
	if err != nil {
		return err
	}

	typ := reflect.TypeOf(reflect.Indirect(reflect.ValueOf(result)).Interface()).Elem()
	objResult := reflect.Indirect(reflect.New(typ))

	params := []reflect.Value{}

	for i := 0; i < objResult.NumField(); i++ {
		params = append(params, objResult.Field(i).Addr())
	}

	method := reflect.ValueOf(rows.Scan)
	refResult := reflect.Indirect(reflect.ValueOf(result))
	temp := reflect.Indirect(reflect.ValueOf(result))

	for rows.Next() {
		res := method.Call(params)
		//error
		if !res[0].IsNil() {
			err = res[0].Elem().Interface().(error)
		}
		if err != nil {
			return err
		}

		obj := reflect.Indirect(reflect.New(typ))
		for i, v := range params {
			obj.Field(i).Set(reflect.Indirect(v))
		}

		temp = reflect.Append(temp, obj)
	}

	refResult.Set(temp)

	return err
}

//Returns multiple rows from the SELECT query
func (c MySQLClient) getRows(method func(query string, args ...interface{}) (*sql.Rows, error),
	query string, selector interface{}) (*sql.Rows, error) {

	rows := new(sql.Rows)
	var err error

	meth := reflect.ValueOf(method)

	params := []reflect.Value{}
	params = append(params, reflect.ValueOf(query))

	objSelect := reflect.Indirect(reflect.ValueOf(selector))

	for i := 0; i < objSelect.NumField(); i++ {
		if !includeValue(objSelect.Field(i)) {
			continue
		}
		params = append(params, objSelect.Field(i).Addr())
	}

	res := meth.Call(params)

	//*sql.Row
	if !res[0].IsNil() {
		*rows = res[0].Elem().Interface().(sql.Rows)
	}

	//error
	if !res[1].IsNil() {
		err = res[1].Elem().Interface().(error)
	}

	return rows, err
}

//Insert inserts the values passed as parameter into the tableName
func (c MySQLClient) Insert(tableName string, values interface{}) error {

	var db *sql.DB
	var err error

	if reflect.ValueOf(values).Kind() != reflect.Ptr {
		return fmt.Errorf("The value for values parameter must be a poiner. Received %t instead", values)
	}

	if db, err = sql.Open("mysql", c.dbConn.GetConnString()); err != nil {
		return err
	}
	defer db.Close()

	if err = db.Ping(); err != nil {
		return err
	}

	query, err := buildINSERTClause(tableName, values)
	if err != nil {
		return err
	}

	_, err = runInsert(db.Exec, query, values)

	return err
}

func runInsert(method func(query string, args ...interface{}) (sql.Result, error),
	query string, values interface{}) (sql.Result, error) {

	var sqlResult sql.Result
	var err error

	if reflect.ValueOf(values).Kind() != reflect.Ptr {
		return nil, fmt.Errorf("The value for values parameter must be a poiner. Received %t instead", values)
	}

	meth := reflect.ValueOf(method)
	objValues := reflect.Indirect(reflect.ValueOf(values))
	params := []reflect.Value{}

	params = append(params, reflect.ValueOf(query))

	for i := 0; i < objValues.NumField(); i++ {
		if !includeValue(objValues.Field(i)) {
			continue
		}

		params = append(params, objValues.Field(i))
	}

	res := meth.Call(params)

	//*sql.Result
	if !res[0].IsNil() {
		sqlResult = res[0].Elem().Interface().(sql.Result)
	}

	//error
	if !res[1].IsNil() {
		err = res[1].Elem().Interface().(error)
	}

	return sqlResult, err
}

//Update updates tableName based on selector with values
func (c MySQLClient) Update(tableName string, selector interface{}, values interface{}) error {

	var db *sql.DB
	var err error

	if reflect.ValueOf(selector).Kind() != reflect.Ptr {
		return fmt.Errorf("The value for selector parameter must be a poiner. Received %t instead", selector)
	}

	if reflect.ValueOf(values).Kind() != reflect.Ptr {
		return fmt.Errorf("The value for values parameter must be a poiner. Received %t instead", values)
	}

	if db, err = sql.Open("mysql", c.dbConn.GetConnString()); err != nil {
		return err
	}
	defer db.Close()

	if err = db.Ping(); err != nil {
		return err
	}

	query, err := buildUPDATEClause(tableName, selector, values)
	if err != nil {
		return err
	}

	_, err = runUpdate(db.Exec, query, selector, values)

	return err
}

func runUpdate(method func(query string, args ...interface{}) (sql.Result, error),
	query string, selector interface{}, values interface{}) (sql.Result, error) {

	var sqlResult sql.Result
	var err error

	if reflect.ValueOf(selector).Kind() != reflect.Ptr {
		return nil, fmt.Errorf("The value for selector parameter must be a poiner. Received %t instead", selector)
	}

	if reflect.ValueOf(values).Kind() != reflect.Ptr {
		return nil, fmt.Errorf("The value for values parameter must be a poiner. Received %t instead", values)
	}

	meth := reflect.ValueOf(method)
	objSelector := reflect.Indirect(reflect.ValueOf(selector))
	objValues := reflect.Indirect(reflect.ValueOf(values))
	params := []reflect.Value{}

	params = append(params, reflect.ValueOf(query))

	for i := 0; i < objValues.NumField(); i++ {
		if !includeValue(objValues.Field(i)) {
			continue
		}

		params = append(params, objValues.Field(i))
	}

	for i := 0; i < objSelector.NumField(); i++ {
		if !includeValue(objSelector.Field(i)) {
			continue
		}

		params = append(params, objSelector.Field(i))
	}

	res := meth.Call(params)

	//*sql.Result
	if !res[0].IsNil() {
		sqlResult = res[0].Elem().Interface().(sql.Result)
	}

	//error
	if !res[1].IsNil() {
		err = res[1].Elem().Interface().(error)
	}

	return sqlResult, err
}

//Delete deletes records from tableName based on the selector
func (c MySQLClient) Delete(tableName string, selector interface{}) error {
	var db *sql.DB
	var err error

	if reflect.ValueOf(selector).Kind() != reflect.Ptr {
		return fmt.Errorf("The value for selector parameter must be a poiner. Received %t instead", selector)
	}

	if db, err = sql.Open("mysql", c.dbConn.GetConnString()); err != nil {
		return err
	}
	defer db.Close()

	if err = db.Ping(); err != nil {
		return err
	}

	query, err := buildDELETEClause(tableName, selector)
	if err != nil {
		return err
	}

	_, err = runDelete(db.Exec, query, selector)
	fmt.Println(query)

	return err
}

func runDelete(method func(query string, args ...interface{}) (sql.Result, error),
	query string, selector interface{}) (sql.Result, error) {

	var sqlResult sql.Result
	var err error

	if reflect.ValueOf(selector).Kind() != reflect.Ptr {
		return nil, fmt.Errorf("The value for selector parameter must be a poiner. Received %t instead", selector)
	}

	meth := reflect.ValueOf(method)
	objSelector := reflect.Indirect(reflect.ValueOf(selector))
	params := []reflect.Value{}

	params = append(params, reflect.ValueOf(query))

	for i := 0; i < objSelector.NumField(); i++ {
		if !includeValue(objSelector.Field(i)) {
			continue
		}

		params = append(params, objSelector.Field(i))
	}

	res := meth.Call(params)

	//*sql.Result
	if !res[0].IsNil() {
		sqlResult = res[0].Elem().Interface().(sql.Result)
	}

	//error
	if !res[1].IsNil() {
		err = res[1].Elem().Interface().(error)
	}

	return sqlResult, err
}

//Close closes the underlying database handler
func (c MySQLClient) Close() {

}

//NewMySQLClient returns an SQLClient object based on the connection info
func NewMySQLClient(connection *MySQLConnection) (*MySQLClient, error) {
	var dbHandler *sql.DB
	var err error

	fmt.Println(connection.GetConnString())
	if dbHandler, err = sql.Open("mysql", connection.GetConnString()); err != nil {
		return nil, err
	}
	if err = dbHandler.Ping(); err != nil {
		return nil, err
	}
	dbHandler.Close()

	client := new(MySQLClient)
	client.dbConn = &MySQLConnection{}
	*client.dbConn = *connection

	return client, nil
}
