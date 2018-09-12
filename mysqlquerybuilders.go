package kazsql

import (
	"errors"
	"fmt"
	"reflect"
	"time"
)

func buildSELECTClause(tableName string, selector interface{}, result interface{}) (string, error) {

	var query string
	var objResult reflect.Value

	if reflect.ValueOf(selector).Kind() != reflect.Ptr {
		//return "", errors.New("Parameter selector is not a pointer")
		return "", fmt.Errorf("The value for %v must be a poiner. Received %t instead", selector, selector)
	}

	refResult := reflect.ValueOf(result)
	if refResult.Kind() != reflect.Ptr && refResult.Kind() != reflect.Slice {
		//return "", errors.New("Parameter result is neither a pointer or a slice")
		return "", fmt.Errorf("The value for %v must be a poiner. Received %t instead", result, result)
	}

	query = "SELECT "

	switch refResult.Kind() {
	case reflect.Slice:
		typ := reflect.TypeOf(result).Elem()
		objResult = reflect.Indirect(reflect.New(typ))
	case reflect.Ptr:
		objResult = reflect.Indirect(reflect.ValueOf(result))

		if reflect.TypeOf(objResult.Interface()).Kind() == reflect.Slice {
			typ := reflect.TypeOf(objResult.Interface()).Elem()
			objResult = reflect.Indirect(reflect.New(typ))
		}
	default:
		return "", fmt.Errorf("Unsupported Type %T", refResult.Type())
	}
	for i := 0; i < objResult.NumField(); i++ {
		switch objResult.Field(i).Kind() {
		case reflect.String:
			query += "IFNULL(" + objResult.Type().Field(i).Name + `, '') AS ` + objResult.Type().Field(i).Name
		default:
			query += objResult.Type().Field(i).Name
		}
		if i+1 < objResult.NumField() {
			query += `, `
		}
	}

	query += " FROM " + tableName

	addWhere := false
	whereClause := ""
	objSelect := reflect.Indirect(reflect.ValueOf(selector))

	for i := 0; i < objSelect.NumField(); i++ {
		if !includeValue(objSelect.Field(i)) {
			continue
		}

		addWhere = true
		name := objSelect.Type().Field(i).Name
		whereClause += name + " = ? AND "
	}

	if addWhere {
		whereClause = whereClause[:len(whereClause)-5]
		query += " WHERE " + whereClause
	}

	return query, nil
}

func buildINSERTClause(tableName string, values interface{}) (string, error) {
	var query string

	if reflect.ValueOf(values).Kind() != reflect.Ptr {
		return "", errors.New("Parameter values is not a pointer")
	}

	objValues := reflect.Indirect(reflect.ValueOf(values))
	hasValues := false
	valuesClause := ""
	valuesList := ""

	for i := 0; i < objValues.NumField(); i++ {
		if !includeValue(objValues.Field(i)) {
			continue
		}

		hasValues = true
		valuesClause += objValues.Type().Field(i).Name + `, `
		valuesList += " ?, "
	}

	if hasValues {
		valuesClause = valuesClause[:len(valuesClause)-2]
		valuesList = valuesList[:len(valuesList)-2]
	}
	query = "INSERT INTO " + tableName + `(` + valuesClause + `)` + " VALUES (" + valuesList + ")"

	return query, nil
}

func buildUPDATEClause(tableName string, selector interface{}, values interface{}) (string, error) {
	var query string

	query = "UPDATE " + tableName + " SET "

	if reflect.ValueOf(selector).Kind() != reflect.Ptr {
		//return "", errors.New("Parameter selector is not a pointer")
		return "", fmt.Errorf("The value for %v must be a poiner. Received %T instead", selector, selector)
	}

	if reflect.ValueOf(values).Kind() != reflect.Ptr {
		//return "", errors.New("Parameter values is not a pointer")
		return "", fmt.Errorf("The value for %v must be a poiner. Received %T instead", values, values)
	}

	//---- SET -----------
	objValues := reflect.Indirect(reflect.ValueOf(values))
	hasValues := false
	setClause := ""

	for i := 0; i < objValues.NumField(); i++ {
		if !includeValue(objValues.Field(i)) {
			continue
		}

		hasValues = true
		setClause += objValues.Type().Field(i).Name + ` = ?, `
	}

	if hasValues {
		setClause = setClause[:len(setClause)-2]
	}

	//---- WHERE -----------
	addWhere := false
	whereClause := ""
	objSelect := reflect.Indirect(reflect.ValueOf(selector))

	for i := 0; i < objSelect.NumField(); i++ {
		if !includeValue(objSelect.Field(i)) {
			continue
		}
		addWhere = true
		name := objSelect.Type().Field(i).Name
		whereClause += name + " = ? AND "
	}

	//---- CONCAT -----------
	query += setClause

	if addWhere {
		whereClause = whereClause[:len(whereClause)-5]
		query += " WHERE " + whereClause
	}

	return query, nil
}

func buildDELETEClause(tableName string, selector interface{}) (string, error) {
	var query string

	query = "DELETE FROM " + tableName

	if reflect.ValueOf(selector).Kind() != reflect.Ptr {
		return "", errors.New("Parameter selector is not a pointer")
	}

	//---- WHERE -----------
	addWhere := false
	whereClause := ""
	objSelect := reflect.Indirect(reflect.ValueOf(selector))

	for i := 0; i < objSelect.NumField(); i++ {
		if !includeValue(objSelect.Field(i)) {
			continue
		}
		addWhere = true
		name := objSelect.Type().Field(i).Name
		whereClause += name + " = ? AND "
	}

	if addWhere {
		whereClause = whereClause[:len(whereClause)-5]
		query += " WHERE " + whereClause
	}

	return query, nil
}

func includeValue(value reflect.Value) bool {
	switch value.Kind() {
	case reflect.String:
		return !(value.String() == "")
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return !(value.Int() == 0)
	case reflect.Float32, reflect.Float64:
		return !(value.Float() == 0.0)
	case reflect.Bool:
		return true
	case reflect.Struct:
		switch value.Interface().(type) {
		case time.Time:
			return !(value.Interface().(time.Time) == time.Time{})
		}
		return true
	default:
		return !value.IsNil()
	}
}
