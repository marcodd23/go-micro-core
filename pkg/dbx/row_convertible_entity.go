package dbx

// RowConvertibleEntity defines an interface for converting a struct into a row of values for database insertion.
//
// This interface is intended for structs that need to be bulk-inserted into a database. Each struct implementing this interface
// must provide the `ToRow()` method, which converts the struct into a slice of values (`[]interface{}`) that can be used
// for bulk operations like `pgx.CopyFrom`.
//
// The values returned by `ToRow()` should correspond to the struct's fields in the order they map to the database columns.
// Struct fields marked with a `db` tag (or other relevant tags) should be included in the returned slice, while fields with
// `db:"-"` or without a tag should be omitted.
//
// Example:
//
//	type MyStruct struct {
//	    ID    int    `db:"id"`
//	    Name  string `db:"name"`
//	    Age   int    `db:"age"`
//	}
//
//	func (m MyStruct) ToRow() []interface{} {
//	    return []interface{}{m.ID, m.Name, m.Age}
//	}
//
// Usage of this interface allows for efficient bulk inserts by converting structs into row data that aligns with
// PostgreSQL's `pgx.CopyFrom` or other similar methods for inserting rows in bulk.
type RowConvertibleEntity interface {
	ToRow() []interface{} // Converts the struct to a row of values.
}
