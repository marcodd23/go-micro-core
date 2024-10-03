package dbx

// PreparedStatement represents a prepared statement query.
//
// A PreparedStatement encapsulates a SQL query that can be prepared and executed multiple times with different arguments.
// Are typically used to improve performance by reducing the overhead of parsing and planning the SQL query
// each time it is executed.
//
// Fields:
//   - Name: A unique name identifying the prepared statement. This name is used to reference the statement when executing it.
//   - Query: The SQL query string associated with the prepared statement. The query can include placeholders for arguments.
type PreparedStatement struct {
	Name  string
	Query string
}

// PreparedStatementsMap represents a map of DbShard and their respective prepared statements.
//
// This struct is used to associate multiple prepared statements with different database shards. It allows for the organization
// and management of prepared statements across various database shards, which is particularly useful in sharded database architectures.
//
// Fields:
//   - DbPrepStmMap: A map where the key is a DbShard and the value is a slice of PreparedStatements associated with that shard.
type PreparedStatementsMap struct {
	DbPrepStmMap map[DbShard][]PreparedStatement
}

// NewPreparedStatement creates a new prepared statement.
//
// This function is a constructor for the PreparedStatement struct. It takes a name and a query string as arguments and returns
// a new PreparedStatement instance. This is useful for creating and registering prepared statements before they are executed.
//
// Arguments:
//   - name: The name of the prepared statement.
//   - query: The SQL query string for the prepared statement.
//
// Returns:
//   - PreparedStatement: A new instance of the PreparedStatement struct.
func NewPreparedStatement(name, query string) PreparedStatement {
	return PreparedStatement{Name: name, Query: query}
}

// GetName returns the name of the prepared statement.
//
// This method returns the unique name associated with the prepared statement. The name is typically used to reference
// the prepared statement when executing it in the context of a database operation.
//
// Returns:
//   - string: The name of the prepared statement.
func (p PreparedStatement) GetName() string {
	return p.Name
}

// GetQuery returns the query of the prepared statement.
//
// This method returns the SQL query string associated with the prepared statement. The query can include placeholders
// for arguments that will be provided at execution time.
//
// Returns:
//   - string: The SQL query string of the prepared statement.
func (p PreparedStatement) GetQuery() string {
	return p.Query
}
