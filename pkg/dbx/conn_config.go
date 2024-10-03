package dbx

// ConnConfig represents the configuration required for database connection.
type ConnConfig struct {
	VpcDirectConnection bool
	Host                string
	Port                int32
	DBName              string
	User                string
	Password            string
	MaxConn             int32
	IsLocalEnv          bool
}
