package configmgr

import (
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/spf13/viper"
)

const defaultConfigBaseName = "property"

func LoadConfigForEnv(config Config) error {
	return ReadConfiguration(getEnvPropertyFileName(defaultConfigBaseName), config)
}

// LoadConfigFromPathForEnv - search the property-<ENV> properties in the given search path (for ex. "./config" )
func LoadConfigFromPathForEnv(searchPath string, config Config) error {
	if searchPath == "" {
		return LoadConfigForEnv(config)
	}

	strings.TrimSuffix(searchPath, "/")
	return ReadConfiguration(getEnvPropertyFileName(fmt.Sprintf("%s/%s", searchPath, defaultConfigBaseName)), config)
}

// ReadConfiguration reads the configuration from the file and environment variables
func ReadConfiguration(configFilePath string, config Config) error {
	log.Println("config filepath: ", configFilePath)

	viper.SetConfigFile(configFilePath) // Specify the file to read
	viper.SetConfigType("yaml")         // Specify the config file type (yaml)
	viper.AutomaticEnv()                // Enable automatic environment variable binding

	// Replace dots in keys with underscores in environment variables
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_", "-", "_"))

	// Attempt to read the configuration file
	if err := viper.ReadInConfig(); err == nil {
		log.Printf("Reading configuration from config file: %s\nSet environment variables will OVERRIDE these values, as the environment takes precedent.", configFilePath)
		log.Printf("Please do not mix a configuration file and environment variables! Your variables may not get read correctly.")
	} else {
		log.Println("No configuration file found, reading configuration from environment variables.")
	}

	// Unmarshal the configuration into the provided struct
	if err := viper.Unmarshal(config); err != nil {
		return fmt.Errorf("unable to decode into config struct, %v", err)
	}

	return nil
}

func getEnvPropertyFileName(baseFileName string) string {
	env := strings.ToUpper(os.Getenv("ENVIRONMENT"))
	if !checkIfLocalEnv(env) {
		return fmt.Sprintf("%s-%s.yaml", baseFileName, strings.ToLower(env))
	}

	return fmt.Sprintf("%s.yaml", baseFileName)
}

func checkIfLocalEnv(env string) bool {
	if env == strings.ToUpper("DEV") || env == strings.ToUpper("STAGE") || env == strings.ToUpper("PROD") {
		return false
	}

	return true
}
