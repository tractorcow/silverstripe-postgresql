<?php
/**
 * This is a helper class for the SS installer.
 * 
 * It does all the specific checking for PostgreSQLDatabase
 * to ensure that the configuration is setup correctly.
 * 
 * @package postgresql
 */
class PostgreSQLDatabaseConfigurationHelper extends DatabaseConfigurationHelper {

	public function makeConnection($databaseConfig) {
		switch($databaseConfig['type']) {
			case 'PostgrePDODatabase':
				$connector = new PDOConnector();
				break;
			case 'PostgreSQLDatabase':
				$connector = new PostgreSQLConnector();
				break;
			default:
				return null;
		}
		// Inject some default parameters
		if(empty($parameters['database'])) {
			$parameters['database'] = PostgreSQLDatabase::MASTER_DATABASE;
		}
        if(empty($parameters['schema'])) {
            $parameters['schema'] = PostgreSQLDatabase::MASTER_SCHEMA;
        }
        if(empty($parameters['port'])) {
            $parameters['port'] = 5432;
        }
        if(empty($parameters['driver'])) {
			$databaseConfig['driver'] = 'postgresql';
		}
		@$connector->connect($databaseConfig);
		return $connector;
	}

	/**
	 * Ensure that the database function pg_connect
	 * is available. If it is, we assume the PHP module for this
	 * database has been setup correctly.
	 * 
	 * @param array $databaseConfig Associative array of database configuration, e.g. "server", "username" etc
	 * @return boolean
	 */
	public function requireDatabaseFunctions($databaseConfig) {
		switch($databaseConfig['type']) {
			case 'PostgreSQLDatabase':
				return function_exists('pg_connect');
			case 'PostgrePDODatabase':
				return class_exists('PDO') && in_array('postgresql', PDO::getAvailableDrivers());
			default:
				return false;
		}
	}

	/**
	 * Ensure a database connection is possible using credentials provided.
	 * @param array $databaseConfig Associative array of db configuration, e.g. "server", "username" etc
	 * @return array Result - e.g. array('success' => true, 'error' => 'details of error')
	 */
	public function requireDatabaseConnection($databaseConfig) {
		$databaseConfig['database'] = 'postgres';
		return parent::requireDatabaseConnection($databaseConfig);
	}

	/**
	 * Ensure that the PostgreSQL version is at least 8.3.
	 * @param array $databaseConfig Associative array of db configuration, e.g. "server", "username" etc
	 * @return array Result - e.g. array('success' => true, 'error' => 'details of error')
	 */
	public function requireDatabaseVersion($databaseConfig) {
		$success = false;
		$error = '';
		$version = $this->getDatabaseVersion($databaseConfig);

		if($version) {
			$success = version_compare($version, '8.3', '>=');
			if(!$success) {
				$error = "Your PostgreSQL version is $version. It's recommended you use at least 8.3.";
			}
		} else {
			$error = "Your PostgreSQL version could not be determined.";
		}

		return array(
			'success' => $success,
			'error' => $error
		);
	}

	/**
	 * Ensure that the database connection is able to use an existing database,
	 * or be able to create one if it doesn't exist.
	 * 
	 * @param array $databaseConfig Associative array of db configuration, e.g. "server", "username" etc
	 * @return array Result - e.g. array('success' => true, 'alreadyExists' => 'true')
	 */
	public function requireDatabaseOrCreatePermissions($databaseConfig) {
		$success = false;
		$alreadyExists = false;
		$check = $this->requireDatabaseConnection($databaseConfig);
		$conn = $check['connection'];
		
		$result = pg_query($conn, "SELECT datname FROM pg_database WHERE datname = '$databaseConfig[database]'");
		if(pg_fetch_array($result)) {
			$success = true;
			$alreadyExists = true;
		} else {
			if(@pg_query($conn, "CREATE DATABASE testing123")) {
				pg_query($conn, "DROP DATABASE testing123");
				$success = true;
				$alreadyExists = false;
			}
		}
		
		return array(
			'success' => $success,
			'alreadyExists' => $alreadyExists
		);
	}

	public function requireDatabaseAlterPermissions($databaseConfig) {
		
	}

}
