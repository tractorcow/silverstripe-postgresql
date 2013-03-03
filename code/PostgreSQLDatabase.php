<?php

/**
 * PostgreSQL connector class.
 * 
 * @package sapphire
 * @subpackage model
 */
class PostgreSQLDatabase extends SS_Database {
    
	/**
	 * Database schema manager object
	 * 
	 * @var PostgreSQLSchemaManager
	 */
    protected $schemaManager;

	/**
	 * The currently selected database schema name.
     * 
	 * @var string
	 */
	protected $schema;

	protected $supportsTransactions = true;
    
    /**
     * Full text cluster method. (e.g. GIN or GiST)
     * 
     * @return string
     */
	public static function default_fts_cluster_method() {
        return Config::inst()->get('PostgreSQLDatabase', 'default_fts_cluster_method');
    }
    
    /**
     * Full text search method.
     * 
     * @return string
     */
	public static function default_fts_search_method() {
        return Config::inst()->get('PostgreSQLDatabase', 'default_fts_search_method');
    }

    /**
	 * Determines whether to check a database exists on the host by
     * querying the 'postgres' database and running createDatabase.
     *
     * Some locked down systems prevent access to the 'postgres' table in
     * which case you need to set this to false.
     *
     * If allow_query_master_postgres is false, and model_schema_as_database is also false, 
     * then attempts to create or check databases beyond the initial connection will
     * result in a runtime error.
     */
	public static function allow_query_master_postgres() {
        return Config::inst()->get('PostgreSQLDatabase', 'allow_query_master_postgres');
    }
    
    /**
     * For instances where multiple databases are used beyond the initial connection
     * you may set this option to true to force database switches to switch schemas
     * instead of using databases. This may be useful if the database user does not
     * have cross-database permissions, and in cases where multiple databases are used
     * (such as in running test cases).
     * 
     * If this is true then the database will only be set during the initial connection,
     * and attempts to change to this database will use the 'public' schema instead
     */
    public function model_schema_as_database() {
        return Config::inst()->get('PostgreSQLDatabase', 'model_schema_as_database');
    }

	/**
	 * Override the language that tsearch uses.  By default it is 'english, but
	 * could be any of the supported languages that can be found in the
	 * pg_catalog.pg_ts_config table.
	 *
	 * @var string
	 */
	public static function search_language() {
        return Config::inst()->get('PostgreSQLDatabase', 'search_language');
    }
    
    /**
     * The database name specified at initial connection
     * 
     * @var string
     */
    protected $databaseOriginal = '';
    
    /**
     * The schema name specified at initial construction. When model_schema_as_database
     * is set to true selecting the $databaseOriginal database will instead reset
     * the schema to this
     *
     * @var string
     */
    protected $schemaOriginal = '';
    
    /**
     * Connection parameters specified at inital connection
     * 
     * @var array 
     */
    protected $parameters = array();
    
    function connect($parameters) {
        
        // Check database name
        if(empty($parameters['database'])) {
            // Check if we can use the master database
            if(!self::allow_query_master_postgres()) {
                throw new ErrorException('PostegreSQLDatabase::connect called without a database name specified');
            }
            // Fallback to master database connection if permission allows
            $parameters['database'] = 'postgres';
        }
        $this->databaseOriginal = $parameters['database'];
        
        // check schema name
        if(empty($parameters['schema'])) {
            $parameters['schema'] = 'public';
        }
        $this->schemaOriginal = $parameters['schema'];
        
        // Ensure that driver is available (required by PDO)
		if(empty($parameters['driver'])) {
			$parameters['driver'] = $this->getDatabaseServer();
		}
        
        // Ensure port number is set (required by postgres)
        if(empty($parameters['port'])) {
            $parameters['port'] = 5432;
        }
        
        $this->parameters = $parameters;
        
        // If allowed, check that the database exists. Otherwise naively assume
        // that the original database exists
        if(self::allow_query_master_postgres()) {
            // Use master connection to setup initial schema
            $this->connectMaster();
            if(!$this->schemaManager->databaseExists($this->databaseOriginal)) {
                $this->schemaManager->createDatabase($this->databaseOriginal);
            }
        }

		// Connect to the actual database we're requesting
		$this->connectDefault();

		// Set up the schema if required
 		if(!$this->schemaManager->schemaExists($this->schemaOriginal)) {
 			 $this->schemaManager->createSchema($this->schemaOriginal);
        }
 		$this->setSchema($this->schemaOriginal);

		// Set the timezone if required.
		if (isset($parameters['timezone'])) {
			$this->selectTimezone($parameters['timezone']);
		}
    }
    
	/**
	 * Sets the system timezone for the database connection
	 * 
	 * @param string $timezone
	 */
	public function selectTimezone($timezone) {
		if (empty($timezone)) return;
        $this->preparedQuery("SET SESSION TIME ZONE ?;", array($timezone));
    }

	public function supportsCollations() {
		return true;
	}

	public function supportsTimezoneOverride() {
		return true;
	}

	public function getDatabaseServer() {
		return "postgresql";
	}

	/**
	 * Returns the name of the current schema in use
     * 
     * @return string Name of current schema
	 */
	public function currentSchema() {
        if($this->schema) return $this->schema;
		return $this->query('SELECT current_schema()')->value();
	}

 	/**
 	 * Utility method to manually set the schema to an alternative
 	 * Check existance & sets search path to the supplied schema name
     * 
 	 * @param string $schema
 	 */
 	public function setSchema($schema) {
 		if(!$this->schemaManager->schemaExists($schema)) {
            user_error("Schema $schema does not exist", E_USER_ERROR);
        }
 		$this->setSchemaSearchPath($schema);
 	 	$this->schema = $schema;
 	}

 	/**
 	 * Override the schema search path. Search using the arguments supplied.
 	 * NOTE: The search path is normally set through setSchema() and only
 	 * one schema is selected. The facility to add more than one schema to
 	 * the search path is provided as an advanced PostgreSQL feature for raw
 	 * SQL queries. Sapphire cannot search for datamodel tables in alternate
 	 * schemas, so be wary of using alternate schemas within the ORM environment.
     * 
 	 * @param string $arg1 First schema to use
 	 * @param string $arg2 Second schema to use
 	 * @param string $argN Nth schema to use
 	 */
 	public function setSchemaSearchPath() {
 		if(func_num_args() == 0) {
 			user_error('At least one Schema must be supplied to set a search path.', E_USER_ERROR);
        }
	 	$schemas = array_values(func_get_args());
	 	$this->query("SET search_path TO \"" . implode("\",\"", $schemas) . "\"");
	}
	
	/**
	 * The core search engine configuration.
	 * @todo Properly extract the search functions out of the core.
	 *
	 * @param string $keywords Keywords as a space separated string
	 * @return object DataObjectSet of result pages
	 */
	public function searchEngine($classesToSearch, $keywords, $start, $pageLength, $sortBy = "ts_rank DESC", $extraFilter = "", $booleanSearch = false, $alternativeFileFilter = "", $invertedMatch = false) {
		//Fix the keywords to be ts_query compatitble:
		//Spaces must have pipes
		//@TODO: properly handle boolean operators here.
		$keywords= trim($keywords);
		$keywords= str_replace(' ', ' | ', $keywords);
		$keywords= str_replace('"', "'", $keywords);

		$keywords = $this->quoteString(trim($keywords));

		//We can get a list of all the tsvector columns though this query:
		//We know what tables to search in based on the $classesToSearch variable:
        $classesPlaceholders = DB::placeholders($classesToSearch);
		$result = $this->preparedQuery("
            SELECT table_name, column_name, data_type
            FROM information_schema.columns
            WHERE data_type='tsvector' AND table_name in ($classesPlaceholders);",
            $classesToSearch
        );
		if (!$result->numRecords()) throw new Exception('there are no full text columns to search');

		$tables = array();
        $tableParameters = array();

		// Make column selection lists
		$select = array(
			'SiteTree' => array(
				'"ClassName"',
				'"SiteTree"."ID"',
				'"ParentID"',
				'"Title"',
				'"URLSegment"',
				'"Content"',
				'"LastEdited"',
				'"Created"',
				'NULL AS "Filename"',
				'NULL AS "Name"',
				'"CanViewType"'
			),
			'File' => array(
				'"ClassName"',
				'"File"."ID"',
				'0 AS "ParentID"',
				'"Title"',
				'NULL AS "URLSegment"',
				'"Content"',
				'"LastEdited"',
				'"Created"',
				'"Filename"',
				'"Name"',
				'NULL AS "CanViewType"'
			)
		);

		foreach($result as $row){
            $conditions = array();
			if($row['table_name'] === 'SiteTree' || $row['table_name'] === 'File') {
                $conditions[] = array('"ShowInSearch"' => 1);
			}

            $method = self::default_fts_search_method();
            $conditions[] = "\"{$row['table_name']}\".\"{$row['column_name']}\" $method q ";
			$query = DataObject::get($row['table_name'], $where)->dataQuery()->query();

            // Could parameterise this, but convention is only to to so for where conditions
			$query->addFrom(array(
                'tsearch' => ", to_tsquery('" . self::search_language() . "', $keywords) AS q"
            ));
			$query->setSelect(array());

			foreach($select[$row['table_name']] as $clause) {
				if(preg_match('/^(.*) +AS +"?([^"]*)"?/i', $clause, $matches)) {
					$query->selectField($matches[1], $matches[2]);
				} else {
					$query->selectField($clause);
				}
			}

			$query->selectField("ts_rank(\"{$row['table_name']}\".\"{$row['column_name']}\", q)", 'Relevance');
			$query->setOrderBy(array());

			//Add this query to the collection
			$tables[] = $query->sql($parameters);
            $tableParameters = array_merge($tableParameters, $parameters);
		}

		$limit = $pageLength;
		$offset = $start;

		if($keywords) $orderBy = " ORDER BY $sortBy";
		else $orderBy='';

		$fullQuery = "SELECT * FROM (" . implode(" UNION ", $tables) . ") AS q1 $orderBy LIMIT $limit OFFSET $offset";

		// Get records
		$records = $this->preparedQuery($fullQuery, $tableParameters);
		$totalCount=0;
		foreach($records as $record){
			$objects[] = new $record['ClassName']($record);
			$totalCount++;
		}

		if(isset($objects)) $results = new ArrayList($objects);
		else $results = new ArrayList();
		$list = new PaginatedList($results);
		$list->setLimitItems(false);
		$list->setPageStart($start);
		$list->setPageLength($pageLength);
		$list->setTotalItems($totalCount);
		return $list;
	}

	public function supportsTransactions() {
		return $this->supportsTransactions;
	}

	/*
	 * This is a quick lookup to discover if the database supports particular extensions
	 */
	public function supportsExtensions($extensions=Array('partitions', 'tablespaces', 'clustering')){
		if(isset($extensions['partitions'])) return true;
		elseif(isset($extensions['tablespaces'])) return true;
		elseif(isset($extensions['clustering'])) return true;
		else return false;
	}

	public function transactionStart($transaction_mode = false, $session_characteristics = false){
		$this->query('BEGIN;');

		if($transaction_mode) {
			$this->preparedQuery('SET TRANSACTION ?;', array($transaction_mode));
        }

		if($session_characteristics) {
			$this->preparedQuery('SET SESSION CHARACTERISTICS AS TRANSACTION ?;', array($session_characteristics));
        }
	}

	public function transactionSavepoint($savepoint){
		$this->preparedQuery("SAVEPOINT ?;", array($savepoint));
	}

	public function transactionRollback($savepoint = false){
		if($savepoint) {
			$this->preparedQuery("ROLLBACK TO ?;", array($savepoint));
        } else {
			$this->query('ROLLBACK;');
        }
	}
	
	public function transactionEnd(){
		$this->query('COMMIT;');
	}
    
	public function comparisonClause($field, $value, $exact = false, $negate = false, $caseSensitive = null, $parameterised = false) {
		if($exact && $caseSensitive === null) {
			$comp = ($negate) ? '!=' : '=';
		} else {
			$comp = ($caseSensitive === true) ? 'LIKE' : 'ILIKE';
			if($negate) $comp = 'NOT ' . $comp;
		}
	
        if($parameterised) {
            return sprintf("%s %s ?", $field, $comp);
        } else {
            return sprintf("%s %s '%s'", $field, $comp, $value);
        }
	}

	/**
	 * Function to return an SQL datetime expression that can be used with Postgres
	 * used for querying a datetime in a certain format
	 * @param string $date to be formated, can be either 'now', literal datetime like '1973-10-14 10:30:00' or field name, e.g. '"SiteTree"."Created"'
	 * @param string $format to be used, supported specifiers:
	 * %Y = Year (four digits)
	 * %m = Month (01..12)
	 * %d = Day (01..31)
	 * %H = Hour (00..23)
	 * %i = Minutes (00..59)
	 * %s = Seconds (00..59)
	 * %U = unix timestamp, can only be used on it's own
	 * @return string SQL datetime expression to query for a formatted datetime
	 */
	function formattedDatetimeClause($date, $format) {

		preg_match_all('/%(.)/', $format, $matches);
		foreach($matches[1] as $match) { 
            if(array_search($match, array('Y','m','d','H','i','s','U')) === false) {
                user_error('formattedDatetimeClause(): unsupported format character %' . $match, E_USER_WARNING);
            }
        }

		$translate = array(
			'/%Y/' => 'YYYY',
			'/%m/' => 'MM',
			'/%d/' => 'DD',
			'/%H/' => 'HH24',
			'/%i/' => 'MI',
			'/%s/' => 'SS',
		);
		$format = preg_replace(array_keys($translate), array_values($translate), $format);

		if(preg_match('/^now$/i', $date)) {
			$date = "NOW()";
		} else if(preg_match('/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/i', $date)) {
			$date = "TIMESTAMP '$date'";
		}

		if($format == '%U') return "FLOOR(EXTRACT(epoch FROM $date))";

		return "to_char($date, TEXT '$format')";

	}

	/**
	 * Function to return an SQL datetime expression that can be used with Postgres
	 * used for querying a datetime addition
	 * @param string $date, can be either 'now', literal datetime like '1973-10-14 10:30:00' or field name, e.g. '"SiteTree"."Created"'
	 * @param string $interval to be added, use the format [sign][integer] [qualifier], e.g. -1 Day, +15 minutes, +1 YEAR
	 * supported qualifiers:
	 * - years
	 * - months
	 * - days
	 * - hours
	 * - minutes
	 * - seconds
	 * This includes the singular forms as well
	 * @return string SQL datetime expression to query for a datetime (YYYY-MM-DD hh:mm:ss) which is the result of the addition
	 */
	function datetimeIntervalClause($date, $interval) {

		if(preg_match('/^now$/i', $date)) {
			$date = "NOW()";
		} else if(preg_match('/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/i', $date)) {
			$date = "TIMESTAMP '$date'";
		}

		// ... when being too precise becomes a pain. we need to cut of the fractions.
		// TIMESTAMP(0) doesn't work because it rounds instead flooring
		return "CAST(SUBSTRING(CAST($date + INTERVAL '$interval' AS VARCHAR) FROM 1 FOR 19) AS TIMESTAMP)";
	}

	/**
	 * Function to return an SQL datetime expression that can be used with Postgres
	 * used for querying a datetime substraction
	 * @param string $date1, can be either 'now', literal datetime like '1973-10-14 10:30:00' or field name, e.g. '"SiteTree"."Created"'
	 * @param string $date2 to be substracted of $date1, can be either 'now', literal datetime like '1973-10-14 10:30:00' or field name, e.g. '"SiteTree"."Created"'
	 * @return string SQL datetime expression to query for the interval between $date1 and $date2 in seconds which is the result of the substraction
	 */
	function datetimeDifferenceClause($date1, $date2) {

		if(preg_match('/^now$/i', $date1)) {
			$date1 = "NOW()";
		} else if(preg_match('/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/i', $date1)) {
			$date1 = "TIMESTAMP '$date1'";
		}

		if(preg_match('/^now$/i', $date2)) {
			$date2 = "NOW()";
		} else if(preg_match('/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/i', $date2)) {
			$date2 = "TIMESTAMP '$date2'";
		}

		return "(FLOOR(EXTRACT(epoch FROM $date1)) - FLOOR(EXTRACT(epoch from $date2)))";
	}

	function now(){
		return 'NOW()';
	}

	function random(){
		return 'RANDOM()';
	}
    
    // @todo - override selectDatabase to use the schema switcheroo stuff
}
