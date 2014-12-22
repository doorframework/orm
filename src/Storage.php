<?php

namespace Door\ORM;
use Door\RDB\Database;

/**
 * Storage implementation
 */
class Storage {
	
	/**
	 * @var Database
	 */
	protected $database = null;
	
	/**
	 * @var callable
	 */
	protected $database_constructor = null;
		
	/**
	 * @var array
	 */
	protected $models = array();
	
	/**
	 *
	 * @var array
	 */
	protected $init_cache = array();
	
	/**
	 * @param Database $db
	 */
	public function set_database(Database $db)
	{
		$this->database = $db;
	}	
	
	/**
	 * @param callable $func
	 * @throws Exception
	 */
	public function set_database_constructor($func)
	{
		if(false == is_callable($func))
		{
			throw new Exception("function must be callable");
		}
		
		$this->database_constructor = $func;
	}
	
	/**
	 * Register model
	 * @param string $model_name
	 * @param string $class_name
	 * @return \Door\ORM\Storage
	 */
	public function register_model($model_name, $class_name)
	{
		$this->models[$model_name] = $class_name;
		return $this;
	}
	
	/**
	 * Register models from array
	 * @param array $models Array of model names and classes ($model_name => $class_name)
	 */
	public function register_models(array $models)
	{
		foreach($models as $model_name => $class_name)
		{
			$this->register_model($model_name, $class_name);
		}
	}
	
	/**
	 * Check if model exists
	 * @param string $model_name
	 * @return bool
	 */
	public function model_exists($model_name)
	{
		return isset($this->models[$model_name]);
	}
	
	/**
	 * Get registered model class name
	 * @param string $model_name
	 * @return string
	 * @throws Exception if model not found
	 */
	public function get_model_classname($model_name)
	{
		if(false == isset($this->models[$model_name]))
		{
			throw new Exception("model $model_name not found");
		}
		return $this->models[$model_name];
	}
	
	/**
	 * Get collection of models
	 * @param string $model_name
	 * @return \Door\ORM\Collection
	 */
	public function collection($model_name)
	{
		$model_classname = $this->get_model_classname($model_name);
		return new Collection($this, $model_name, $model_classname);
	}	
	
	/**
	 * Get database instance
	 * @return Database
	 * @throws Exception
	 */
	public function db()
	{
		if($this->database == null)
		{
			$this->init_db();
		}
		return $this->database;
	}
	
	/**
	 * Initialize database
	 * @throws Exception
	 */
	protected function init_db()
	{
		if($this->database_constructor != null)
		{
			$this->database = call_user_func($this->database_constructor);
			unset($this->database_constructor);
		}
		else
		{
			throw new Exception("Database not initialized");
		}

		if(false == ($this->database instanceof Database))
		{
			throw new Exception("Specified database creator has created bad database");
		}				
	}
	
	/**
	 * 
	 * @param string $model_name
	 * @param mixed $id
	 * @return \Door\ORM\Model
	 * @throws Exception
	 */
	public function factory($model_name, $id = null)
	{
		if(false == isset($this->models[$model_name]))
		{
			throw new Exception("Model {$model_name} not found");
		}
		
		$class = $this->models[$model_name];
		
		return new $class($this, $model_name, $id);
	}
	
	public function set_init_cache($model_name, array $init_cache)
	{
		$this->init_cache[$model_name] = $init_cache;
	}
	
	public function get_init_cache($model_name)
	{
		if(isset($this->init_cache[$model_name]))
		{
			return $this->init_cache[$model_name];
		}
		return null;
	}
	
	
	
}
