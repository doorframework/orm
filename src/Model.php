<?php

namespace Door\ORM;

use Door\RDB\Database;
use Serializable;
use Sirius\Validation\Validator;
use Exception;

/**
 * [Object Relational Mapping][ref-orm] (ORM) is a method of abstracting database
 * access to standard PHP calls. All table rows are represented as model objects,
 * with object properties representing row data. ORM in Kohana generally follows
 * the [Active Record][ref-act] pattern.
 *
 * [ref-orm]: http://wikipedia.org/wiki/Object-relational_mapping
 * [ref-act]: http://wikipedia.org/wiki/Active_record
 *
 * @package    Kohana/ORM
 * @author     Kohana Team
 * @copyright  (c) 2007-2012 Kohana Team
 * @license    http://kohanaframework.org/license
 */
class Model implements Serializable
{

    /**
     * "Has one" relationships
     * @var array
     */
    protected $_has_one = array();

    /**
     * "Belongs to" relationships
     * @var array
     */
    protected $_belongs_to = array();

    /**
     * "Has many" relationships
     * @var array
     */
    protected $_has_many = array();

    /**
     * Relationships that should always be joined
     * @var array
     */
    protected $_load_with = array();

    /**
     * Validation object created before saving/updating
     * @var Validator
     */
    protected $_validation = NULL;

    /**
     * Current object
     * @var array
     */
    protected $_object = array();

    /**
     * @var array
     */
    protected $_changed = array();

    /**
     * @var array
     */
    protected $_original_values = array();

    /**
     * @var array
     */
    protected $_related = array();

    /**
     * @var bool
     */
    protected $_valid = FALSE;

    /**
     * @var bool
     */
    protected $_loaded = FALSE;

    /**
     * @var bool
     */
    protected $_saved = FALSE;

    /**
     * @var array
     */
    protected $_sorting;

    /**
     * Foreign key suffix
     * @var string
     */
    protected $_foreign_key_suffix = '_id';

    /**
     * Model name
     * @var string
     */
    protected $_object_name;

    /**
     * Table name
     * @var string
     */
    protected $_table_name;

    /**
     * Table columns
     * @var array
     */
    protected $_table_columns;

    /**
     * Auto-update columns for updates
     * @var string
     */
    protected $_updated_column = NULL;

    /**
     * Auto-update columns for creation
     * @var string
     */
    protected $_created_column = NULL;

    /**
     * Auto-serialize and unserialize columns on get/set
     * @var array
     */
    protected $_serialize_columns = array();

    /**
     * Table primary key
     * @var string
     */
    protected $_primary_key = 'id';

    /**
     * Primary key value
     * @var mixed
     */
    protected $_primary_key_value;

    /**
     * Model configuration, table names plural?
     * @var bool
     */
    protected $_table_names_plural = TRUE;

    /**
     * Model configuration, reload on wakeup?
     * @var bool
     */
    protected $_reload_on_wakeup = TRUE;

    /**
     * Storage Object
     * @var Storage
     */
    protected $_storage = NULL;

    /**
     * Database Object
     * @var Database
     */
    protected $_db = NULL;

    /**
     * Database config group
     * @var String
     */
    protected $_db_group = NULL;

    /**
     * Database methods applied
     * @var array
     */
    protected $_db_applied = array();

    /**
     * Database methods pending
     * @var array
     */
    protected $_db_pending = array();

    /**
     * Reset builder
     * @var bool
     */
    protected $_db_reset = TRUE;

    /**
     * Database query builder
     * @var Database_Query_Builder_Select
     */
    protected $_db_builder;

    /**
     * With calls already applied
     * @var array
     */
    protected $_with_applied = array();

    /**
     * Data to be loaded into the model from a database call cast
     * @var array
     */
    protected $_cast_data = array();

    /**
     * The message filename used for validation errors.
     * Defaults to ORM::$_object_name
     * @var string
     */
    protected $_errors_filename = NULL;

    /**
     * Constructs a new model and loads a record if given
     *
     * @param   mixed $id Parameter for find or object to load
     */
    public function __construct(Storage $storage, $id = NULL)
    {
        $this->_storage = $storage;
        $this->_db = $storage->db();

        $this->_initialize();

        if ($id !== NULL) {
            $this->_init_from_id($id);
        }
    }

    /**
     * Override this method to put your initialisation values
     */
    protected function _init_model()
    {
        
    }

    private function _init_from_id($id)
    {
        if (is_array($id)) {
            foreach ($id as $column => $value) {
                // Passing an array of column => values
                $this->where($column, '=', $value);
            }

            $this->find();
        } else {
            // Passing the primary key
            $this->where($this->_object_name . '.' . $this->_primary_key, '=', $id)->find();
        }
    }

    /**
     * Prepares the model database connection
     * and loads column information.
     *
     * @return void
     */
    private function _initialize()
    {
        $init_cache = $this->_storage->get_init_cache($this->_object_name);

        // Check if this model has already been initialized
        if ($init_cache == null) {
            $this->_init_cache();
        } else {
            foreach ($init_cache as $key => $value) {
                $this->$key = $value;
            }
        }

        // Clear initial model state
        $this->clear();
    }

    private function _init_cache()
    {
        $init_fields = array(
            "_has_one", "_has_many", "_table_columns", "_belongs_to"
        );

        $this->_init_model();

        $init_cache = array();
        foreach ($init_fields as $init_field) {
            $init_cache[$init_field] = new ColumnsArray($this->$init_field);
        }

        $this->_storage->set_init_cache($this->_object_name, $init_cache);
    }

    /**
     * Unloads the current object and clears the status.
     *
     * @chainable
     * @return $this
     */
    public function clear()
    {
        // Create an array with all the columns set to NULL
        $values = array_combine(array_keys($this->_table_columns), array_fill(0, count($this->_table_columns), NULL));

        // Replace the object and reset the object status
        $this->_object = $this->_changed = $this->_related = $this->_original_values = array();

        // Replace the current object with an empty one
        $this->_load_values($values);

        // Reset primary key
        $this->_primary_key_value = NULL;

        // Reset the loaded state
        $this->_loaded = FALSE;

        $this->reset();

        return $this;
    }

    /**
     * Reloads the current object from the database.
     *
     * @chainable
     * @return $this
     */
    public function reload()
    {
        $primary_key = $this->pk();

        // Replace the object and reset the object status
        $this->_object = $this->_changed = $this->_related = $this->_original_values = array();

        // Only reload the object if we have one to reload
        if ($this->_loaded) {
            return $this->clear()
                            ->where($this->_object_name . '.' . $this->_primary_key, '=', $primary_key)
                            ->find();
        } else {
            return $this->clear();
        }
    }

    /**
     * Checks if object data is set.
     *
     * @param  string $column Column name
     * @return boolean
     */
    public function __isset($column)
    {
        return (isset($this->_object[$column]) OR
                isset($this->_related[$column]) OR
                isset($this->_has_one[$column]) OR
                isset($this->_belongs_to[$column]) OR
                isset($this->_has_many[$column]));
    }

    /**
     * Unsets object data.
     *
     * @param  string $column Column name
     * @return void
     */
    public function __unset($column)
    {
        unset($this->_object[$column], $this->_changed[$column], $this->_related[$column]);
    }

    /**
     * Displays the primary key of a model when it is converted to a string.
     *
     * @return string
     */
    public function __toString()
    {
        return (string) $this->pk();
    }

    /**
     * Allows serialization of only the object data and state, to prevent
     * "stale" objects being unserialized, which also requires less memory.
     *
     * @return string
     */
    public function serialize()
    {
        // Store only information about the object
        foreach (array('_primary_key_value', '_object', '_changed', '_loaded', '_saved', '_sorting', '_original_values') as $var) {
            $data[$var] = $this->{$var};
        }

        return serialize($data);
    }

    /**
     * Check whether the model data has been modified.
     * If $field is specified, checks whether that field was modified.
     *
     * @param string  $field  field to check for changes
     * @return  bool  Whether or not the field has changed
     */
    public function changed($field = NULL)
    {
        return ($field === NULL) ? $this->_changed : isset($this->_changed[$field]);
    }

    /**
     * Prepares the database connection and reloads the object.
     *
     * @param string $data String for unserialization
     * @return  void
     */
    public function unserialize($data)
    {
        // Initialize model
        $this->_initialize();

        foreach (unserialize($data) as $name => $var) {
            $this->{$name} = $var;
        }

        if ($this->_reload_on_wakeup === TRUE) {
            // Reload the object
            $this->reload();
        }
    }

    /**
     * Handles retrieval of all model values, relationships, and metadata.
     * [!!] This should not be overridden.
     *
     * @param   string $column Column name
     * @return  mixed
     */
    public function __get($column)
    {
        return $this->get($column);
    }

    /**
     * Handles getting of column
     * Override this method to add custom get behavior
     *
     * @param   string $column Column name
     * @throws Exception
     * @return mixed
     */
    public function get($column)
    {
        if (array_key_exists($column, $this->_object)) {
            return (in_array($column, $this->_serialize_columns)) ? $this->_unserialize_value($this->_object[$column]) : $this->_object[$column];
        } elseif (isset($this->_related[$column])) {
            // Return related model that has already been fetched
            return $this->_related[$column];
        } elseif (isset($this->_belongs_to[$column])) {
            $model = $this->_related($column);

            // Use this model's column and foreign model's primary key
            $col = $model->_object_name . '.' . $model->_primary_key;
            $val = $this->_object[$this->_belongs_to[$column]['foreign_key']];

            // Make sure we don't run WHERE "AUTO_INCREMENT column" = NULL queries. This would
            // return the last inserted record instead of an empty result.
            // See: http://mysql.localhost.net.ar/doc/refman/5.1/en/server-session-variables.html#sysvar_sql_auto_is_null
            if ($val !== NULL) {
                $model->where($col, '=', $val)->find();
            }

            return $this->_related[$column] = $model;
        } elseif (isset($this->_has_one[$column])) {
            $model = $this->_related($column);

            // Use this model's primary key value and foreign model's column
            $col = $model->_object_name . '.' . $this->_has_one[$column]['foreign_key'];
            $val = $this->pk();

            $model->where($col, '=', $val)->find();

            return $this->_related[$column] = $model;
        } elseif (isset($this->_has_many[$column])) {
            $model = $this->_storage->factory($this->_has_many[$column]['model']);

            if (isset($this->_has_many[$column]['through'])) {
                // Grab has_many "through" relationship table
                $through = $this->_has_many[$column]['through'];

                // Join on through model's target foreign key (far_key) and target model's primary key
                $join_col1 = $through . '.' . $this->_has_many[$column]['far_key'];
                $join_col2 = $model->_object_name . '.' . $model->_primary_key;

                $model->join($through)->on($join_col1, '=', $join_col2);

                // Through table's source foreign key (foreign_key) should be this model's primary key
                $col = $through . '.' . $this->_has_many[$column]['foreign_key'];
                $val = $this->pk();
            } else {
                // Simple has_many relationship, search where target model's foreign key is this model's primary key
                $col = $model->_object_name . '.' . $this->_has_many[$column]['foreign_key'];
                $val = $this->pk();
            }

            return $model->where($col, '=', $val);
        } else {
            throw new Exception("The $column property does not exist in the model {$this->_object_name}");
        }
    }

    /**
     * Base set method.
     * [!!] This should not be overridden.
     *
     * @param  string $column  Column name
     * @param  mixed  $value   Column value
     * @return void
     */
    public function __set($column, $value)
    {
        $this->set($column, $value);
    }

    /**
     * Handles setting of columns
     * Override this method to add custom set behavior
     *
     * @param  string $column Column name
     * @param  mixed  $value  Column value
     * @throws Exception
     * @return $this
     */
    public function set($column, $value)
    {
        if (!isset($this->_object_name)) {
            // Object not yet constructed, so we're loading data from a database call cast
            $this->_cast_data[$column] = $value;

            return $this;
        }

        if (in_array($column, $this->_serialize_columns)) {
            $value = $this->_serialize_value($value);
        }

        if (array_key_exists($column, $this->_object)) {
            // Filter the data
            $value = $this->run_filter($column, $value);

            // See if the data really changed
            if ($value !== $this->_object[$column]) {
                $this->_object[$column] = $value;

                // Data has changed
                $this->_changed[$column] = $column;

                // Object is no longer saved or valid
                $this->_saved = $this->_valid = FALSE;
            }
        } elseif (isset($this->_belongs_to[$column])) {
            // Update related object itself
            $this->_related[$column] = $value;

            // Update the foreign key of this model
            $this->_object[$this->_belongs_to[$column]['foreign_key']] = ($value instanceof ORM) ? $value->pk() : NULL;

            $this->_changed[$column] = $this->_belongs_to[$column]['foreign_key'];
        } else {
            throw new Exception("The $column property does not exist in the model {$this->_object_name}");
        }

        return $this;
    }

    /**
     * Set values from an array with support for one-one relationships.  This method should be used
     * for loading in post data, etc.
     *
     * @param  array $values   Array of column => val
     * @param  array $expected Array of keys to take from $values
     * @return $this
     */
    public function values(array $values, array $expected = NULL)
    {
        // Default to expecting everything except the primary key
        if ($expected === NULL) {
            $expected = array_keys($this->_table_columns);

            // Don't set the primary key by default
            unset($values[$this->_primary_key]);
        }

        foreach ($expected as $key => $column) {
            if (is_string($key)) {
                // isset() fails when the value is NULL (we want it to pass)
                if (!array_key_exists($key, $values))
                    continue;

                // Try to set values to a related model
                $this->{$key}->values($values[$key], $column);
            }
            else {
                // isset() fails when the value is NULL (we want it to pass)
                if (!array_key_exists($column, $values))
                    continue;

                // Update the column, respects __set()
                $this->$column = $values[$column];
            }
        }

        return $this;
    }

    /**
     * Returns the values of this object as an array
     *
     * @return array
     */
    public function as_array()
    {
        $object = array();

        foreach (array_keys($this->_object) as $column) {
            // Call __get for any user processing
            $object[$column] = $this->__get($column);
        }

        return $object;
    }

    /**
     * Binds another one-to-one object to this model.  One-to-one objects
     * can be nested using 'object1:object2' syntax
     *
     * @param  string $target_path Target model to bind to
     * @return $this
     */
    public function with($target_path)
    {
        if (isset($this->_with_applied[$target_path])) {
            // Don't join anything already joined
            return $this;
        }

        // Split object parts
        $aliases = explode(':', $target_path);
        $target = $this;
        foreach ($aliases as $alias) {
            // Go down the line of objects to find the given target
            $parent = $target;
            $target = $parent->_related($alias);

            if (!$target) {
                // Can't find related object
                return $this;
            }
        }

        // Target alias is at the end
        $target_alias = $alias;

        // Pop-off top alias to get the parent path (user:photo:tag becomes user:photo - the parent table prefix)
        array_pop($aliases);
        $parent_path = implode(':', $aliases);

        if (empty($parent_path)) {
            // Use this table name itself for the parent path
            $parent_path = $this->_object_name;
        } else {
            if (!isset($this->_with_applied[$parent_path])) {
                // If the parent path hasn't been joined yet, do it first (otherwise LEFT JOINs fail)
                $this->with($parent_path);
            }
        }

        // Add to with_applied to prevent duplicate joins
        $this->_with_applied[$target_path] = TRUE;

        // Use the keys of the empty object to determine the columns
        foreach (array_keys($target->_object) as $column) {
            $name = $target_path . '.' . $column;
            $alias = $target_path . ':' . $column;

            // Add the prefix so that load_result can determine the relationship
            $this->select(array($name, $alias));
        }

        if (isset($parent->_belongs_to[$target_alias])) {
            // Parent belongs_to target, use target's primary key and parent's foreign key
            $join_col1 = $target_path . '.' . $target->_primary_key;
            $join_col2 = $parent_path . '.' . $parent->_belongs_to[$target_alias]['foreign_key'];
        } else {
            // Parent has_one target, use parent's primary key as target's foreign key
            $join_col1 = $parent_path . '.' . $parent->_primary_key;
            $join_col2 = $target_path . '.' . $parent->_has_one[$target_alias]['foreign_key'];
        }

        // Join the related object into the result
        $this->join(array($target->_table_name, $target_path), 'LEFT')->on($join_col1, '=', $join_col2);

        return $this;
    }

    /**
     * Initializes the Database Builder to given query type
     *
     * @param  integer $type Type of Database query
     * @return $this
     */
    protected function _build($type)
    {
        // Construct new builder object based on query type
        switch ($type) {
            case Database::SELECT:
                $this->_db_builder = $this->_db->select();
                break;
            case Database::UPDATE:
                $this->_db_builder = $this->_db->update(array($this->_table_name, $this->_object_name));
                break;
            case Database::DELETE:
                // Cannot use an alias for DELETE queries
                $this->_db_builder = $this->_db->delete($this->_table_name);
        }

        // Process pending database method calls
        foreach ($this->_db_pending as $method) {
            $name = $method['name'];
            $args = $method['args'];

            $this->_db_applied[$name] = $name;

            call_user_func_array(array($this->_db_builder, $name), $args);
        }

        return $this;
    }

    /**
     * Finds and loads a single database row into the object.
     *
     * @chainable
     * @throws Exception
     * @return $this
     */
    public function find()
    {
        if ($this->_loaded)
            throw new Exception('Method find() cannot be called on loaded objects');

        if (!empty($this->_load_with)) {
            foreach ($this->_load_with as $alias) {
                // Bind auto relationships
                $this->with($alias);
            }
        }

        $this->_build(Database::SELECT);

        return $this->_load_result(FALSE);
    }

    /**
     * Finds multiple database rows and returns an iterator of the rows found.
     *
     * @throws Exception
     * @return Database_Result
     */
    public function find_all()
    {
        if ($this->_loaded)
            throw new Exception('Method find_all() cannot be called on loaded objects');

        if (!empty($this->_load_with)) {
            foreach ($this->_load_with as $alias) {
                // Bind auto relationships
                $this->with($alias);
            }
        }

        $this->_build(Database::SELECT);

        return $this->_load_result(TRUE);
    }

    /**
     * Returns an array of columns to include in the select query. This method
     * can be overridden to change the default select behavior.
     *
     * @return array Columns to select
     */
    protected function _build_select()
    {
        $columns = array();

        foreach ($this->_table_columns as $column => $_) {
            $columns[] = array($this->_object_name . '.' . $column, $column);
        }

        return $columns;
    }

    /**
     * Loads a database result, either as a new record for this model, or as
     * an iterator for multiple rows.
     *
     * @chainable
     * @param  bool $multiple Return an iterator or load a single row
     * @return $this|Database_Result
     */
    protected function _load_result($multiple = FALSE)
    {
        $this->_db_builder->from(array($this->_table_name, $this->_object_name));

        if ($multiple === FALSE) {
            // Only fetch 1 record
            $this->_db_builder->limit(1);
        }

        // Select all columns by default
        $this->_db_builder->select_array($this->_build_select());

        if (!isset($this->_db_applied['order_by']) AND ! empty($this->_sorting)) {
            foreach ($this->_sorting as $column => $direction) {
                if (strpos($column, '.') === FALSE) {
                    // Sorting column for use in JOINs
                    $column = $this->_object_name . '.' . $column;
                }

                $this->_db_builder->order_by($column, $direction);
            }
        }

        if ($multiple === TRUE) {
            // Return database iterator casting to this object type
            $result = $this->_db_builder->as_object(get_class($this))->execute($this->_db);

            $this->reset();

            return $result;
        } else {
            // Load the result as an associative array
            $result = $this->_db_builder->as_assoc()->execute($this->_db);

            $this->reset();

            if ($result->count() === 1) {
                // Load object values
                $this->_load_values($result->current());
            } else {
                // Clear the object, nothing was found
                $this->clear();
            }

            return $this;
        }
    }

    /**
     * Loads an array of values into into the current object.
     *
     * @chainable
     * @param  array $values Values to load
     * @return $this
     */
    protected function _load_values(array $values)
    {
        if (array_key_exists($this->_primary_key, $values)) {
            if ($values[$this->_primary_key] !== NULL) {
                // Flag as loaded and valid
                $this->_loaded = $this->_valid = TRUE;

                // Store primary key
                $this->_primary_key_value = $values[$this->_primary_key];
            } else {
                // Not loaded or valid
                $this->_loaded = $this->_valid = FALSE;
            }
        }

        // Related objects
        $related = array();

        foreach ($values as $column => $value) {
            if (strpos($column, ':') === FALSE) {
                // Load the value to this model
                $this->_object[$column] = $value;
            } else {
                // Column belongs to a related model
                list ($prefix, $column) = explode(':', $column, 2);

                $related[$prefix][$column] = $value;
            }
        }

        if (!empty($related)) {
            foreach ($related as $object => $values) {
                // Load the related objects with the values in the result
                $this->_related($object)->_load_values($values);
            }
        }

        if ($this->_loaded) {
            // Store the object in its original state
            $this->_original_values = $this->_object;
        }

        return $this;
    }

    /**
     * Rule definitions for validation
     *
     * @return array
     */
    public function rules()
    {
        return array();
    }

    /**
     * Filters a value for a specific column
     *
     * @param  string $field  The column name
     * @param  string $value  The value to filter
     * @return string
     */
    protected function run_filter($field, $value)
    {
        $filters = $this->filters();

        // Get the filters for this column
        $wildcards = empty($filters[TRUE]) ? array() : $filters[TRUE];

        // Merge in the wildcards
        $filters = empty($filters[$field]) ? $wildcards : array_merge($wildcards, $filters[$field]);

        // Bind the field name and model so they can be used in the filter method
        $_bound = array
            (
            ':field' => $field,
            ':model' => $this,
        );

        foreach ($filters as $array) {
            // Value needs to be bound inside the loop so we are always using the
            // version that was modified by the filters that already ran
            $_bound[':value'] = $value;

            // Filters are defined as array($filter, $params)
            $filter = $array[0];
            $params = Arr::get($array, 1, array(':value'));

            foreach ($params as $key => $param) {
                if (is_string($param) AND array_key_exists($param, $_bound)) {
                    // Replace with bound value
                    $params[$key] = $_bound[$param];
                }
            }

            if (is_array($filter) OR ! is_string($filter)) {
                // This is either a callback as an array or a lambda
                $value = call_user_func_array($filter, $params);
            } elseif (strpos($filter, '::') === FALSE) {
                // Use a function call
                $function = new ReflectionFunction($filter);

                // Call $function($this[$field], $param, ...) with Reflection
                $value = $function->invokeArgs($params);
            } else {
                // Split the class and method of the rule
                list($class, $method) = explode('::', $filter, 2);

                // Use a static method call
                $method = new ReflectionMethod($class, $method);

                // Call $Class::$method($this[$field], $param, ...) with Reflection
                $value = $method->invokeArgs(NULL, $params);
            }
        }

        return $value;
    }

    /**
     * Filter definitions for validation
     *
     * @return array
     */
    public function filters()
    {
        return array();
    }

    /**
     * Label definitions for validation
     *
     * @return array
     */
    public function labels()
    {
        return array();
    }

    /**
     * Validates the current model's data
     *
     * @param  Validation $extra_validation Validation object
     * @return boolean
     */
    public function check()
    {
        $this->_validation();
        return $this->validation()->validate($this->_object);
    }

    /**
     * Insert a new object to the database
     * @param  Validation $validation Validation object
     * @throws Exception
     * @return $this
     */
    public function create()
    {
        if ($this->_loaded)
            throw new Exception('Cannot create model because it is already loaded.');

        if(false == $this->check()) {
            throw new Exception("Model not valid");
        }

        $data = array();
        foreach ($this->_changed as $column) {
            // Generate list of column => values
            $data[$column] = $this->_object[$column];
        }

        if (is_array($this->_created_column)) {
            // Fill the created column
            $column = $this->_created_column['column'];
            $format = $this->_created_column['format'];

            $data[$column] = $this->_object[$column] = ($format === TRUE) ? time() : date($format);
        }

        $result = $this->_db->insert($this->_table_name)
                ->columns(array_keys($data))
                ->values(array_values($data))
                ->execute($this->_db);

        if (!array_key_exists($this->_primary_key, $data)) {
            // Load the insert id as the primary key if it was left out
            $this->_object[$this->_primary_key] = $this->_primary_key_value = $result[0];
        } else {
            $this->_primary_key_value = $this->_object[$this->_primary_key];
        }

        // Object is now loaded and saved
        $this->_loaded = $this->_saved = TRUE;

        // All changes have been saved
        $this->_changed = array();
        $this->_original_values = $this->_object;

        return $this;
    }

    /**
     * Updates a single record or multiple records
     *
     * @chainable
     * @param  Validation $validation Validation object
     * @throws Exception
     * @return $this
     */
    public function update()
    {
        if (!$this->_loaded)
            throw new Exception('Cannot update  model because it is not loaded.');

        if(false == $this->check()) {
            throw new Exception("model not valid");
        }

        if (empty($this->_changed)) {
            // Nothing to update
            return $this;
        }

        $data = array();
        foreach ($this->_changed as $column) {
            // Compile changed data
            $data[$column] = $this->_object[$column];
        }

        if (is_array($this->_updated_column)) {
            // Fill the updated column
            $column = $this->_updated_column['column'];
            $format = $this->_updated_column['format'];

            $data[$column] = $this->_object[$column] = ($format === TRUE) ? time() : date($format);
        }

        // Use primary key value
        $id = $this->pk();

        // Update a single record
        $this->_db->update($this->_table_name)
                ->set($data)
                ->where($this->_primary_key, '=', $id)
                ->execute($this->_db);

        if (isset($data[$this->_primary_key])) {
            // Primary key was changed, reflect it
            $this->_primary_key_value = $data[$this->_primary_key];
        }

        // Object has been saved
        $this->_saved = TRUE;

        // All changes have been saved
        $this->_changed = array();
        $this->_original_values = $this->_object;

        return $this;
    }

    /**
     * Updates or Creates the record depending on loaded()
     *
     * @chainable
     * @param  Validation $validation Validation object
     * @return $this
     */
    public function save()
    {
        return $this->loaded() ? $this->update() : $this->create();
    }

    /**
     * Deletes a single record while ignoring relationships.
     *
     * @chainable
     * @throws Exception
     * @return $this
     */
    public function delete()
    {
        if (!$this->_loaded)
            throw new Exception('Cannot delete model because it is not loaded.');

        // Use primary key value
        $id = $this->pk();

        // Delete the object
        $this->_db->delete($this->_table_name)
                ->where($this->_primary_key, '=', $id)
                ->execute($this->_db);

        return $this->clear();
    }

    /**
     * Tests if this object has a relationship to a different model,
     * or an array of different models. When providing far keys, the number
     * of relations must equal the number of keys.
     * 
     *
     *     // Check if $model has the login role
     *     $model->has('roles', $this->_storage->factory('role', array('name' => 'login')));
     *     // Check for the login role if you know the roles.id is 5
     *     $model->has('roles', 5);
     *     // Check for all of the following roles
     *     $model->has('roles', array(1, 2, 3, 4));
     *     // Check if $model has any roles
     *     $model->has('roles')
     *
     * @param  string  $alias    Alias of the has_many "through" relationship
     * @param  mixed   $far_keys Related model, primary key, or an array of primary keys
     * @return boolean
     */
    public function has($alias, $far_keys = NULL)
    {
        $count = $this->count_relations($alias, $far_keys);
        if ($far_keys === NULL) {
            return (bool) $count;
        } else {
            return $count === count($far_keys);
        }
    }

    /**
     * Tests if this object has a relationship to a different model,
     * or an array of different models. When providing far keys, this function
     * only checks that at least one of the relationships is satisfied.
     *
     *     // Check if $model has the login role
     *     $model->has('roles', $this->_storage->factory('role', array('name' => 'login')));
     *     // Check for the login role if you know the roles.id is 5
     *     $model->has('roles', 5);
     *     // Check for any of the following roles
     *     $model->has('roles', array(1, 2, 3, 4));
     *     // Check if $model has any roles
     *     $model->has('roles')
     *
     * @param  string  $alias    Alias of the has_many "through" relationship
     * @param  mixed   $far_keys Related model, primary key, or an array of primary keys
     * @return boolean
     */
    public function has_any($alias, $far_keys = NULL)
    {
        return (bool) $this->count_relations($alias, $far_keys);
    }

    /**
     * Returns the number of relationships 
     *
     *     // Counts the number of times the login role is attached to $model
     *     $model->count_relations('roles', $this->_storage->factory('role', array('name' => 'login')));
     *     // Counts the number of times role 5 is attached to $model
     *     $model->count_relations('roles', 5);
     *     // Counts the number of times any of roles 1, 2, 3, or 4 are attached to
     *     // $model
     *     $model->count_relations('roles', array(1, 2, 3, 4));
     *     // Counts the number roles attached to $model
     *     $model->count_relations('roles')
     *
     * @param  string  $alias    Alias of the has_many "through" relationship
     * @param  mixed   $far_keys Related model, primary key, or an array of primary keys
     * @return integer
     */
    public function count_relations($alias, $far_keys = NULL)
    {
        if ($far_keys === NULL) {
            return (int) $this->_db->select(array($this->_db->expr('COUNT(*)'), 'records_found'))
                            ->from($this->_has_many[$alias]['through'])
                            ->where($this->_has_many[$alias]['foreign_key'], '=', $this->pk())
                            ->execute($this->_db)->get('records_found');
        }

        $far_keys = ($far_keys instanceof ORM) ? $far_keys->pk() : $far_keys;

        // We need an array to simplify the logic
        $far_keys = (array) $far_keys;

        // Nothing to check if the model isn't loaded or we don't have any far_keys
        if (!$far_keys OR ! $this->_loaded)
            return 0;

        $count = (int) $this->_db->select(array($this->_db->expr('COUNT(*)'), 'records_found'))
                        ->from($this->_has_many[$alias]['through'])
                        ->where($this->_has_many[$alias]['foreign_key'], '=', $this->pk())
                        ->where($this->_has_many[$alias]['far_key'], 'IN', $far_keys)
                        ->execute($this->_db)->get('records_found');

        // Rows found need to match the rows searched
        return (int) $count;
    }

    /**
     * Adds a new relationship to between this model and another.
     *
     *     // Add the login role using a model instance
     *     $model->add('roles', $this->_storage->factory('role', array('name' => 'login')));
     *     // Add the login role if you know the roles.id is 5
     *     $model->add('roles', 5);
     *     // Add multiple roles (for example, from checkboxes on a form)
     *     $model->add('roles', array(1, 2, 3, 4));
     *
     * @param  string  $alias    Alias of the has_many "through" relationship
     * @param  mixed   $far_keys Related model, primary key, or an array of primary keys
     * @return $this
     */
    public function add($alias, $far_keys)
    {
        $far_keys = ($far_keys instanceof ORM) ? $far_keys->pk() : $far_keys;

        $columns = array($this->_has_many[$alias]['foreign_key'], $this->_has_many[$alias]['far_key']);
        $foreign_key = $this->pk();

        $query = $this->_db->insert($this->_has_many[$alias]['through'], $columns);

        foreach ((array) $far_keys as $key) {
            $query->values(array($foreign_key, $key));
        }

        $query->execute($this->_db);

        return $this;
    }

    /**
     * Removes a relationship between this model and another.
     *
     *     // Remove a role using a model instance
     *     $model->remove('roles', $this->_storage->factory('role', array('name' => 'login')));
     *     // Remove the role knowing the primary key
     *     $model->remove('roles', 5);
     *     // Remove multiple roles (for example, from checkboxes on a form)
     *     $model->remove('roles', array(1, 2, 3, 4));
     *     // Remove all related roles
     *     $model->remove('roles');
     *
     * @param  string $alias    Alias of the has_many "through" relationship
     * @param  mixed  $far_keys Related model, primary key, or an array of primary keys
     * @return $this
     */
    public function remove($alias, $far_keys = NULL)
    {
        $far_keys = ($far_keys instanceof ORM) ? $far_keys->pk() : $far_keys;

        $query = $this->_db->delete($this->_has_many[$alias]['through'])
                ->where($this->_has_many[$alias]['foreign_key'], '=', $this->pk());

        if ($far_keys !== NULL) {
            // Remove all the relationships in the array
            $query->where($this->_has_many[$alias]['far_key'], 'IN', (array) $far_keys);
        }

        $query->execute($this->_db);

        return $this;
    }

    /**
     * Count the number of records in the table.
     *
     * @return integer
     */
    public function count_all()
    {
        $selects = array();

        foreach ($this->_db_pending as $key => $method) {
            if ($method['name'] == 'select') {
                // Ignore any selected columns for now
                $selects[$key] = $method;
                unset($this->_db_pending[$key]);
            }
        }

        if (!empty($this->_load_with)) {
            foreach ($this->_load_with as $alias) {
                // Bind relationship
                $this->with($alias);
            }
        }

        $this->_build(Database::SELECT);

        $records = $this->_db_builder->from(array($this->_table_name, $this->_object_name))
                ->select(array($this->_db->expr('COUNT(' . $this->_db->quote_column($this->_object_name . '.' . $this->_primary_key) . ')'), 'records_found'))
                ->execute($this->_db)
                ->get('records_found');

        // Add back in selected columns
        $this->_db_pending += $selects;

        $this->reset();

        // Return the total number of records in a table
        return (int) $records;
    }

    /**
     * Proxy method to Database list_columns.
     *
     * @return array
     */
    public function list_columns()
    {
        // Proxy to database
        return $this->_db->list_columns($this->_table_name);
    }

    /**
     * Returns an ORM model for the given one-one related alias
     *
     * @param  string $alias Alias name
     * @return $this
     */
    protected function _related($alias)
    {
        if (isset($this->_related[$alias])) {
            return $this->_related[$alias];
        } elseif (isset($this->_has_one[$alias])) {
            return $this->_related[$alias] = $this->_storage->factory($this->_has_one[$alias]['model']);
        } elseif (isset($this->_belongs_to[$alias])) {
            return $this->_related[$alias] = $this->_storage->factory($this->_belongs_to[$alias]['model']);
        } else {
            return FALSE;
        }
    }

    /**
     * Returns the value of the primary key
     *
     * @return mixed Primary key
     */
    public function pk()
    {
        return $this->_primary_key_value;
    }

    /**
     * Returns last executed query
     *
     * @return string
     */
    public function last_query()
    {
        return $this->_db->last_query;
    }

    /**
     * Clears query builder.  Passing FALSE is useful to keep the existing
     * query conditions for another query.
     *
     * @param bool $next Pass FALSE to avoid resetting on the next call
     * @return $this
     */
    public function reset($next = TRUE)
    {
        if ($next AND $this->_db_reset) {
            $this->_db_pending = array();
            $this->_db_applied = array();
            $this->_db_builder = NULL;
            $this->_with_applied = array();
        }

        // Reset on the next call?
        $this->_db_reset = $next;

        return $this;
    }

    protected function _serialize_value($value)
    {
        return json_encode($value);
    }

    protected function _unserialize_value($value)
    {
        return json_decode($value, TRUE);
    }

    public function object_name()
    {
        return $this->_object_name;
    }

    public function loaded()
    {
        return $this->_loaded;
    }

    public function saved()
    {
        return $this->_saved;
    }

    public function primary_key()
    {
        return $this->_primary_key;
    }

    public function table_name()
    {
        return $this->_table_name;
    }

    public function table_columns()
    {
        return $this->_table_columns;
    }

    public function has_one()
    {
        return $this->_has_one;
    }

    public function belongs_to()
    {
        return $this->_belongs_to;
    }

    public function has_many()
    {
        return $this->_has_many;
    }

    public function load_with()
    {
        return $this->_load_with;
    }

    public function original_values()
    {
        return $this->_original_values;
    }

    public function created_column()
    {
        return $this->_created_column;
    }

    public function updated_column()
    {
        return $this->_updated_column;
    }

    public function validation()
    {
        if (!isset($this->_validation)) {
            // Initialize the validation object
            $this->_validation();
        }

        return $this->_validation;
    }

    protected function _validation()
    {
        $this->_validation = new Validator();
    }

    public function object()
    {
        return $this->_object;
    }

    public function errors_filename()
    {
        return $this->_errors_filename;
    }

    /**
     * Alias of and_where()
     *
     * @param   mixed   $column  column name or array($column, $alias) or object
     * @param   string  $op      logic operator
     * @param   mixed   $value   column value
     * @return  $this
     */
    public function where($column, $op, $value)
    {
        // Add pending database call which is executed after query type is determined
        $this->_db_pending[] = array(
            'name' => 'where',
            'args' => array($column, $op, $value),
        );

        return $this;
    }

    /**
     * Creates a new "AND WHERE" condition for the query.
     *
     * @param   mixed   $column  column name or array($column, $alias) or object
     * @param   string  $op      logic operator
     * @param   mixed   $value   column value
     * @return  $this
     */
    public function and_where($column, $op, $value)
    {
        // Add pending database call which is executed after query type is determined
        $this->_db_pending[] = array(
            'name' => 'and_where',
            'args' => array($column, $op, $value),
        );

        return $this;
    }

    /**
     * Creates a new "OR WHERE" condition for the query.
     *
     * @param   mixed   $column  column name or array($column, $alias) or object
     * @param   string  $op      logic operator
     * @param   mixed   $value   column value
     * @return  $this
     */
    public function or_where($column, $op, $value)
    {
        // Add pending database call which is executed after query type is determined
        $this->_db_pending[] = array(
            'name' => 'or_where',
            'args' => array($column, $op, $value),
        );

        return $this;
    }

    /**
     * Alias of and_where_open()
     *
     * @return  $this
     */
    public function where_open()
    {
        return $this->and_where_open();
    }

    /**
     * Opens a new "AND WHERE (...)" grouping.
     *
     * @return  $this
     */
    public function and_where_open()
    {
        // Add pending database call which is executed after query type is determined
        $this->_db_pending[] = array(
            'name' => 'and_where_open',
            'args' => array(),
        );

        return $this;
    }

    /**
     * Opens a new "OR WHERE (...)" grouping.
     *
     * @return  $this
     */
    public function or_where_open()
    {
        // Add pending database call which is executed after query type is determined
        $this->_db_pending[] = array(
            'name' => 'or_where_open',
            'args' => array(),
        );

        return $this;
    }

    /**
     * Closes an open "AND WHERE (...)" grouping.
     *
     * @return  $this
     */
    public function where_close()
    {
        return $this->and_where_close();
    }

    /**
     * Closes an open "AND WHERE (...)" grouping.
     *
     * @return  $this
     */
    public function and_where_close()
    {
        // Add pending database call which is executed after query type is determined
        $this->_db_pending[] = array(
            'name' => 'and_where_close',
            'args' => array(),
        );

        return $this;
    }

    /**
     * Closes an open "OR WHERE (...)" grouping.
     *
     * @return  $this
     */
    public function or_where_close()
    {
        // Add pending database call which is executed after query type is determined
        $this->_db_pending[] = array(
            'name' => 'or_where_close',
            'args' => array(),
        );

        return $this;
    }

    /**
     * Applies sorting with "ORDER BY ..."
     *
     * @param   mixed   $column     column name or array($column, $alias) or object
     * @param   string  $direction  direction of sorting
     * @return  $this
     */
    public function order_by($column, $direction = NULL)
    {
        // Add pending database call which is executed after query type is determined
        $this->_db_pending[] = array(
            'name' => 'order_by',
            'args' => array($column, $direction),
        );

        return $this;
    }

    /**
     * Return up to "LIMIT ..." results
     *
     * @param   integer  $number  maximum results to return
     * @return  $this
     */
    public function limit($number)
    {
        // Add pending database call which is executed after query type is determined
        $this->_db_pending[] = array(
            'name' => 'limit',
            'args' => array($number),
        );

        return $this;
    }

    /**
     * Enables or disables selecting only unique columns using "SELECT DISTINCT"
     *
     * @param   boolean  $value  enable or disable distinct columns
     * @return  $this
     */
    public function distinct($value)
    {
        // Add pending database call which is executed after query type is determined
        $this->_db_pending[] = array(
            'name' => 'distinct',
            'args' => array($value),
        );

        return $this;
    }

    /**
     * Choose the columns to select from.
     *
     * @param   mixed  $columns  column name or array($column, $alias) or object
     * @param   ...
     * @return  $this
     */
    public function select($columns = NULL)
    {
        $columns = func_get_args();

        // Add pending database call which is executed after query type is determined
        $this->_db_pending[] = array(
            'name' => 'select',
            'args' => $columns,
        );

        return $this;
    }

    /**
     * Choose the tables to select "FROM ..."
     *
     * @param   mixed  $tables  table name or array($table, $alias) or object
     * @param   ...
     * @return  $this
     */
    public function from($tables)
    {
        $tables = func_get_args();

        // Add pending database call which is executed after query type is determined
        $this->_db_pending[] = array(
            'name' => 'from',
            'args' => $tables,
        );

        return $this;
    }

    /**
     * Adds addition tables to "JOIN ...".
     *
     * @param   mixed   $table  column name or array($column, $alias) or object
     * @param   string  $type   join type (LEFT, RIGHT, INNER, etc)
     * @return  $this
     */
    public function join($table, $type = NULL)
    {
        // Add pending database call which is executed after query type is determined
        $this->_db_pending[] = array(
            'name' => 'join',
            'args' => array($table, $type),
        );

        return $this;
    }

    /**
     * Adds "ON ..." conditions for the last created JOIN statement.
     *
     * @param   mixed   $c1  column name or array($column, $alias) or object
     * @param   string  $op  logic operator
     * @param   mixed   $c2  column name or array($column, $alias) or object
     * @return  $this
     */
    public function on($c1, $op, $c2)
    {
        // Add pending database call which is executed after query type is determined
        $this->_db_pending[] = array(
            'name' => 'on',
            'args' => array($c1, $op, $c2),
        );

        return $this;
    }

    /**
     * Creates a "GROUP BY ..." filter.
     *
     * @param   mixed   $columns  column name or array($column, $alias) or object
     * @param   ...
     * @return  $this
     */
    public function group_by($columns)
    {
        $columns = func_get_args();

        // Add pending database call which is executed after query type is determined
        $this->_db_pending[] = array(
            'name' => 'group_by',
            'args' => $columns,
        );

        return $this;
    }

    /**
     * Alias of and_having()
     *
     * @param   mixed   $column  column name or array($column, $alias) or object
     * @param   string  $op      logic operator
     * @param   mixed   $value   column value
     * @return  $this
     */
    public function having($column, $op, $value = NULL)
    {
        return $this->and_having($column, $op, $value);
    }

    /**
     * Creates a new "AND HAVING" condition for the query.
     *
     * @param   mixed   $column  column name or array($column, $alias) or object
     * @param   string  $op      logic operator
     * @param   mixed   $value   column value
     * @return  $this
     */
    public function and_having($column, $op, $value = NULL)
    {
        // Add pending database call which is executed after query type is determined
        $this->_db_pending[] = array(
            'name' => 'and_having',
            'args' => array($column, $op, $value),
        );

        return $this;
    }

    /**
     * Creates a new "OR HAVING" condition for the query.
     *
     * @param   mixed   $column  column name or array($column, $alias) or object
     * @param   string  $op      logic operator
     * @param   mixed   $value   column value
     * @return  $this
     */
    public function or_having($column, $op, $value = NULL)
    {
        // Add pending database call which is executed after query type is determined
        $this->_db_pending[] = array(
            'name' => 'or_having',
            'args' => array($column, $op, $value),
        );

        return $this;
    }

    /**
     * Alias of and_having_open()
     *
     * @return  $this
     */
    public function having_open()
    {
        return $this->and_having_open();
    }

    /**
     * Opens a new "AND HAVING (...)" grouping.
     *
     * @return  $this
     */
    public function and_having_open()
    {
        // Add pending database call which is executed after query type is determined
        $this->_db_pending[] = array(
            'name' => 'and_having_open',
            'args' => array(),
        );

        return $this;
    }

    /**
     * Opens a new "OR HAVING (...)" grouping.
     *
     * @return  $this
     */
    public function or_having_open()
    {
        // Add pending database call which is executed after query type is determined
        $this->_db_pending[] = array(
            'name' => 'or_having_open',
            'args' => array(),
        );

        return $this;
    }

    /**
     * Closes an open "AND HAVING (...)" grouping.
     *
     * @return  $this
     */
    public function having_close()
    {
        return $this->and_having_close();
    }

    /**
     * Closes an open "AND HAVING (...)" grouping.
     *
     * @return  $this
     */
    public function and_having_close()
    {
        // Add pending database call which is executed after query type is determined
        $this->_db_pending[] = array(
            'name' => 'and_having_close',
            'args' => array(),
        );

        return $this;
    }

    /**
     * Closes an open "OR HAVING (...)" grouping.
     *
     * @return  $this
     */
    public function or_having_close()
    {
        // Add pending database call which is executed after query type is determined
        $this->_db_pending[] = array(
            'name' => 'or_having_close',
            'args' => array(),
        );

        return $this;
    }

    /**
     * Start returning results after "OFFSET ..."
     *
     * @param   integer   $number  starting result number
     * @return  $this
     */
    public function offset($number)
    {
        // Add pending database call which is executed after query type is determined
        $this->_db_pending[] = array(
            'name' => 'offset',
            'args' => array($number),
        );

        return $this;
    }

    /**
     * Enables the query to be cached for a specified amount of time.
     *
     * @param   integer  $lifetime  number of seconds to cache
     * @return  $this
     * @uses    Kohana::$cache_life
     */
    public function cached($lifetime = NULL)
    {
        // Add pending database call which is executed after query type is determined
        $this->_db_pending[] = array(
            'name' => 'cached',
            'args' => array($lifetime),
        );

        return $this;
    }

    /**
     * Set the value of a parameter in the query.
     *
     * @param   string   $param  parameter key to replace
     * @param   mixed    $value  value to use
     * @return  $this
     */
    public function param($param, $value)
    {
        // Add pending database call which is executed after query type is determined
        $this->_db_pending[] = array(
            'name' => 'param',
            'args' => array($param, $value),
        );

        return $this;
    }

    /**
     * Adds "USING ..." conditions for the last created JOIN statement.
     *
     * @param   string  $columns  column name
     * @return  $this
     */
    public function using($columns)
    {
        // Add pending database call which is executed after query type is determined
        $this->_db_pending[] = array(
            'name' => 'using',
            'args' => array($columns),
        );

        return $this;
    }

    /**
     * Checks whether a column value is unique.
     * Excludes itself if loaded.
     *
     * @param   string   $field  the field to check for uniqueness
     * @param   mixed    $value  the value to check for uniqueness
     * @return  bool     whteher the value is unique
     */
    public function unique($field, $value)
    {
        $model = $this->_storage->factory($this->object_name())
                ->where($field, '=', $value)
                ->find();

        if ($this->loaded()) {
            return (!($model->loaded() AND $model->pk() != $this->pk()));
        }

        return (!$model->loaded());
    }

}

// End ORM
