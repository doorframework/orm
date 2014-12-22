<?php

namespace Door\ORM;
/**
 * Description of ColumnsArray
 */
class ColumnsArray implements \ArrayAccess{

    private $container = array();
	
    public function __construct(array $columns) {
		$this->container = $columns;
    }
	
    public function offsetSet($offset, $value) {
		
		throw new Exception("Its readonly");
		
    }
	
    public function offsetExists($offset) {
        return isset($this->container[$offset]);
    }
	
    public function offsetUnset($offset) {
        unset($this->container[$offset]);
    }
	
    public function offsetGet($offset) {
        return isset($this->container[$offset]) ? $this->container[$offset] : null;
    }	
	
}
