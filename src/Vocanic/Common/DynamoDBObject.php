<?php
/**
 * @namespace Vocanic.Common
 */
namespace Vocanic\Common;

use Aws\Common\Enum\Region;
use Aws\DynamoDb\DynamoDbClient;
use Aws\DynamoDb\Marshaler;
use Aws\DynamoDb\Exception\ResourceNotFoundException;

interface PersistentObject {
	function Save();
	function Delete();
	function Load($keys, $consistantRead = true);
	function Find($query);
	function errorMsg();
	function getDataFields();
	function getTableName();
	function getTableMeta();
	function Count($query);
}

/**
 * Abstract functionality for performing operations on Dynamo DB Table
 * @class DynamoDBObject
 *
 */

abstract class DynamoDBObject implements PersistentObject {

	protected static $DB;
	protected static $MARSHALER;


	private static $AWS_REGION = Region::SINGAPORE;


	protected static $FIELDS = array();

	protected static $KEYS = array();
	protected static $SECONDARY_INDEXES = array();


	protected $lastError = "";
	protected $db;
	protected $marshaler;


	public function __construct(){
		$this->db = DynamoDBObject::$DB;
		$this->marshaler = DynamoDBObject::$MARSHALER;
	}

	public static function setDatabaseAdapter($db){
		DynamoDBObject::$DB = $db;
	}

	public static function getDatabaseAdapter(){
		return DynamoDBObject::$DB;
	}

	public static function addKey($key){

		if(!isset(self::$KEYS[get_called_class()])){
			self::$KEYS[get_called_class()] = array();
		}
		if(!in_array($key, self::$KEYS[get_called_class()])){
			self::$KEYS[get_called_class()][] = $key;
		}

	}

    public static function addSecondaryIndex($key, $index){

        if(!isset(self::$SECONDARY_INDEXES[get_called_class()])){
            self::$SECONDARY_INDEXES[get_called_class()] = array();
        }
        if(!in_array($key, self::$SECONDARY_INDEXES[get_called_class()])){
            self::$SECONDARY_INDEXES[get_called_class()][$key] = $index;
        }

    }

	public static function getKeys(){
		if(isset(self::$KEYS[get_called_class()])){
			return self::$KEYS[get_called_class()];
		}
		return array();
	}

    public static function getSecondaryIndex($key){
        if(isset(self::$SECONDARY_INDEXES[get_called_class()][$key])){
            return self::$SECONDARY_INDEXES[get_called_class()][$key];
        }
        return NULL;
    }

	public static function initializeDynamoDBClient($awsKey, $awsSecret, $region = NULL, $localDB = false){
		if(empty($region)){
			$region = self::$AWS_REGION;
		}

        if($localDB){
            DynamoDBObject::$DB = DynamoDbClient::factory(array(
                    'credentials' => array(
                            'key'    => $awsKey,
                            'secret' => $awsSecret,
                    ),
                'region' => $region, #replace with your desired region
                'endpoint' => 'http://127.0.0.1:8000'
            ));
            error_log("Creating Dynamo DB local test db connection");
        }else{
            DynamoDBObject::$DB = DynamoDbClient::factory(array(
                    'credentials' => array(
                            'key'    => $awsKey,
                            'secret' => $awsSecret,
                    ),
                    'region' => $region
            ));
        }

/*
		DynamoDBObject::$DB = DynamoDbClient::factory(array(
				'credentials' => array(
						'key'    => $awsKey,
						'secret' => $awsSecret,
				),
				'region' => $region
		));
*/
		DynamoDBObject::$MARSHALER = new Marshaler();
	}

	public static function isTableExists(){
		try{
			$class = get_called_class();
			$obj = new $class();
			$response = DynamoDBObject::$DB->describeTable(array("TableName"=>$obj->getTableName()));
			if(empty($response) || !isset($response->status)){
					return true;
			}
			return !($response->status."" == "400");
		}catch(ResourceNotFoundException $e){
			return false;
		}
	}

	public static function createTable(){
		$class = get_called_class();
		$obj = new $class();
		try{
			DynamoDBObject::$DB->createTable($obj->getTableMeta());
			DynamoDBObject::$DB->waitUntil('TableExists', array(
					'TableName' => $obj->getTableName()
			));
		}catch(ResourceNotFoundException $e){

		}
	}

	public static function deleteTable(){
		$class = get_called_class();
		$obj = new $class();
		try{
			DynamoDBObject::$DB->deleteTable(array("TableName"=>$obj->getTableName()));
			DynamoDBObject::$DB->waitUntil('TableNotExists', array(
					'TableName' => $obj->getTableName()
			));
		}catch(ResourceNotFoundException $e){

		}
	}

	public static function cleanTable(){
		$class = get_called_class();
		$obj = new $class();
		try{
			$scan = DynamoDBObject::$DB->getIterator('Scan', array('TableName' => $obj->getTableName()));
			foreach ($scan as $item) {
				$keys = self::getKeys();

				$keyValues = array();
				foreach($keys as $key){
					$keyValues[] = $item[$key]['S'];
				}

				$keyList = $obj->getObjectKeyList($keyValues);
				DynamoDBObject::$DB->deleteItem(array(
						'TableName' => $obj->getTableName(),
						'Key' => $keyList
				));
			}
		}catch(\Exception $e){

		}
	}

	public function uuidV4()
	{
		return sprintf('%04x%04x-%04x-%04x-%04x-%04x%04x%04x',

				// 32 bits for "time_low"
				mt_rand(0, 0xffff), mt_rand(0, 0xffff),

				// 16 bits for "time_mid"
				mt_rand(0, 0xffff),

				// 16 bits for "time_hi_and_version",
				// four most significant bits holds version number 4
				mt_rand(0, 0x0fff) | 0x4000,

				// 16 bits, 8 bits for "clk_seq_hi_res",
				// 8 bits for "clk_seq_low",
				// two most significant bits holds zero and one for variant DCE1.1
				mt_rand(0, 0x3fff) | 0x8000,

				// 48 bits for "node"
				mt_rand(0, 0xffff), mt_rand(0, 0xffff), mt_rand(0, 0xffff)
		);
	}

	protected function autoGenerateKeyValue($key){
		return $this->uuidV4();
	}

	protected function getObjectKeyList($keyValues){
		$keys = self::getKeys();
		$keyList = array();

		for($i=0;$i<count($keys); $i++){
            $secIndex = self::getSecondaryIndex($keys[$i]);
            if(empty($secIndex)){
                $keyList[$keys[$i]] = array("S"=>$keyValues[$i]);
            }

		}

		return $keyList;
	}

	public function Save(){

		$keys = self::getKeys();

		foreach($keys as $key){
			if(!isset($this->$key)){
				$this->$key = NULL;
			}
		}

		foreach($this as $key=>$value){
			if(in_array($key, $keys) && empty($this->$key)){
				$this->$key = $this->autoGenerateKeyValue($key);
			}
		}

		$dataFields = $this->getDataFields();
		foreach ($dataFields as $field){
			//$data[$field] = (isset($this->$field) && !empty($this->$field))?$this->$field:"";
			if(isset($this->$field) && !empty($this->$field)){
				$data[$field] = $this->$field;
			}
		}

		error_log(print_r($data,true));

		try{
			$response = $this->db->putItem(array(
					'TableName'=>$this->getTableName(),
					'Item'=>$this->marshaler->marshalItem($data)
			));

			return $response;

		}catch(\Exception $e){
			$this->lastError = "Error saving:".$e->getMessage();
			error_log($e->getTraceAsString());
			error_log($e->getMessage());
            return false;
		}


	}

	public function Delete(){
		$keys = self::getKeys();
		$keyValues = array();
		foreach($keys as $key){
			$keyValues[] = $this->$key;
		}
		$keyList = $this->getObjectKeyList($keyValues);

		try{

			$result = $this->db->deleteItem(array(
					'TableName' => $this->getTableName(),
					'Key'       => $keyList
			));

			return true;

		}catch(\Exception $e){
			$this->lastError = "Error Deleting:".$e->getMessage();
			error_log($e->getTraceAsString());
			error_log($e->getMessage());
			return false;
		}


	}

	public function Load($id, $consistantRead = true){

		$keyList = $this->getObjectKeyList($id);
		$query = array(
				'ConsistentRead' => $consistantRead,
				'TableName' => $this->getTableName(),
				'Key'       => $keyList
		);
		try{
			$result = $this->db->getItem($query);
		}catch(\Exception $e){
			$this->lastError = "Error Loading:".$e->getMessage();
			error_log($e->getTraceAsString());
			error_log($e->getMessage());
			error_log("Query:".print_r($query,true));
			return false;
		}



		$dataFields = $this->getDataFields();


		if(!empty($result['Item'])){
			$item = $this->marshaler->unmarshalItem($result['Item']);
			foreach ($dataFields as $field){
				$this->$field = isset($item[$field])?$item[$field]:NULL;
			}
			return true;
		}else{
			return false;
		}
	}

	public function Find($query){
		$nonKeyFieldFound = false;
		$attributeValueList = array();
		$comparisonOperator = array();
        $secIndexSupportedKeyConditions = array("EQ");

		$keys = self::getKeys();

		foreach($query as $field => $data){
            $secIndex = NULL;
            $secIndex = self::getSecondaryIndex($field);
			if(!in_array($field, $keys) && empty($secIndex)){
				$nonKeyFieldFound = true;
			}
			$attributeValueList[$field] = array($data[0]);
			$comparisonOperator[$field] = $data[1];
		}

		if(count($query) == 0){
			$nonKeyFieldFound = true;
		}

		$operation = ($nonKeyFieldFound)?"Scan":"Query";
		$attributeListName = ($nonKeyFieldFound)?"ScanFilter":"KeyConditions";

		$dbQuery = array();
		$dbQuery['TableName'] = $this->getTableName();


        $firstKey = array_keys($attributeValueList)[0];
        $secIndex = $this->getSecondaryIndex($firstKey);
        if($operation == "Query"){
            if(count($attributeValueList) == 1 && !empty($secIndex)
                && in_array($comparisonOperator[$firstKey],$secIndexSupportedKeyConditions)){
                $dbQuery['IndexName'] = $secIndex;
            }else{
                $operation = 'Scan';
                $attributeListName = 'ScanFilter';
            }
        }

        $dbQuery[$attributeListName] = array();

		foreach($attributeValueList as $field => $val){
			$dbQuery[$attributeListName][$field]['AttributeValueList'] = $attributeValueList[$field];
			$dbQuery[$attributeListName][$field]['ComparisonOperator'] = $comparisonOperator[$field];

		}

		if(empty($dbQuery[$attributeListName])){
			unset($dbQuery[$attributeListName]);
		}

		try{
			$iterator = $this->db->getIterator($operation, $dbQuery);
		}catch(\Exception $e){

			$this->lastError = "Error Finding:".$e->getMessage();
			error_log($e->getTraceAsString());
			error_log($e->getMessage());
			return array();
		}

		$class = get_class($this);
		$items = array();
		$dataFields = $this->getDataFields();

		foreach ($iterator as $item) {
			$itemObj = new $class();
			$itemArr = $this->marshaler->unmarshalItem($item);
			foreach ($dataFields as $field){
				$itemObj->$field = isset($itemArr[$field])?$itemArr[$field]:NULL;
			}
			$items[] = $itemObj;
		}

		return $items;
	}

	public function Count($query){
		$nonKeyFieldFound = false;
		$attributeValueList = array();
		$comparisonOperator = array();

		$keys = self::getKeys();

		foreach($query as $field => $data){
			if(!in_array($field, $keys)){
				$nonKeyFieldFound = true;
			}
			$attributeValueList[$field] = array($data[0]);
			$comparisonOperator[$field] = $data[1];
		}

		if(count($query) == 0){
			$nonKeyFieldFound = true;
		}

		$operation = ($nonKeyFieldFound)?"Scan":"Query";
		$attributeListName = ($nonKeyFieldFound)?"ScanFilter":"KeyConditions";

		$dbQuery = array();
		$dbQuery['TableName'] = $this->getTableName();
		$dbQuery[$attributeListName] = array();

		foreach($attributeValueList as $field => $val){
			$dbQuery[$attributeListName][$field]['AttributeValueList'] = $attributeValueList[$field];
			$dbQuery[$attributeListName][$field]['ComparisonOperator'] = $comparisonOperator[$field];
		}

		try{
			$iterator = $this->db->getIterator($operation, $dbQuery);
		}catch(\Exception $e){
			$this->lastError = "Error Counting:".$e->getMessage();
			error_log($e->getTraceAsString());
			error_log($e->getMessage());
			return 0;
		}

		$count = 0;
		foreach ($iterator as $item) {
			$count++;
		}

		return $count;
	}

	public function errorMsg(){
		return $this->lastError;
	}

}
