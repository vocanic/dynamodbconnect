<?php
/**
 * @namespace Vocanic.Common
 */

namespace Vocanic\Common;


abstract class VocanicDynamoDBObject extends DynamoDBObject{
    public function getTableName(){
        return ENV_NAME."_".$this->_table;
    }

    public function getTableMeta(){
        return array(
            'TableName'=>$this->getTableName(),
            'AttributeDefinitions'=>array(
                array(
                    'AttributeName' => 'id',
                    'AttributeType' => 'S'
                )
            ),
            'KeySchema' => array(
                array(
                    'AttributeName' => 'id',
                    'KeyType'       => 'HASH'
                )
            ),
            'ProvisionedThroughput' => array(
                'ReadCapacityUnits'  => 1,
                'WriteCapacityUnits' => 1
            )
        );
    }
}