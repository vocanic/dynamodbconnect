<?php
namespace Vocanic\Common;
use Vocanic\Common\DynamoDBObject;


if(!class_exists("TestTemplate")) {
	include dirname(__FILE__).'/../TestTemplate.php';
}

class DynamoDBObjectTest extends \TestTemplate{

	var $obj = null;
	public $client = 'vocanic';

	protected function setUp()
	{
		parent::setUp();
        $isTestDB = true;
        if(defined('USE_LOCAL_DB') && USE_LOCAL_DB === "0"){
            $isTestDB = false;
        }
		DynamoDBObject::initializeDynamoDBClient(AWS_DYNAMO_DB_ACCESS_KEY, AWS_DYNAMO_DB_ACCESS_SECRET, NULL, $isTestDB);
		User::addKey('client');
		User::addKey('userId');
		User::addKey('email');
        User::addSecondaryIndex('email','emailIndex');



        if(User::isTableExists()){
            User::cleanTable();
        }else{
            User::createTable(new User());
        }

	}

	public function testSaveLoad(){

		$user = new User();
		$user->client = $this->client;
		$user->email = "user@vocanic.com";
		$user->password = "Password";
		$user->name = "User ".rand();
		$user->location = "SL";
		$user->time = 100;
		$user->Save();

		error_log(json_encode($user));

		$user1 = new User();
		$ok = $user1->Load(array($user->client,$user->userId), true);
		$this->assertEquals($ok, true);
		$this->assertEquals($user1->userId, $user->userId);



	}

	public function testUpdate(){

		$user = new User();
		$user->client = $this->client;
		$user->email = "user@vocanic.com";
		$user->password = "Password";
		$user->name = "User ".rand();
		$user->location = "SL";
		$user->Save();

		$user1 = new User();
		$ok = $user1->Load(array($user->client,$user->userId), true);
		$this->assertEquals($ok, true);
		$this->assertEquals($user1->userId, $user->userId);

		$user1->time = 234;
		$user1->email = "test@hc.com";
		$user1->Save();

		$user2 = new User();
		$ok = $user2->Load(array($user->client,$user->userId), true);
		$this->assertEquals($ok, true);
		$this->assertEquals($user2->userId, $user->userId);
		$this->assertEquals($user2->password, $user->password);
		$this->assertEquals($user2->name, $user->name);
		$this->assertEquals($user2->email, "test@hc.com");
		$this->assertEquals($user2->time, 234);

	}

	public function testDelete(){
		$user = new User();
		$user->client = $this->client;
		$user->email = "user@vocanic.com";
		$user->password = "Password";
		$user->name = "User ".rand();
		$user->location = "SL";
		$user->Save();

		$user1 = new User();
		$user1->Load(array($user->client,$user->userId), true);

		$user1->Delete();

		$user2 = new User();
		$ok = $user2->Load(array($user->client,$user->userId));

		$this->assertEquals($ok, false);
	}

	public function testFindandCount(){

		/*
		 * KeyConditions = EQ | LE | LT | GE | GT | BEGINS_WITH | BETWEEN
		 * ComparisonOperator = EQ | NE | LE | LT | GE | GT | NOT_NULL | NULL | CONTAINS | NOT_CONTAINS | BEGINS_WITH | IN | BETWEEN
		 */


		$user = new User();
		$user->client = $this->client;
		$user->email = "user1@vocanic.com";
		$user->password = "Password";
		$user->time = 5001;
		$user->name = "User ".rand();
		$user->location = "SL";
		$user->Save();

		$user = new User();
		$user->client = $this->client;
		$user->email = "user2@vocanic.com";
		$user->password = "Password";
		$user->time = 6034;
		$user->name = "User ".rand();
		$user->location = "SL";
		$user->Save();

		$user = new User();
		$user->client = $this->client;
		$user->email = "user2@huckleberryapp.com";
		$user->password = "Password";
		$user->time = 1230;
		$user->name = "User ".rand();
		$user->location = "SL";
		$user->Save();

		$users = $user->Find(
				array(
					"email"=>array(
						array("S"=>"user1@vocanic.com"),
						"EQ"
					)
				)
		);
		echo "Users :".json_encode($users)."\r\n";

		$this->assertEquals(1,count($users));

		$users = $user->Find(
				array(
						"email"=>array(
								array("S"=>"vocanic.com"),
								"CONTAINS"
						)
				)
		);
		echo "Users :".json_encode($users)."\r\n";

		$this->assertEquals(2,count($users));


		$users = $user->Find(
				array(
						"email"=>array(
								array("S"=>"user1"),
								"CONTAINS"
						),
						"password"=>array(
								array("S"=>"Password"),
								"EQ"
						)
				)
		);
		echo "Users :".json_encode($users)."\r\n";

		$this->assertEquals(1,count($users));


		$users = $user->Find(
				array(
						"time"=>array(
								array("N"=>10),
								"GT"
						)
				)
		);
		echo "Users :".json_encode($users)."\r\n";

		$this->assertEquals(3,count($users));

		$users = $user->Find(
				array(
						"time"=>array(
								array("N"=>5000),
								"GT"
						)
				)
		);
		echo "Users :".json_encode($users)."\r\n";

		$this->assertEquals(2,count($users));


		$users = $user->Find(
				array(
						"time"=>array(
								array("N"=>5000),
								"GT"
						)
				)
		);
		echo "Users :".json_encode($users)."\r\n";

		$this->assertEquals(2,count($users));

		$usersCount = $user->Count(
				array(
						"time"=>array(
								array("N"=>5000),
								"GT"
						)
				)
		);
		echo "Users count:".$usersCount."\r\n";

		$this->assertEquals(2,$usersCount);

	}

    public function testSecondaryIndex(){

        /*
         * KeyConditions = EQ | LE | LT | GE | GT | BEGINS_WITH | BETWEEN
         * ComparisonOperator = EQ | NE | LE | LT | GE | GT | NOT_NULL | NULL | CONTAINS | NOT_CONTAINS | BEGINS_WITH | IN | BETWEEN
         */



        $user = new User();
        $user->client = $this->client;
        $user->email = "user1@vocanic.com";
        $user->password = "Password";
        $user->time = 5001;
        $user->name = "User ".rand();
        $user->location = "SL";
        $user->Save();

        $user = new User();
        $user->client = $this->client;
        $user->email = "user2@vocanic.com";
        $user->password = "Password";
        $user->time = 6034;
        $user->name = "User ".rand();
        $user->location = "SL";
        $user->Save();

        $user = new User();
        $user->client = $this->client;
        $user->email = "user2@huckleberryapp.com";
        $user->password = "Password";
        $user->time = 1230;
        $user->name = "User ".rand();
        $user->location = "SL";
        $user->Save();

        $users = $user->Find(
            array(
                "email"=>array(
                    array("S"=>"user1@vocanic.com"),
                    "EQ"
                )
            )
        );
        echo "Users :".json_encode($users)."\r\n";

        $this->assertEquals(1,count($users));



    }
}




class User extends DynamoDBObject{

    public function getTableName(){
        return ENV_NAME."_UserTester";
    }

    public function getDataFields(){
        return array(
            "client",
            "userId",
            "email",
            "password",
            "name",
            "location",
            "time"
        );
    }

    public function getTableMeta(){
        return array(
            'TableName'=>$this->getTableName(),
            'AttributeDefinitions'=>array(
                array(
                    'AttributeName' => 'client',
                    'AttributeType' => 'S'
                ),
                array(
                    'AttributeName' => 'userId',
                    'AttributeType' => 'S'
                ),
                array(
                    'AttributeName' => 'email',
                    'AttributeType' => 'S'
                )
            ),
            'KeySchema' => array(
                array(
                    'AttributeName' => 'client',
                    'KeyType'       => 'HASH'
                ),
                array(
                    'AttributeName' => 'userId',
                    'KeyType'       => 'RANGE'
                )
            ),

            'GlobalSecondaryIndexes' => array(
                array(
                    'IndexName' => 'emailIndex',
                    'KeySchema' => array(
                        array('AttributeName' => 'email',    'KeyType' => 'HASH')
                    ),
                    'Projection' => array(
                        'ProjectionType' => 'KEYS_ONLY',
                    ),
                    'ProvisionedThroughput' => array(
                        'ReadCapacityUnits'  => 1,
                        'WriteCapacityUnits' => 1
                    )
                )
            ),


            'ProvisionedThroughput' => array(
                'ReadCapacityUnits'  => 1,
                'WriteCapacityUnits' => 1
            )
        );
    }
}