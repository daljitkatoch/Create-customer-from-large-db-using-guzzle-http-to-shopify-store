<?php
require __DIR__ . '/vendor/autoload.php';

use GuzzleHttp\Client;
use Monolog\Logger;
use Monolog\Handler\StreamHandler;

$logfile = new Logger('create_customer_log');
$logfile->pushHandler(new StreamHandler('customer_import.log', Logger::INFO));

$accessToken = 'Enter-access-token-here';
$shopUrl = 'https://dev-store.myshopify.com/admin/api/2021-07/graphql.json';

$client = new Client([
    'base_uri' => $shopUrl,
    'headers' => [
        'Content-Type' => 'application/json',
        'X-Shopify-Access-Token' => $accessToken,
    ],
]);

/* Create connection */
$servername = "localhost";
$username = "username";
$password = "password";
$dbname = "dbname";
$port = 3306;

$conn = new mysqli($servername, $username, $password, $dbname, $port);
if ($conn->connect_error) {
    die("Connection failed: " . $conn->connect_error);
}


/* get total record */
$query_count = 'SELECT count(*) as total_records FROM TABLE_NAME';
$result_num = $conn->query($query_count);
$records = $result_num->num_rows; 

$batchSize = 10000; /* Batch Size*/
$totalRecords = $records; /* Total number of records to import */
$offset = 0; /* Starting offset for each batch */

$query = <<<QUERY
    mutation customerCreate(\$input: CustomerInput!) {
        customerCreate(input: \$input) {
            userErrors {
                field
                message
            }
            customer {
                id
                email
                phone
                taxExempt
                acceptsMarketing
                firstName
                lastName
                addresses {
                    address1
                    city
					          province
                    country
                    phone
                    zip
					          lastName
					          firstName
                }
				        tags
            }
        }
    }
QUERY;

while ($offset < $totalRecords) {
	
	/*Disable ONLY_FULL_GROUP_BY mode*/
	$setsql2 = "SET SESSION sql_mode = (SELECT REPLACE(@@sql_mode, 'ONLY_FULL_GROUP_BY', ''))";
	$conn->query($setsql2);
	
    /* Fetch a batch of record */
	$query_batch = "SELECT *	FROM TABLE_NAME LIMIT $offset, $batchSize";
  
    $result = $conn->query($query_batch);    
    if ($result->num_rows > 0) {        
        while ($row = $result->fetch_assoc()) {
            
			/* check string is available for encoding */
			$expectedEncoding = "UTF-8";
			$inputString_1 = $row['FIRSTNAME'];
			$inputString_2 = $row['LASTNAME'];
			if (mb_check_encoding($inputString, $expectedEncoding) == false || mb_check_encoding($inputString_2, $expectedEncoding) == false){
				continue;
			}
 			
            $variables = [
				'input' => [
					'email' => $row['EMAIL'],
					'phone' => $row['PHONE'],
					'firstName' => mb_convert_encoding($row['FIRSTNAME'], 'UTF-8', 'auto'),
					'lastName' => mb_convert_encoding($row["LASTNAME"], 'UTF-8', 'auto'),
					'acceptsMarketing' => true,
					"addresses" => [
						[
							"address1"=>"412 fake st",
							"city"=>"Ottawa",
							"province"=>"ON",
							"country"=>"CA",				
							"phone"=>"+16465555559", 
							"zip"=>"A1A 4A1", 
							"lastName"=>"Lastname", 
							"firstName"=>"Steve"				
						]
					],
					'tags' => 'imported'
				]
			];
			
			$requestData = [
				'json' => [
					'query' => $query,
					'variables' => $variables,
				],
			];
			
            try {
				$response = $client->request('POST', '', $requestData);
				$body = $response->getBody();
				$data = json_decode($body, true);
				$email = $variables['input']['email'];
				if (isset($data['errors'])) {
					
					$error_message = "";
					$errors = $data['errors'];
					foreach($errors as $error){
						$error_message .= $error['message'];
					}
					$logfile->error("error for  $email: $error_message"."\n");
					
				} else {	
					$field_errors = $data['data']['customerCreate']['userErrors'];
					if(count($field_errors)>0){
						$field_error_message = "";		
						foreach($field_errors as $error){
							$field_error_message .= $error['message'].' ';
						}
						$logfile->error("error for  $email: $field_error_message"."\n");
						echo "error for  $email: ".$field_error_message. "\n";
					}else{
						
						$id = $data['data']['customerCreate']['customer']['id'];
						
						$integerId = substr($id, strrpos($id, '/') + 1);
						/* Convert the extracted part to an integer */
						$integerIdValue = (int)$integerId;
						
						$logfile->info("Customer created successfully $email: $id"."\n");
						echo $success_message = "Customer created: ".$id;
						
						
					}		
				}
			} catch (\Exception $e) {
				$statusCode = $e->getResponse()->getStatusCode();
				echo $errorMessage = $e->getMessage();
			}	
			ob_flush();	
			flush();
			usleep(500000);
     }
        
    }
	$offset += $batchSize;

}

/* Close the connections */
$conn->close();
?>
