<?php

namespace App;



use App\Events\Poc\Common\SharedMeta;
use App\Events\Poc\User\V1\UserEvent;
use App\Producer\Config;
use App\Producer\Producer;
use App\Producer\ProducerFactory;
use Faker\Factory;

require_once __DIR__ . '/../vendor/autoload.php';
//
//$faker = Factory::create();
//$m = new SharedMeta();
//$e = new UserEvent();
//$meta1 = $m
//  ->setUuid($faker->uuid);
//$userEventV1 = $e
//  ->setUserId($faker->randomDigit)
//  ->setMeta($meta1);
//var_dump(json_encode($e));
//$faker->seed(1);
//
$topic = 'user-event';
$config = new Config(
  'brokers',
  'schema-registry:8081'
  );

$producer  = ProducerFactory::instance();

$faker = Factory::create();
for ($i = 1; $i < 50; $i++) {
    echo "Producing to topic: $topic".PHP_EOL;
    $meta1 = (new SharedMeta())
      ->setUuid($i);
    $userEventV1 = (new UserEvent())
      ->setUserId($faker->randomDigit)
      ->setMeta($meta1);
    $a = $producer->fire($topic, $userEventV1);
    print "--- $a ----";
    $producer->fire($topic, $meta1);
    $producer->kafkaProducer->poll(0);
}
