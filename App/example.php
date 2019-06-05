<?php

namespace App;



use App\Events\Poc\Common\SharedMeta;
use App\Events\Poc\User\V1\UserEvent;
use App\Producer\ProducerConfig;
use App\Producer\Producer;
use App\Producer\ProducerFactory;
use Faker\Factory;

require_once __DIR__ . '/../vendor/autoload.php';

$topic = 'user-event';
$config = new ProducerConfig('schema-registry:8081', 'brokers');

$producer  = (new ProducerFactory($config))->instance();

$faker = Factory::create();

for ($i = 1; $i < 50; $i++) {

    echo "Producing to topic: $topic".PHP_EOL;
    $meta1 = (new SharedMeta())->setUuid($i);
    $userEventV1 = (new UserEvent())->setUserId($faker->randomDigit)->setMeta($meta1);
    $producer->fire($topic, $userEventV1);
    $producer->fire($topic, $meta1);
}
