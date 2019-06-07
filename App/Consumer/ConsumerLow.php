<?php

namespace App\Consumer;

use RdKafka\TopicConf;

class ConsumerLow
{

    public function consume()
    {
        $topicConf = new TopicConf();
        $topicConf->set('auto.commit.enable', 'false');
        $consumer = new \RdKafka\Consumer();
        $topic = $consumer->newTopic('test', $topicConf);
        $topic->consumeStart(0, $offset)
    }
}