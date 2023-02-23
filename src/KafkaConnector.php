<?php

namespace Kafka;

use Illuminate\Queue\Connectors\ConnectorInterface;

class KafkaConnector
{
    public function connect(array $config){

        $conf = new \RdKafka\Conf();

        $conf->set('bootstrap.servers', $config['bootstrap_servers']);
        $conf->set('security.protocol', $config['security_protocol']);
        $conf->set('sasl.mechanisms', $config['sasl_mechanisms']);
        $conf->set('sasl.username', $config['sasl_username']);
        $conf->set('sasl.password', $config['sasl_password']);

        $producer = new \RdKafka\Producer($conf); /// producer instance

        $conf->set('group.id', $config['group_id']);
        $conf->set('auto.offset.reset', 'earliest');

        $consumer = new \RdKafka\KafkaConsumer($conf); /// consumer instance


        return new KafkaQueue($producer,$consumer);

    }
}
