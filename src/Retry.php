<?php declare(strict_types=1);

namespace Glider88\AsyncRedisStreams;

use Glider88\AsyncRedisStreams\Redis\Redis;
use function Async\{spawn_thread, delay};

readonly class Retry
{
    public function __construct(
        private Config $config,
    ) {}
    
    public function run(): void
    {
        $cfg = $this->config;
        spawn_thread(
            task: static function() use ($cfg) {
                $redis = Redis::fromConfig($cfg);
                while (true) {
                    if ($redis->isConsumerCanceled($cfg->consumer)) {
                        return;
                    }

                    delay($cfg->retryInterval->milli());
                    $cfg->logger->debug('new auto retry loop tick');
                    $items = $redis->retryRange();
                    $cfg->logger->debug('count retry: ' . count($items));
                    foreach ($items as $item) {
                        $cfg->logger->debug("put from retry to stream");
                        $redis->add($item);
                        $redis->removeFromRetry($item);
                    }
                }
            },
            bootloader: $this->config->threadBootloader,
        );
    }
}
