<?php declare(strict_types=1);

namespace Glider88\AsyncRedisStreams\Redis;

use Glider88\AsyncRedisStreams\Config;
use Async\Pool;
use Glider88\AsyncRedisStreams\TimeInterval\TimeIntervalInterface;

readonly class PoolPredisWrapper implements ClientInterface
{
    private Pool $pool;

    public function __construct(
        ClientFactoryInterface $factory,
        private int $minRedisWorkers,
        private int $maxRedisWorkers,
        private TimeIntervalInterface $acquireTimeout,
        private TimeIntervalInterface $healthcheckInterval,
    ) {
        $this->pool = new Pool(
            factory: static fn() => $factory->make(),
            destructor: static fn(ClientInterface $client) => $client->close(),
            healthcheck: static fn(ClientInterface $client) => $client->ping(),
            min: $this->minRedisWorkers,
            max:  $this->maxRedisWorkers,
            healthcheckInterval: $this->healthcheckInterval->milli(),
        );
    }

    public static function fromConfig(Config $cfg): self
    {
        return new PoolPredisWrapper(
            factory: PredisClientFactory::fromConfig($cfg),
            minRedisWorkers: $cfg->minRedisWorkers,
            maxRedisWorkers: $cfg->maxRedisWorkers,
            acquireTimeout: $cfg->timeoutJob,
            healthcheckInterval: $cfg->healthcheckInterval,
        );
    }

    public function execute(array $args): mixed
    {
        /** @var ClientInterface $redis */
        $redis = $this->pool->acquire(timeout: $this->acquireTimeout->milli());
        try {
            $response = $redis->execute($args);
        } finally {
            $this->pool->release($redis);
        }

        return $response;
    }

    public function ping(): void {}

    public function close(): void
    {
        $this->pool->close();
    }
}
