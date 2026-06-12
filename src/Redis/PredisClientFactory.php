<?php declare(strict_types=1);

namespace Glider88\AsyncRedisStreams\Redis;

use Glider88\AsyncRedisStreams\Config;
use Glider88\AsyncRedisStreams\Logger\LoggerInterface;
use Predis\Client;

readonly class PredisClientFactory implements ClientFactoryInterface
{
    public function __construct(
        private string $redisUrl,
        private LoggerInterface $logger,
    ) {}

    public static function fromConfig(Config $cfg): self
    {
        return new PredisClientFactory(
            redisUrl: $cfg->redisUrl,
            logger: $cfg->logger
        );
    }

    public function make(): ClientInterface
    {
        $conn = new Client($this->redisUrl);
        $conn->connect();

        return new PredisWrapper($conn, $this->logger);
    }
}
