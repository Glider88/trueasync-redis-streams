<?php declare(strict_types=1);

namespace Glider88\AsyncRedisStreams\Redis;

use Glider88\AsyncRedisStreams\Logger\LoggerInterface;
use Predis\Client;

readonly class PredisWrapper implements ClientInterface
{
    public function __construct(
        private Client $predis,
        private LoggerInterface $logger,
    ) {}

    /** @param list<string|int> $args */
    public function execute(array $args): mixed
    {
        $this->logger->debug(implode(' ', $args));

        return $this->predis->executeRaw($args);
    }

    public function ping(): void
    {
        $this->predis->ping();
    }

    public function close(): void
    {
        $this->predis->disconnect();
    }
}
