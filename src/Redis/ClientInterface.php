<?php declare(strict_types=1);

namespace Glider88\AsyncRedisStreams\Redis;

interface ClientInterface
{
    public const string OK = 'OK';
    public function execute(array $args): mixed;
    public function ping(): void;
    public function close(): void;
}
