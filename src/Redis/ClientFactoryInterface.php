<?php declare(strict_types=1);

namespace Glider88\AsyncRedisStreams\Redis;

interface ClientFactoryInterface
{
    public function make(): ClientInterface;
}
